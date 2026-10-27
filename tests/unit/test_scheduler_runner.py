"""UT-PS-11: fake clock, persistent ledger, cancellation and startup retry."""

import asyncio
import logging
import signal
from datetime import UTC, date, datetime, time, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
from dependency_injector import providers

from core.entities import ParseDispatch, ParseDispatchTrigger
from core.protocols.clients.parse_commands import ParseCommandPublisherProtocol
from core.protocols.parsing.provider import ParseTarget
from core.protocols.repositories.parse_dispatch_repository import ParseDispatchRepositoryProtocol
from core.services.parsing import EnvTimezoneProvider, ParseDispatchService, SlotCalculator
from core.services.parsing.registry import ParseProviderRegistry
from scheduler import main as process
from scheduler.runner import SchedulerRunner


class DemoProvider:
    code = "demo"
    subject = "commands.parse.demo.run"

    def __init__(self) -> None:
        self.targets = [ParseTarget(uuid4()), ParseTarget(uuid4())]

    async def list_targets(self, *, offset: int, limit: int) -> tuple[list[ParseTarget], int]:
        return self.targets[offset : offset + limit], len(self.targets)

    async def is_target(self, *, equestrian_id: UUID) -> bool:
        return True

    def build_params(self, *, trigger: ParseDispatchTrigger) -> dict[str, object]:
        return {}


class Clock:
    now = datetime(2026, 10, 29, 2, 59, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


def runner_graph(*, enabled=True, dispatch=None, sleep=asyncio.sleep):
    clock = Clock()
    registry = ParseProviderRegistry(enabled_codes=["demo", "other"])
    registry.register(DemoProvider())
    other = DemoProvider()
    other.code, other.subject = "other", "commands.parse.other.run"
    registry.register(other)
    dispatch = dispatch if dispatch is not None else Mock(dispatch_scheduled_slot=AsyncMock(return_value=True))
    runner = SchedulerRunner(
        registry=registry,
        slots=SlotCalculator(timezone_provider=EnvTimezoneProvider(utc_offset_hours=0), schedule_time=time(3)),
        dispatch=dispatch,
        enabled=enabled,
        tick_seconds=30,
        clock=clock,
        sleep=sleep,
    )
    return runner, clock, dispatch


async def test_before_due_once_per_day_and_new_day():
    runner, clock, dispatch = runner_graph()
    await runner.tick()
    dispatch.dispatch_scheduled_slot.assert_not_awaited()
    clock.now += timedelta(minutes=1)
    await runner.tick()
    await runner.tick()
    assert dispatch.dispatch_scheduled_slot.await_count == 2
    dispatch.dispatch_scheduled_slot.assert_any_await(provider="demo", local_date=date(2026, 10, 29))
    clock.now += timedelta(days=1)
    await runner.tick()
    assert dispatch.dispatch_scheduled_slot.await_count == 4
    assert runner._completed == {("demo", date(2026, 10, 30)), ("other", date(2026, 10, 30))}


async def test_disabled_is_idle_without_clock_registry_or_dispatch():
    runner, _, dispatch = runner_graph(enabled=False)
    runner._clock = Mock(side_effect=AssertionError("disabled must not read clock"))
    await runner.tick()
    dispatch.dispatch_scheduled_slot.assert_not_awaited()


async def test_provider_error_and_false_are_retried_without_caching(caplog):
    runner, clock, dispatch = runner_graph()
    clock.now += timedelta(hours=1)
    dispatch.dispatch_scheduled_slot.side_effect = [RuntimeError("password"), True, False, True]
    await runner.tick()
    await runner.tick()
    await runner.tick()
    await runner.tick()
    assert [call.kwargs["provider"] for call in dispatch.dispatch_scheduled_slot.await_args_list] == [
        "demo",
        "other",
        "demo",
        "demo",
    ]
    assert "password" not in caplog.text


async def test_restart_uses_ledger_and_target_failure_does_not_stop_others():
    provider = DemoProvider()
    registry = ParseProviderRegistry(enabled_codes=["demo"])
    registry.register(provider)
    ledger = AsyncMock(spec=ParseDispatchRepositoryProtocol)
    publisher = AsyncMock(spec=ParseCommandPublisherProtocol)
    published: set[UUID] = set()
    now = datetime(2026, 10, 29, 3, tzinfo=UTC)

    def claim(**kw):
        if kw["job_id"] in published:
            return None
        return ParseDispatch(
            id=kw["job_id"],
            provider=kw["provider"],
            equestrian_id=kw["equestrian_id"],
            trigger="schedule",
            schedule_date=kw["schedule_date"],
            status="pending",
            attempts=1,
            last_error_code=None,
            published_at=None,
            created_at=now,
            updated_at=now,
        )

    def mark(**kw):
        published.add(kw["job_id"])
        return True

    ledger.claim_schedule.side_effect = claim
    ledger.mark_published.side_effect = mark
    publisher.publish.side_effect = [RuntimeError("private"), None, None]
    dispatch = ParseDispatchService(
        clock=lambda: now,
        slots=SlotCalculator(timezone_provider=EnvTimezoneProvider(utc_offset_hours=0), schedule_time=time(3)),
        publisher=publisher,
        repository=ledger,
        registry=registry,
        page_size=1,
        max_attempts=3,
    )

    def build():
        return SchedulerRunner(
            registry=registry,
            slots=SlotCalculator(timezone_provider=EnvTimezoneProvider(utc_offset_hours=0), schedule_time=time(3)),
            dispatch=dispatch,
            enabled=True,
            tick_seconds=30,
            clock=lambda: now,
        )

    runner = build()
    await runner.tick()
    assert publisher.publish.await_count == 2  # next target survived
    assert not runner._completed
    await runner.tick()
    assert publisher.publish.await_count == 3  # only previously failed target
    restarted = build()
    await restarted.tick()
    await restarted.tick()
    assert publisher.publish.await_count == 3
    assert len(published) == 2
    # Existing published claim skip is False, not reliable evidence of completion.
    assert not restarted._completed
    assert all(call.kwargs["lease"] == timedelta(minutes=5) for call in ledger.claim_schedule.await_args_list)
    assert all(call.kwargs["attempts"] == 1 for call in ledger.mark_published.await_args_list)


async def test_tick_loop_interval_and_transient_tick_error():
    runner, _, _ = runner_graph(sleep=AsyncMock(side_effect=[None, asyncio.CancelledError()]))
    runner.tick = AsyncMock(side_effect=[RuntimeError("private"), None])
    with pytest.raises(asyncio.CancelledError):
        await runner.run()
    assert runner.tick.await_count == 2
    assert [call.args[0] for call in runner._sleep.await_args_list] == [30, 30]


async def test_cancel_during_dispatch_propagates():
    runner, clock, dispatch = runner_graph()
    clock.now += timedelta(hours=1)
    dispatch.dispatch_scheduled_slot.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await runner.tick()
    assert not runner._completed
    assert dispatch.dispatch_scheduled_slot.await_count == 1


@pytest.mark.parametrize("step", ["connect", "setup_streams"])
async def test_nats_retry_bounded_backoff_one_escalation(step, caplog):
    client = Mock(connect=AsyncMock(), setup_streams=AsyncMock(), close=AsyncMock())
    getattr(client, step).side_effect = [RuntimeError("nats://secret:password@peer")] * 7 + [None]
    sleep = AsyncMock()
    await process.setup_nats(client, sleep=sleep)
    assert [call.args[0] for call in sleep.await_args_list] == [1, 2, 4, 8, 16, 30, 30]
    assert client.connect.await_count == 8
    assert client.setup_streams.await_count == (1 if step == "connect" else 8)
    assert len([record for record in caplog.records if record.levelno == logging.ERROR]) == 1
    assert "password" not in caplog.text and "nats://" not in caplog.text


@pytest.mark.parametrize("phase", ["connect", "setup_streams", "run"])
@pytest.mark.parametrize("cancel_parent", [False, True])
async def test_stop_cancels_work_and_closes_resources(phase, cancel_parent):
    started, cancelled, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def pending():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    runner = Mock(run=AsyncMock())
    client = Mock(connect=AsyncMock(), setup_streams=AsyncMock(), close=AsyncMock())
    target = runner if phase == "run" else client
    getattr(target, phase).side_effect = pending
    close_database = AsyncMock()
    task = asyncio.create_task(process.serve(runner=runner, client=client, stop=stop, close_database=close_database))
    await asyncio.wait_for(started.wait(), timeout=1)
    if cancel_parent:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        stop.set()
        await asyncio.wait_for(task, timeout=1)
    assert cancelled.is_set()
    client.close.assert_awaited_once()
    close_database.assert_awaited_once()
    assert not any(t.get_name().startswith("parse-scheduler") for t in asyncio.all_tasks())


async def test_database_cleanup_even_if_client_close_fails():
    client = Mock(connect=AsyncMock(), setup_streams=AsyncMock(), close=AsyncMock(side_effect=RuntimeError))
    stop = asyncio.Event()
    stop.set()
    close_database = AsyncMock()
    with pytest.raises(RuntimeError):
        await process.serve(runner=Mock(run=AsyncMock()), client=client, stop=stop, close_database=close_database)
    close_database.assert_awaited_once()


async def test_main_composition_shared_client_short_session_repository_and_signals(monkeypatch):
    from containers.application import clients_container, repositories_container, settings_container

    client = Mock()
    captured = {}
    loop = Mock()
    monkeypatch.setattr(process.asyncio, "get_running_loop", lambda: loop)

    async def serve(**kwargs):
        captured.update(kwargs)
        assert isinstance(kwargs["runner"], SchedulerRunner)
        # No ContextLocalResource/session initialization by scheduler composition.
        assert not repositories_container.session.initialized
        assert kwargs["runner"]._dispatch._repository.session_factory is not None
        assert kwargs["runner"]._dispatch._publisher._client is client
        for call in loop.add_signal_handler.call_args_list:
            call.args[1]()
            assert kwargs["stop"].is_set()
            kwargs["stop"].clear()

    monkeypatch.setattr(process, "serve", serve)
    with clients_container.nats_client.override(providers.Object(client)):
        await process.main()
    assert captured["client"] is client
    assert captured["runner"]._tick_seconds == settings_container.parse_settings().parse_scheduler_tick_seconds
    assert [call.args[0] for call in loop.add_signal_handler.call_args_list] == [signal.SIGTERM, signal.SIGINT]
    assert loop.remove_signal_handler.call_count == 2


async def test_cancel_during_nats_backoff():
    waiting = asyncio.Event()
    client = Mock(connect=AsyncMock(side_effect=RuntimeError), setup_streams=AsyncMock())

    async def sleep(_):
        waiting.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(process.setup_nats(client, sleep=sleep))
    await asyncio.wait_for(waiting.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    client.connect.assert_awaited_once()
    client.setup_streams.assert_not_awaited()


@pytest.mark.parametrize("offset", [0, 3, -12, 14])
@pytest.mark.parametrize("failed", [False, True])
async def test_midnight_between_providers_never_dispatches_previous_day(offset, failed):
    runner, clock, dispatch = runner_graph()
    runner._slots = SlotCalculator(
        timezone_provider=EnvTimezoneProvider(utc_offset_hours=offset), schedule_time=time(3)
    )
    clock.now = datetime(2026, 10, 29, 23, 59, 59, tzinfo=UTC) - timedelta(hours=offset)

    async def first_provider(**kwargs):
        clock.now += timedelta(seconds=2)
        if failed:
            raise RuntimeError("provider failed across midnight")
        return True

    dispatch.dispatch_scheduled_slot.side_effect = first_provider
    await runner.tick()
    dispatch.dispatch_scheduled_slot.assert_awaited_once_with(provider="demo", local_date=date(2026, 10, 29))
    # Previous published result is not revoked; failure never counts as completed.
    assert runner._completed == (set() if failed else {("demo", date(2026, 10, 29))})
    await runner.tick()  # new day before due time: neither provider runs
    assert dispatch.dispatch_scheduled_slot.await_count == 1
    assert not runner._completed
    clock.now += timedelta(hours=3)
    dispatch.dispatch_scheduled_slot.side_effect = None
    dispatch.dispatch_scheduled_slot.return_value = True
    await runner.tick()
    assert dispatch.dispatch_scheduled_slot.await_count == 3
    assert runner._completed == {("demo", date(2026, 10, 30)), ("other", date(2026, 10, 30))}
