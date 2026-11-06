"""Runtime composition and restart tests, NOT PostgreSQL/JetStream acceptance."""

import asyncio
import logging
from contextlib import suppress
from pathlib import Path
from time import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from nats.js.errors import BadRequestError
from pydantic import ValidationError

from clients.nats.maintenance_results import CLEANUP_SUBJECT, MERGE_SUBJECT, MaintenanceResultConsumer
from depends.normalization_runtime import build_runtime
from maintenance import main as entrypoint
from maintenance.readiness import is_ready
from maintenance.runtime import MaintenanceRuntime, serve
from settings import NormalizationSettings


@pytest.fixture(autouse=True)
def no_env(monkeypatch):
    monkeypatch.setitem(NormalizationSettings.model_config, "env_file", None)
    from settings import NatsSettings

    monkeypatch.setitem(NatsSettings.model_config, "env_file", None)
    monkeypatch.delenv("SEO_NORMALIZATION_ENABLED", raising=False)


def harness(tmp_path):
    settings = NormalizationSettings(
        SEO_NORMALIZATION_ENABLED=True,
        SEO_NORMALIZATION_TICK_SECONDS=0.1,
        SEO_NORMALIZATION_READINESS_FILE=str(tmp_path / "ready"),
    )
    client = MagicMock(is_connected=True, connect=AsyncMock(), setup_maintenance=AsyncMock(), close=AsyncMock())
    consumers = [MagicMock(is_ready=False), MagicMock(is_ready=False)]
    for consumer in consumers:

        async def run(c=consumer):
            c.is_ready = True
            try:
                await asyncio.Event().wait()
            finally:
                c.is_ready = False

        consumer.run = AsyncMock(side_effect=run)
    dispatcher = MagicMock(tick=AsyncMock(return_value=0))
    runtime = MaintenanceRuntime(client=client, consumers=consumers, dispatcher=dispatcher, settings=settings)
    return runtime, client, consumers, dispatcher, settings


async def until(predicate):
    async with asyncio.timeout(5):
        while not predicate():  # noqa: ASYNC110 - bounded observation of another task
            await asyncio.sleep(0.01)


async def cancel(task):
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def test_readiness_requires_bound_consumers_live_tasks_and_successful_tick(tmp_path):
    runtime, client, consumers, dispatcher, settings = harness(tmp_path)
    assert not runtime.is_ready
    worker = asyncio.create_task(runtime.run())
    try:
        await until(lambda: is_ready(settings))
        assert runtime.is_ready
        consumers[0].is_ready = False
        assert not runtime.is_ready
        consumers[0].is_ready = True
        client.is_connected = False
        assert not runtime.is_ready
        client.is_connected = True
        dispatcher.tick.return_value = 1
        await until(lambda: not runtime.is_ready)
        await until(lambda: not is_ready(settings))
        dispatcher.tick.return_value = 0
        await until(lambda: is_ready(settings))
    finally:
        await cancel(worker)
    assert not runtime.is_ready
    assert not is_ready(settings)
    assert all(not c.is_ready for c in consumers)
    client.close.assert_awaited()
    assert not runtime._tasks


async def test_setup_retry_then_subscription_start_and_shutdown(tmp_path, caplog):
    runtime, client, consumers, _, settings = harness(tmp_path)
    client.setup_maintenance.side_effect = [OSError("secret connection string"), None]
    with caplog.at_level(logging.WARNING):
        worker = asyncio.create_task(runtime.run())
        try:
            await until(lambda: client.setup_maintenance.await_count >= 2)
            await until(lambda: is_ready(settings))
            assert all(c.run.await_count == 1 for c in consumers)
        finally:
            await cancel(worker)
    assert "secret connection string" not in caplog.text
    assert client.close.await_count == 2


async def test_incompatible_topology_fails_fast_and_closes_client(tmp_path, caplog):
    runtime, client, consumers, _, settings = harness(tmp_path)
    client.setup_maintenance.side_effect = BadRequestError(description="secret topology")
    with caplog.at_level(logging.ERROR), pytest.raises(BadRequestError):
        await runtime.run()
    client.setup_maintenance.assert_awaited_once()
    client.close.assert_awaited_once()
    assert all(c.run.await_count == 0 for c in consumers)
    assert not is_ready(settings)
    assert "secret topology" not in caplog.text


@pytest.mark.parametrize("stage", ["consumer", "dispatcher", "returned"])
async def test_worker_failure_restarts_whole_epoch_not_detached(tmp_path, stage):
    runtime, client, consumers, dispatcher, settings = harness(tmp_path)
    original = consumers[0].run.side_effect
    if stage in ("consumer", "returned"):
        calls = 0

        async def first_run():
            nonlocal calls
            calls += 1
            if calls == 1:
                if stage == "consumer":
                    raise OSError("fetch failed")
                return
            await original()

        consumers[0].run.side_effect = first_run
    else:
        dispatcher.tick.side_effect = [OSError("discovery failed"), 0]

        # Repeat successful ticks once the first failure is consumed.
        async def tick():
            if dispatcher.tick.await_count == 1:
                raise OSError("discovery failed")
            return 0

        dispatcher.tick.side_effect = tick
    worker = asyncio.create_task(runtime.run())
    try:
        await until(lambda: client.setup_maintenance.await_count >= 2)
        await until(lambda: is_ready(settings))
        assert all(c.run.await_count >= 2 for c in consumers)
    finally:
        await cancel(worker)
    assert all(not c.is_ready for c in consumers)


async def test_shutdown_during_setup_cancels_and_disposes_database(tmp_path):
    runtime, client, _, _, settings = harness(tmp_path)
    client.setup_maintenance.side_effect = lambda: None
    entered = asyncio.Event()

    async def setup():
        entered.set()
        await asyncio.Event().wait()

    client.setup_maintenance.side_effect = setup
    close_database = AsyncMock()
    stop = asyncio.Event()
    task = asyncio.create_task(serve(runtime=runtime, stop=stop, close_database=close_database))
    await entered.wait()
    stop.set()
    await asyncio.wait_for(task, 2)
    client.close.assert_awaited_once()
    close_database.assert_awaited_once()
    assert not is_ready(settings)


async def test_serve_propagates_fatal_failure_and_disposes_database(tmp_path):
    runtime, client, _, _, _ = harness(tmp_path)
    client.setup_maintenance.side_effect = BadRequestError()
    close_database = AsyncMock()
    with pytest.raises(BadRequestError):
        await serve(runtime=runtime, stop=asyncio.Event(), close_database=close_database)
    close_database.assert_awaited_once()


async def test_disabled_entrypoint_never_builds_runtime(monkeypatch):
    factory = MagicMock()
    monkeypatch.setattr("depends.normalization_runtime.build_runtime", factory)
    with pytest.raises(SystemExit, match="disabled"):
        await entrypoint.main()
    factory.assert_not_called()


@pytest.mark.parametrize("value", ["0", "nan", "inf", "garbage", str(time() - 10), str(time() + 100)])
def test_probe_rejects_stale_invalid_or_future_heartbeat(tmp_path, value):
    _, _, _, _, settings = harness(tmp_path)
    path = Path(settings.normalization_readiness_file)
    assert not is_ready(settings)
    path.write_text(value)
    assert not is_ready(settings)
    path.write_text(str(time()))
    assert is_ready(settings)
    settings.normalization_enabled = False
    assert not is_ready(settings)


@pytest.mark.parametrize(
    "field,value",
    [
        ("normalization_scan_batch_size", 501),
        ("normalization_delete_batch_size", 0),
        ("normalization_relay_batch_size", 501),
        ("normalization_tenant_page_size", 0),
        ("normalization_tick_seconds", 0),
        ("normalization_tick_seconds", float("nan")),
        ("normalization_tick_seconds", float("inf")),
    ],
)
def test_settings_bounds(field, value):
    with pytest.raises(ValidationError):
        NormalizationSettings(**{field: value})


def test_factory_prepared_dispatcher_does_not_connect_or_start(monkeypatch):
    from containers.application import clients_container

    client = MagicMock()
    with clients_container.nats_client.override(client):
        runtime = build_runtime(settings=NormalizationSettings())
    assert runtime.client is client
    assert len(runtime.consumers) == 2
    assert not runtime.is_ready
    client.connect.assert_not_called()
    assert not NormalizationSettings().normalization_enabled


@pytest.mark.parametrize("subject", [MERGE_SUBJECT, CLEANUP_SUBJECT])
async def test_consumer_readiness_only_after_bind_and_resets_on_cancel(subject):
    from settings import NatsSettings

    client = MagicMock()
    bound = asyncio.Event()
    binding = asyncio.Event()
    sub = MagicMock(unsubscribe=AsyncMock())

    async def bind(**kwargs):
        bound.set()
        await binding.wait()
        return sub

    async def fetch(**kwargs):
        await asyncio.Event().wait()

    sub.fetch = AsyncMock(side_effect=fetch)
    client.jetstream.pull_subscribe_bind = AsyncMock(side_effect=bind)
    consumer = MaintenanceResultConsumer(client=client, settings=NatsSettings(), handler=MagicMock(), subject=subject)
    task = asyncio.create_task(consumer.run())
    try:
        await bound.wait()
        assert not consumer.is_ready
        binding.set()
        await until(lambda: consumer.is_ready)
        consumer._storage_ok = False
        assert not consumer.is_ready
    finally:
        await cancel(task)
    assert not consumer.is_ready
    sub.unsubscribe.assert_awaited_once()


async def test_supervisor_escalates_fifth_failure_without_secret_text(tmp_path, monkeypatch, caplog):
    runtime, client, _, _, _ = harness(tmp_path)
    client.setup_maintenance.side_effect = OSError("secret diagnostic")
    delays = []

    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 5:
            raise asyncio.CancelledError

    monkeypatch.setattr("maintenance.runtime.asyncio.sleep", sleep)
    with caplog.at_level(logging.WARNING), pytest.raises(asyncio.CancelledError):
        await runtime.run()
    assert delays == [1, 2, 4, 8, 16]
    assert client.close.await_count == 5
    assert any(record.levelno == logging.ERROR for record in caplog.records)
    assert "secret diagnostic" not in caplog.text


async def test_storage_retry_revokes_consumer_readiness_until_commit_and_ack():
    from settings import NatsSettings

    handler = MagicMock(handle=AsyncMock(side_effect=[OSError("database unavailable"), None]))
    consumer = MaintenanceResultConsumer(
        client=MagicMock(), settings=NatsSettings(), handler=handler, subject=MERGE_SUBJECT
    )
    consumer._bound = True
    assert consumer.is_ready
    msg = MagicMock(subject=MERGE_SUBJECT, data=b"not used by mocked handler")
    msg.metadata.num_delivered = 1
    msg.nak = AsyncMock()
    msg.ack = AsyncMock()
    await consumer.process(msg)
    assert not consumer.is_ready
    msg.nak.assert_awaited_once_with(delay=1)
    await consumer.process(msg)
    msg.ack.assert_awaited_once()
    assert consumer.is_ready


async def test_entrypoint_wires_once_removes_signals_and_redacts_fatal_error(monkeypatch, caplog):
    import maintenance.runtime as runtime_module

    monkeypatch.setenv("SEO_NORMALIZATION_ENABLED", "true")
    factory = MagicMock(return_value=MagicMock())
    monkeypatch.setattr("depends.normalization_runtime.build_runtime", factory)
    serving = AsyncMock(side_effect=RuntimeError("secret diagnostic"))
    monkeypatch.setattr(runtime_module, "serve", serving)
    monkeypatch.setattr("utils.configure_sentry.configure_sentry", MagicMock())
    loop = asyncio.get_running_loop()
    add = MagicMock()
    remove = MagicMock()
    monkeypatch.setattr(loop, "add_signal_handler", add)
    monkeypatch.setattr(loop, "remove_signal_handler", remove)
    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as error:
        await entrypoint.main()
    assert error.value.code == 1
    factory.assert_called_once()
    serving.assert_awaited_once()
    assert serving.call_args.kwargs["runtime"] is factory.return_value
    assert add.call_count == remove.call_count == 2
    assert "secret diagnostic" not in caplog.text
