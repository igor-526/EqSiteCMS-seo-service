import asyncio
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from core.entities import ParseDispatch, ParseDispatchStatus, ParseDispatchTrigger
from core.exceptions import ParseQueueUnavailableError, ParseTargetsUnavailableError
from core.protocols.clients.parse_commands import ParseCommandPublisherProtocol
from core.protocols.parsing.provider import ParseTarget
from core.protocols.repositories.parse_dispatch_repository import ParseDispatchRepositoryProtocol
from core.schemas.messaging import schedule_job_id
from core.services.parsing import EnvTimezoneProvider, ParseDispatchService, SlotCalculator
from core.services.parsing.registry import ParseProviderRegistry

DAY = date(2026, 10, 29)
NOW = datetime(2026, 10, 29, tzinfo=UTC)


class DemoProvider:
    code = "demo"
    subject = "commands.parse.demo.run"

    def __init__(self, count: int = 3) -> None:
        self.targets = [ParseTarget(uuid4()) for _ in range(count)]
        self.pages: list[tuple[int, int]] = []
        self.fail_at: int | None = None

    async def list_targets(self, *, offset: int, limit: int) -> tuple[list[ParseTarget], int]:
        self.pages.append((offset, limit))
        if self.fail_at == offset:
            raise ParseTargetsUnavailableError()
        return self.targets[offset : offset + limit], len(self.targets)

    async def is_target(self, *, equestrian_id: UUID) -> bool:
        return ParseTarget(equestrian_id) in self.targets

    def build_params(self, *, trigger: ParseDispatchTrigger) -> dict[str, object]:
        return {"demo": trigger.value}


def claim_for(tenant: UUID, *, attempts: int = 1) -> ParseDispatch:
    return ParseDispatch(
        id=schedule_job_id("demo", tenant, DAY),
        provider="demo",
        equestrian_id=tenant,
        trigger=ParseDispatchTrigger.SCHEDULE,
        schedule_date=DAY,
        status=ParseDispatchStatus.PENDING,
        attempts=attempts,
        last_error_code=None,
        created_at=NOW,
        updated_at=NOW,
        published_at=None,
    )


def graph(
    provider: DemoProvider, *, offset: int = 0, clock: Callable[[], datetime] = lambda: NOW
) -> tuple[ParseDispatchService, AsyncMock, AsyncMock]:
    publisher = AsyncMock(spec=ParseCommandPublisherProtocol)
    repository = AsyncMock(spec=ParseDispatchRepositoryProtocol)
    repository.claim_schedule.side_effect = lambda **kw: claim_for(kw["equestrian_id"])
    repository.mark_published.return_value = True
    registry = ParseProviderRegistry(enabled_codes=[provider.code])
    registry.register(provider)
    service = ParseDispatchService(
        slots=SlotCalculator(timezone_provider=EnvTimezoneProvider(utc_offset_hours=offset), schedule_time=time(3)),
        publisher=publisher,
        repository=repository,
        registry=registry,
        page_size=2,
        max_attempts=2,
        clock=clock,
    )
    return service, publisher, repository


async def test_demo_pages_claim_publish_mark_and_restart_skip() -> None:
    provider = DemoProvider()
    service, publisher, repository = graph(provider)
    events: list[str] = []

    async def claim(**kw: object) -> ParseDispatch:
        events.append("claim")
        return claim_for(UUID(str(kw["equestrian_id"])))

    async def publish(**kw: object) -> None:
        events.append("publish")

    async def mark(**kw: object) -> bool:
        events.append("mark")
        return True

    repository.claim_schedule.side_effect = claim
    publisher.publish.side_effect = publish
    repository.mark_published.side_effect = mark
    assert await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    assert events == ["claim", "publish", "mark"] * 3
    assert provider.pages == [(0, 2), (2, 2)]
    for index, target in enumerate(provider.targets):
        command = publisher.publish.await_args_list[index].kwargs["command"]
        assert command.job_id == schedule_job_id("demo", target.equestrian_id, DAY)
        assert command.provider == "demo" and command.trigger == "schedule"
        assert command.window.mode == "auto" and command.params == {"demo": "schedule"}
        repository.mark_published.assert_any_await(job_id=command.job_id, attempts=1)
        assert repository.claim_schedule.await_args_list[index].kwargs["lease"] == timedelta(minutes=5)
        assert repository.claim_schedule.await_args_list[index].kwargs["max_attempts"] == 2
    repository.claim_schedule.side_effect = None
    repository.claim_schedule.return_value = None
    assert not await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    assert publisher.publish.await_count == 3
    repository.record_manual.assert_not_awaited()


async def test_publish_failure_retry_cap_and_no_secret_logs(caplog: pytest.LogCaptureFixture) -> None:
    provider = DemoProvider(count=1)
    service, publisher, repository = graph(provider)
    tenant = provider.targets[0].equestrian_id
    repository.claim_schedule.side_effect = [claim_for(tenant), claim_for(tenant, attempts=2), None]
    publisher.publish.side_effect = ParseQueueUnavailableError("SECRET")
    for _ in range(3):
        assert not await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    assert publisher.publish.await_count == 2
    assert [c.kwargs["attempts"] for c in repository.mark_failed.await_args_list] == [1, 2]
    assert all(c.kwargs["error_code"] == "nats_unavailable" for c in repository.mark_failed.await_args_list)
    repository.mark_published.assert_not_awaited()
    assert "SECRET" not in caplog.text
    assert "attempts exhausted" in caplog.text


async def test_failed_attempt_retries_same_job_and_uses_new_fence() -> None:
    provider = DemoProvider(count=1)
    service, publisher, repository = graph(provider)
    tenant = provider.targets[0].equestrian_id
    repository.claim_schedule.side_effect = [claim_for(tenant), claim_for(tenant, attempts=2)]
    publisher.publish.side_effect = [ParseQueueUnavailableError(), None]
    assert not await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    assert await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    commands = [c.kwargs["command"] for c in publisher.publish.await_args_list]
    assert commands[0].job_id == commands[1].job_id
    repository.mark_failed.assert_awaited_once_with(
        job_id=commands[0].job_id, attempts=1, error_code="nats_unavailable"
    )
    repository.mark_published.assert_awaited_once_with(job_id=commands[0].job_id, attempts=2)


@pytest.mark.parametrize("fail_at", [0, 2])
async def test_listing_failure_propagates_without_completion(fail_at: int) -> None:
    provider = DemoProvider()
    provider.fail_at = fail_at
    service, publisher, repository = graph(provider)
    with pytest.raises(ParseTargetsUnavailableError):
        await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    assert publisher.publish.await_count == fail_at
    assert repository.claim_schedule.await_count == fail_at


async def test_target_failure_isolated_and_stale_mark_never_reports_complete() -> None:
    provider = DemoProvider()
    service, publisher, repository = graph(provider)
    repository.claim_schedule.side_effect = [
        RuntimeError("SECRET"),
        claim_for(provider.targets[1].equestrian_id),
        claim_for(provider.targets[2].equestrian_id),
    ]
    repository.mark_published.side_effect = [False, RuntimeError("SECRET")]
    assert not await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    assert publisher.publish.await_count == 2
    repository.mark_failed.assert_not_awaited()


async def test_concurrent_claim_loser_does_not_publish() -> None:
    provider = DemoProvider(count=1)
    service, publisher, repository = graph(provider)
    repository.claim_schedule.side_effect = [claim_for(provider.targets[0].equestrian_id), None]
    results = await asyncio.gather(
        *[service.dispatch_scheduled_slot(provider="demo", local_date=DAY) for _ in range(2)]
    )
    assert sorted(results) == [False, True]
    publisher.publish.assert_awaited_once()
    repository.mark_published.assert_awaited_once()


async def test_cancel_propagates_leaving_pending_for_lease_recovery() -> None:
    service, publisher, repository = graph(DemoProvider(count=1))
    publisher.publish.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    repository.mark_failed.assert_not_awaited()
    repository.mark_published.assert_not_awaited()


async def test_empty_targets_complete_without_claim() -> None:
    service, publisher, repository = graph(DemoProvider(count=0))
    assert await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    publisher.publish.assert_not_awaited()
    repository.claim_schedule.assert_not_awaited()


@pytest.mark.parametrize("offset", [0, 3, -12, 14])
@pytest.mark.parametrize("boundary", ["batch", "page", "claim", "before_start"])
async def test_midnight_stops_new_claims_and_publications(offset: int, boundary: str) -> None:
    before = datetime(2026, 10, 29, 23, 59, 59, tzinfo=UTC) - timedelta(hours=offset)
    after = before + timedelta(seconds=2)
    now = before
    provider = DemoProvider(count=3)
    service, publisher, repository = graph(provider, offset=offset, clock=lambda: now)
    # Default page_size=2: batch crosses between targets; page crosses after
    # two already confirmed targets and while awaiting the second list request.
    claims: list[tuple[datetime, date]] = []
    publications: list[datetime] = []
    original_list = provider.list_targets

    async def list_targets(*, offset: int, limit: int) -> tuple[list[ParseTarget], int]:
        nonlocal now
        if boundary == "page" and offset == 2:
            now = after
        return await original_list(offset=offset, limit=limit)

    async def claim(**kw: object) -> ParseDispatch:
        nonlocal now
        claims.append((now, date.fromisoformat(str(kw["schedule_date"]))))
        if boundary == "claim":
            now = after  # claim transaction returns after local midnight
        return claim_for(UUID(str(kw["equestrian_id"])))

    async def publish(**kw: object) -> None:
        nonlocal now
        publications.append(now)
        if boundary == "batch":
            now = after  # PubAck is late; still mark this confirmed command

    provider.list_targets = list_targets  # type: ignore[method-assign]
    repository.claim_schedule.side_effect = claim
    publisher.publish.side_effect = publish
    if boundary == "before_start":
        now = after
    assert not await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    assert all(actual == before and scheduled == DAY for actual, scheduled in claims)
    assert all(actual == before for actual in publications)
    expected_published = {"batch": 1, "page": 2, "claim": 0, "before_start": 0}[boundary]
    assert publisher.publish.await_count == expected_published
    assert repository.mark_published.await_count == expected_published
    assert repository.claim_schedule.await_count == expected_published + (boundary == "claim")
    if boundary == "claim":
        repository.mark_failed.assert_awaited_once_with(
            job_id=claim_for(provider.targets[0].equestrian_id).id, attempts=1, error_code="slot_expired"
        )
    else:
        repository.mark_failed.assert_not_awaited()
    if boundary == "before_start":
        assert provider.pages == []


async def test_scheduled_dispatch_without_timezone_fails_closed() -> None:
    provider = DemoProvider(count=1)
    service, publisher, repository = graph(provider)
    service._slots = None  # Manual-only composition cannot run unguarded scheduled dispatch.
    with pytest.raises(ValueError, match="slot calculator"):
        await service.dispatch_scheduled_slot(provider="demo", local_date=DAY)
    repository.claim_schedule.assert_not_awaited()
    publisher.publish.assert_not_awaited()
