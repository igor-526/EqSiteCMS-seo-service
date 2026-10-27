from dataclasses import asdict
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from core.exceptions import (
    ParseProviderUnknownError,
    ParseQueueUnavailableError,
    ParseTargetMissingError,
    ParseTargetsUnavailableError,
)
from core.protocols.clients.parse_commands import ParseCommandPublisherProtocol
from core.protocols.parsing.provider import ParseProviderProtocol
from core.protocols.repositories.parse_dispatch_repository import ParseDispatchRepositoryProtocol
from core.services.parsing import ParseDispatchService
from core.services.parsing.registry import ParseProviderRegistry


@pytest.mark.parametrize("duplicate", [False, True])
async def test_manual_puback_precedes_audit_and_new_uuid_each_time(duplicate: bool) -> None:
    tenant = uuid4()
    provider = Mock(spec=ParseProviderProtocol, code="demo", subject="commands.parse.demo.run")
    provider.is_target.return_value = True
    provider.build_params.return_value = {"demo": True}
    publisher = AsyncMock(spec=ParseCommandPublisherProtocol)
    publisher.publish.return_value.duplicate = duplicate
    repository = AsyncMock(spec=ParseDispatchRepositoryProtocol)
    registry = ParseProviderRegistry(enabled_codes=["demo"])
    registry.register(provider)
    events = Mock()
    events.attach_mock(publisher, "publisher")
    events.attach_mock(repository, "repository")
    service = ParseDispatchService(
        publisher=publisher,
        repository=repository,
        registry=registry,
        page_size=2,
        max_attempts=3,
        clock=lambda: datetime(2026, 10, 29, tzinfo=UTC),
    )
    first = await service.dispatch_manual(provider="demo", equestrian_id=tenant)
    second = await service.dispatch_manual(provider="demo", equestrian_id=tenant)
    assert first.job_id != second.job_id
    assert asdict(first) == {"job_id": first.job_id, "provider": "demo", "status": "accepted"}
    assert [c[0] for c in events.mock_calls] == [
        "publisher.publish",
        "repository.record_manual",
        "publisher.publish",
        "repository.record_manual",
    ]
    command = publisher.publish.await_args_list[0].kwargs["command"]
    assert command.job_id == first.job_id and command.equestrian_id == tenant
    assert command.trigger == "manual" and command.window.mode == "auto" and command.params == {"demo": True}
    assert command.occurred_at == datetime(2026, 10, 29, tzinfo=UTC)
    repository.record_manual.assert_any_await(job_id=first.job_id, provider="demo", equestrian_id=tenant)


@pytest.mark.parametrize("outcome", ["unknown", "disabled", "missing", "targets_down", "queue_down"])
async def test_manual_errors_never_write_ledger(outcome: str) -> None:
    provider = Mock(spec=ParseProviderProtocol, code="demo", subject="commands.parse.demo.run")
    provider.build_params.return_value = {}
    provider.is_target.return_value = outcome != "missing"
    publisher = AsyncMock(spec=ParseCommandPublisherProtocol)
    repository = AsyncMock(spec=ParseDispatchRepositoryProtocol)
    registry = ParseProviderRegistry(enabled_codes=[] if outcome == "disabled" else ["demo"])
    if outcome != "unknown":
        registry.register(provider)
    error: type[Exception] = ParseProviderUnknownError
    if outcome == "missing":
        error = ParseTargetMissingError
    elif outcome == "targets_down":
        error = ParseTargetsUnavailableError
        provider.is_target.side_effect = error()
    elif outcome == "queue_down":
        error = ParseQueueUnavailableError
        publisher.publish.side_effect = error()
    service = ParseDispatchService(
        publisher=publisher, repository=repository, registry=registry, page_size=2, max_attempts=3
    )
    with pytest.raises(error):
        await service.dispatch_manual(provider="demo", equestrian_id=uuid4())
    repository.record_manual.assert_not_awaited()
    repository.claim_schedule.assert_not_awaited()
    if outcome != "queue_down":
        publisher.publish.assert_not_awaited()
    if outcome in ("unknown", "disabled"):
        provider.is_target.assert_not_awaited()
