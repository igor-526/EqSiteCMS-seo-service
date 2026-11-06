"""Real transaction adapter -> durable inbox, migration-backed PostgreSQL only."""

import json
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from clients.nats.maintenance_results import CLEANUP_SUBJECT, InvalidMaintenanceResult, MaintenanceResultHandler
from core.services.normalization_results import CLEANUP_CONSUMER
from repositories.normalization_repository import NormalizationRepository
from repositories.normalization_transaction import NormalizationTransaction

pytestmark = pytest.mark.infrastructure


@pytest.mark.parametrize("invalid", ["missing", "mismatch", "wrong_uuid5", "numeric_timestamp"])
async def test_cleanup_commit_survives_handler_restart_and_conflict_rollback(pg_engine, invalid):
    tenant, operation = uuid4(), uuid4()
    identity = uuid5(NAMESPACE_URL, f"eqcms:maintenance:v1:{CLEANUP_SUBJECT}:{operation}:completed")
    headers = {"Nats-Msg-Id": str(identity)}
    data = dict(
        schema_version=1,
        message_id=str(identity),
        operation_id=str(operation),
        equestrian_id=str(tenant),
        occurred_at="2026-01-01T00:00:00Z",
        cleanup_seq=1,
        status="completed",
    )
    factory = async_sessionmaker(pg_engine, expire_on_commit=False, autoflush=False)
    handler = MaintenanceResultHandler(transaction=NormalizationTransaction(factory))
    invalid_data = dict(data)
    invalid_headers = dict(headers)
    if invalid == "missing":
        invalid_headers = None
    elif invalid == "mismatch":
        invalid_headers["Nats-Msg-Id"] = str(uuid4())
    elif invalid == "numeric_timestamp":
        invalid_data["occurred_at"] = 1767225600
    else:
        invalid_data["message_id"] = str(uuid4())
        invalid_headers["Nats-Msg-Id"] = invalid_data["message_id"]
    with pytest.raises(InvalidMaintenanceResult):
        await handler.handle(
            subject=CLEANUP_SUBJECT, payload=json.dumps(invalid_data).encode(), headers=invalid_headers
        )
    async with AsyncSession(pg_engine) as session:
        assert (
            await NormalizationRepository(session).get_inbox_result(
                consumer=CLEANUP_CONSUMER, equestrian_id=tenant, operation_id=operation
            )
            is None
        )
    await handler.handle(subject=CLEANUP_SUBJECT, payload=json.dumps(data).encode(), headers=headers)
    # New process dependencies/session: no in-memory correlation or dedup state.
    restarted = MaintenanceResultHandler(transaction=NormalizationTransaction(factory))
    await restarted.handle(subject=CLEANUP_SUBJECT, payload=json.dumps(data, indent=2).encode(), headers=headers)
    data["cleanup_seq"] = 2
    with pytest.raises(InvalidMaintenanceResult):
        await restarted.handle(subject=CLEANUP_SUBJECT, payload=json.dumps(data).encode(), headers=headers)
    async with AsyncSession(pg_engine) as session:
        saved = await NormalizationRepository(session).get_inbox_result(
            consumer=CLEANUP_CONSUMER, equestrian_id=tenant, operation_id=operation
        )
        assert saved is not None
        assert saved.message_id == identity
        assert json.loads(saved.payload_json)["cleanup_seq"] == 1
