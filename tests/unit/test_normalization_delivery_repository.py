"""UT-N-04: durable identities and bounded fenced batches, not PG acceptance."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.normalization import (
    MaintenanceMessage,
    NormalizationRevision,
    NormalizationSnapshot,
    OutboxClaim,
    PageMapping,
    RevisionStatus,
    ScanCheckpoint,
    StorageLease,
)
from core.protocols.repositories.normalization_repository import NormalizationRepositoryProtocol
from repositories.normalization_repository import NormalizationRepository

NOW = datetime.now(UTC)
DURATION = timedelta(minutes=5)


def message(tenant=None, operation=None, part=None):
    payload = {"operation_id": str(operation or uuid4()), "version": 1}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return MaintenanceMessage(
        tenant or uuid4(),
        operation or uuid4(),
        uuid4(),
        "mappings",
        encoded,
        sha256(encoded.encode()).hexdigest(),
        part,
    )


def row(message):
    return dict(
        equestrian_id=message.equestrian_id,
        operation_id=message.operation_id,
        message_id=message.message_id,
        payload_hash=message.payload_hash,
        payload=json.loads(message.payload_json),
        kind=message.kind,
        part_index=message.part_index,
    )


def result(rows=()):
    mock = MagicMock()
    mock.first.return_value = rows[0] if rows else None
    mock.mappings.return_value.first.return_value = rows[0] if rows else None
    mock.mappings.return_value.one.return_value = rows[0] if rows else None
    mock.mappings.return_value.all.return_value = list(rows)
    return mock


def sql(statement):
    return str(statement.compile(dialect=postgresql.dialect()))


@pytest.fixture
def repo():
    session = AsyncMock(spec=AsyncSession)
    # begin_nested is a synchronous factory returning an async context manager.
    session.begin_nested = MagicMock(
        return_value=MagicMock(__aenter__=AsyncMock(), __aexit__=AsyncMock(return_value=False))
    )
    instance: Any = NormalizationRepository(session)
    instance.lock_tenant = AsyncMock()
    instance.get_revision = AsyncMock(
        return_value=NormalizationRevision(uuid4(), uuid4(), 1, RevisionStatus.SCANNING, NormalizationSnapshot((), ()))
    )
    return instance


def test_concrete_storage_implements_protocol():
    for name in NormalizationRepositoryProtocol.__dict__:
        if not name.startswith("_"):
            assert callable(getattr(NormalizationRepository, name, None)), name


@pytest.mark.parametrize("invalid", ["hash", "json", "array", "nan", "oversize"])
async def test_invalid_payload_is_rejected_before_sql(repo, invalid):
    msg = message()
    payload = {
        "hash": msg.payload_json,
        "json": "{",
        "array": "[]",
        "nan": '{"n":NaN}',
        "oversize": '{"s":"' + "x" * (256 * 1024) + '"}',
    }[invalid]
    msg = replace(msg, payload_json=payload, payload_hash="0" * 64)
    for method in (repo.accept_inbox, repo.enqueue_outbox):
        with pytest.raises(ValueError):
            await method(**({"consumer": "seo"} if method == repo.accept_inbox else {}), message=msg)
    repo.session.execute.assert_not_awaited()


@pytest.mark.parametrize("duplicate", [False, True])
async def test_inbox_dedup_survives_window_without_overwrite(repo, duplicate):
    msg = message()
    repo.session.execute.side_effect = [result([] if duplicate else [(msg.message_id,)]), result([row(msg)])]
    assert await repo.accept_inbox(consumer="seo", message=msg) is not duplicate
    statements = [sql(call.args[0]) for call in repo.session.execute.await_args_list]
    assert "ON CONFLICT ON CONSTRAINT uq_normalization_inbox_message DO NOTHING" in statements[0]
    assert "consumer =" in statements[1] and "message_id =" in statements[1]
    repo.session.commit.assert_not_awaited()


@pytest.mark.parametrize("field", ["equestrian_id", "operation_id", "payload_hash", "payload"])
async def test_inbox_mismatched_identity_never_overwrites(repo, field):
    msg = message()
    previous = row(msg)
    previous[field] = {} if field == "payload" else uuid4()
    repo.session.execute.side_effect = [result(), result([previous])]
    with pytest.raises(ValueError, match="identity conflict"):
        await repo.accept_inbox(consumer="seo", message=msg)
    assert repo.session.execute.await_count == 2


async def test_inbox_processed_marker_requires_correlation(repo):
    msg = message()
    repo.session.execute.return_value = result([row(msg)])
    await repo.mark_inbox_processed(consumer="seo", message=msg, now=NOW)
    assert "FOR UPDATE" in sql(repo.session.execute.await_args_list[0].args[0])
    assert "processed_at IS NULL" in sql(repo.session.execute.await_args_list[1].args[0])
    repo.session.execute.return_value = result()
    with pytest.raises(ValueError):
        await repo.mark_inbox_processed(consumer="seo", message=msg, now=NOW)


@pytest.mark.parametrize("conflict", [False, True])
async def test_outbox_exact_identity_and_natural_key(repo, conflict):
    msg = message(part=0)
    previous = row(msg)
    if conflict:
        previous["message_id"] = uuid4()
    repo.session.execute.side_effect = [result(), result([previous])]
    if conflict:
        with pytest.raises(ValueError, match="identity conflict"):
            await repo.enqueue_outbox(message=msg)
    else:
        await repo.enqueue_outbox(message=msg)
    assert "ON CONFLICT DO NOTHING" in sql(repo.session.execute.await_args_list[0].args[0])
    repo.session.commit.assert_not_awaited()


async def test_claim_scan_scoped_expiring_and_monotonic(repo):
    repo.session.execute.return_value = result([(3,)])
    lease = await repo.claim_scan(equestrian_id=uuid4(), operation_id=uuid4(), now=NOW, duration=DURATION)
    assert lease is not None and lease.attempts == 3 and lease.until == NOW + DURATION
    query = sql(repo.session.execute.await_args.args[0])
    assert "equestrian_id =" in query and "operation_id =" in query
    assert "lease_until <=" in query and "attempts=(normalization_scans.attempts +" in query
    repo.session.execute.return_value = result()
    assert await repo.claim_scan(equestrian_id=uuid4(), operation_id=uuid4(), now=NOW, duration=DURATION) is None


@pytest.mark.parametrize("status", [RevisionStatus.PENDING, RevisionStatus.BLOCKED, RevisionStatus.FINALIZED])
async def test_claim_scan_rejects_non_executable_states(repo, status):
    repo.get_revision.return_value = replace(repo.get_revision.return_value, status=status)
    assert await repo.claim_scan(equestrian_id=uuid4(), operation_id=uuid4(), now=NOW, duration=DURATION) is None
    repo.session.execute.assert_not_awaited()


@pytest.mark.parametrize("invalid", ["token", "checkpoint", "expiry", "attempts"])
async def test_fence_checks_token_clock_attempts_and_full_checkpoint(repo, invalid):
    lease = StorageLease(uuid4(), NOW + DURATION, 2)
    checkpoint = ScanCheckpoint(high_water=10)
    data = dict(cursor=0, high_water=10, delete_cursor=0, next_part_index=0, sealed=0, lease_until=lease.until)
    if invalid == "checkpoint":
        data["cursor"] = 1
    repo.session.execute.return_value = result([data] if invalid == "checkpoint" else [])
    with pytest.raises(ValueError, match="stale"):
        await repo._fence_scan(uuid4(), uuid4(), lease, checkpoint, NOW)
    query = sql(repo.session.execute.await_args.args[0])
    assert "lease_token =" in query and "attempts =" in query
    assert "clock_timestamp()" in query and "FOR UPDATE" in query


async def test_scan_mapping_outbox_checkpoint_share_savepoint_and_final_clock_fence(repo):
    tenant, operation = uuid4(), uuid4()
    expected = ScanCheckpoint(high_water=10)
    checkpoint = replace(expected, cursor=5, next_part_index=1)
    lease = StorageLease(uuid4(), NOW + DURATION, 1)
    item = PageMapping(uuid4(), uuid4(), 3, 0)
    msg = message(tenant, operation, 0)
    repo._fence_scan = AsyncMock()
    repo.store_mappings = AsyncMock()
    repo.enqueue_outbox = AsyncMock()
    repo.session.execute.return_value = result([(operation,)])
    await repo.commit_scan_batch(
        equestrian_id=tenant,
        operation_id=operation,
        lease=lease,
        expected=expected,
        checkpoint=checkpoint,
        mappings=[item],
        messages=[msg],
        now=NOW,
    )
    repo.session.begin_nested.assert_called_once()
    repo._fence_scan.assert_awaited_once_with(tenant, operation, lease, expected, NOW)
    repo.store_mappings.assert_awaited_once_with(equestrian_id=tenant, operation_id=operation, batch=[item])
    repo.enqueue_outbox.assert_awaited_once_with(message=msg)
    update = repo.session.execute.await_args.args[0]
    assert "clock_timestamp()" in sql(update)
    assert update.compile().params["lease_token"] is None
    repo.session.commit.assert_not_awaited()
    repo.session.rollback.assert_not_awaited()
    # Lease loss after effects must escape the savepoint, not leave half a batch.
    repo.session.execute.return_value = result()
    with pytest.raises(ValueError, match="expired during batch"):
        await repo.commit_scan_batch(
            equestrian_id=tenant,
            operation_id=operation,
            lease=lease,
            expected=expected,
            checkpoint=checkpoint,
            mappings=[item],
            messages=[msg],
            now=NOW,
        )
    assert repo.session.begin_nested.return_value.__aexit__.await_args.args[0] is ValueError


@pytest.mark.parametrize(
    "invalid", ["backward", "watermark", "delete", "part", "source", "tenant", "no_outbox", "sealed"]
)
async def test_batch_invalid_checkpoint_precedes_all_effects(repo, invalid):
    tenant, operation = uuid4(), uuid4()
    expected = ScanCheckpoint(cursor=2, high_water=10)
    checkpoint = replace(expected, cursor=5, next_part_index=1)
    item = PageMapping(uuid4(), uuid4(), 3, 0)
    messages = [message(tenant, operation, 0)]
    if invalid == "backward":
        checkpoint = replace(checkpoint, cursor=1)
    elif invalid == "watermark":
        checkpoint = replace(checkpoint, high_water=11)
    elif invalid == "delete":
        checkpoint = replace(checkpoint, delete_cursor=1)
    elif invalid == "part":
        item = replace(item, part_index=1)
    elif invalid == "source":
        item = replace(item, source_scan_seq=1)
    elif invalid == "tenant":
        messages = [replace(messages[0], equestrian_id=uuid4())]
    elif invalid == "no_outbox":
        messages = []
    else:
        expected = ScanCheckpoint(cursor=10, high_water=10, sealed=True)
    with pytest.raises(ValueError):
        await repo.commit_scan_batch(
            equestrian_id=tenant,
            operation_id=operation,
            lease=StorageLease(uuid4(), NOW + DURATION, 1),
            expected=expected,
            checkpoint=checkpoint,
            mappings=[item],
            messages=messages,
            now=NOW,
        )
    repo.session.execute.assert_not_awaited()
    repo.session.begin_nested.assert_not_called()


async def test_outbox_claim_skip_locked_and_puback_fenced(repo):
    msg = message()
    data = dict(row(msg), attempts=4)
    repo.session.execute.side_effect = [result([data]), result()]
    claims = await repo.claim_outbox(equestrian_id=msg.equestrian_id, now=NOW, duration=DURATION, limit=2)
    assert len(claims) == 1 and claims[0].lease.attempts == 5
    assert claims[0].message == msg
    query = sql(repo.session.execute.await_args_list[0].args[0])
    assert "SKIP LOCKED" in query and "LIMIT" in query and "published_at IS NULL" in query
    repo.session.execute.side_effect = None
    repo.session.execute.return_value = result([(msg.message_id,)])
    assert await repo.finish_outbox(claim=claims[0], now=NOW, published=True)
    query = sql(repo.session.execute.await_args.args[0])
    assert "clock_timestamp()" in query and "lease_token =" in query and "attempts =" in query
    repo.session.execute.return_value = result()
    assert not await repo.finish_outbox(claim=claims[0], now=NOW, published=True)


async def test_outbox_retry_keeps_intent_without_cap(repo):
    claim = OutboxClaim(message(), StorageLease(uuid4(), NOW + DURATION, 999))
    repo.session.execute.return_value = result([(claim.message.message_id,)])
    assert await repo.finish_outbox(claim=claim, now=NOW, published=False, retry_after=timedelta(seconds=60))
    params = repo.session.execute.await_args.args[0].compile().params
    assert params["next_attempt_at"] == NOW + timedelta(seconds=60)
    assert "published_at" not in params
    assert params["lease_token"] is None
    with pytest.raises(ValueError):
        await repo.finish_outbox(claim=claim, now=NOW, published=False, retry_after=timedelta(seconds=61))


@pytest.mark.parametrize("cleanup", [False, True])
async def test_scan_seal_updates_revision_in_same_savepoint_before_final_fence(repo, cleanup):
    tenant, operation = uuid4(), uuid4()
    expected = ScanCheckpoint(cursor=10, high_water=10, next_part_index=4)
    checkpoint = replace(expected, sealed=True)
    revision = repo.get_revision.return_value
    if cleanup:
        repo.get_revision.return_value = replace(
            revision, status=RevisionStatus.AWAITING_CLEANUP, cleanup_operation_id=uuid4(), cleanup_seq=8
        )
    repo._fence_scan = AsyncMock()
    repo.enqueue_outbox = AsyncMock()
    repo.session.execute.side_effect = [result(), result([(operation,)])]
    kwargs = dict(
        equestrian_id=tenant,
        operation_id=operation,
        lease=StorageLease(uuid4(), NOW + DURATION, 1),
        expected=expected,
        checkpoint=checkpoint,
        mappings=[],
        messages=[message(tenant, operation)],
        now=NOW,
    )
    await repo.commit_scan_batch(**kwargs)
    statements = [call.args[0] for call in repo.session.execute.await_args_list]
    params = statements[0].compile().params
    assert params["part_count"] == 4
    assert params["status"] == ("awaiting_cleanup" if cleanup else "awaiting_result")
    assert "normalization_revisions.equestrian_id" in sql(statements[0])
    assert "clock_timestamp()" in sql(statements[-1])
    repo.session.execute.side_effect = [result(), result()]
    with pytest.raises(ValueError, match="expired during batch"):
        await repo.commit_scan_batch(**kwargs)
    assert repo.session.begin_nested.return_value.__aexit__.await_args.args[0] is ValueError
    repo.session.commit.assert_not_awaited()


async def test_oldest_unfinished_revision_selection_is_tenant_scoped(repo):
    tenant, operation = uuid4(), uuid4()
    repo.session.scalar.return_value = operation
    active = await repo.get_active_revision(equestrian_id=tenant)
    assert active == repo.get_revision.return_value
    query = sql(repo.session.scalar.await_args.args[0])
    assert "normalization_revisions.equestrian_id =" in query
    assert "normalization_revisions.status !=" in query
    assert "ORDER BY normalization_revisions.revision" in query and "LIMIT" in query
    repo.get_revision.assert_awaited_once_with(equestrian_id=tenant, operation_id=operation)
    repo.get_revision.reset_mock()
    repo.session.scalar.return_value = None
    assert await repo.get_active_revision(equestrian_id=tenant) is None
    repo.get_revision.assert_not_awaited()
