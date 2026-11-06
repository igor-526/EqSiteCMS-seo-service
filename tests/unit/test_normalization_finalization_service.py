"""UT-N-04/05/06/07: result correlation and durable finalization orchestration."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from core.entities.normalization import (
    MaintenanceMessage,
    NormalizationRevision,
    NormalizationSnapshot,
    RevisionStatus,
    ScanCheckpoint,
    StorageLease,
)
from core.schemas.messaging.maintenance import MergeCompleted, MergeFinalized
from core.services.normalization_finalization_service import FINALIZED_SUBJECT, NormalizationFinalizationService
from core.services.normalization_results import CLEANUP_CONSUMER, parse_result, validate_correlation

NOW = datetime.now(UTC)


def result_message(revision, *, cleanup=False, **fields: Any):
    payload: dict[str, Any] = dict(
        schema_version=1,
        message_id=str(uuid4()),
        operation_id=str(revision.operation_id),
        equestrian_id=str(revision.equestrian_id),
        occurred_at=NOW.isoformat(),
        status="completed",
    )
    payload.update(dict(cleanup_seq=1) if cleanup else dict(normalization_revision=revision.revision, part_count=2))
    payload.update(fields)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return MaintenanceMessage(
        UUID(payload["equestrian_id"]),
        UUID(payload["operation_id"]),
        UUID(payload["message_id"]),
        "cleanup_completed" if cleanup else "merge_completed",
        encoded,
        sha256(encoded.encode()).hexdigest(),
    )


def setup_finalizer(*, sealed=True, superseded=False):
    repo = AsyncMock()
    revision = NormalizationRevision(
        uuid4(),
        uuid4(),
        3,
        RevisionStatus.AWAITING_CLEANUP if superseded else RevisionStatus.AWAITING_RESULT,
        NormalizationSnapshot((), ()),
        2 if sealed else None,
        uuid4() if superseded else None,
        1 if superseded else None,
    )
    cp = ScanCheckpoint(cursor=20 if sealed else 4, high_water=20, next_part_index=2, sealed=sealed)
    options: dict[str, Any] = (
        dict(
            status="superseded_by_delete",
            cleanup_operation_id=str(revision.cleanup_operation_id),
            cleanup_seq=revision.cleanup_seq,
            part_count=None,
        )
        if superseded
        else {}
    )
    msg = result_message(revision, **options)
    cleanup = (
        result_message(revision, cleanup=True, operation_id=str(revision.cleanup_operation_id)) if superseded else None
    )
    repo.get_active_revision.return_value = revision
    repo.get_checkpoint.return_value = cp
    repo.get_inbox_result.side_effect = lambda **kw: cleanup if kw["consumer"] == CLEANUP_CONSUMER else msg
    repo.claim_scan.return_value = StorageLease(uuid4(), NOW + timedelta(seconds=30), 1)
    repo.commit_delete_batch.return_value = cp
    return repo, revision, msg, NormalizationFinalizationService(repo, batch_size=2)


async def run(repo, service):
    return await service.run_batch(equestrian_id=repo.get_active_revision.return_value.equestrian_id, now=NOW)


@pytest.mark.parametrize("duplicate", [True, False])
async def test_inbox_duplicate_is_not_processed_or_delete(duplicate):
    repo, _, message, service = setup_finalizer()
    repo.record_result.return_value = not duplicate
    assert await service.accept_result(message=message, now=NOW) is not duplicate
    repo.record_result.assert_awaited_once_with(message=message, now=NOW)
    repo.commit_delete_batch.assert_not_awaited()
    repo.claim_scan.assert_not_awaited()


@pytest.mark.parametrize("sealed,cleanup", [(False, False), (False, True), (True, False), (True, True)])
async def test_superseded_requires_both_barriers(sealed, cleanup):
    repo, _, msg, service = setup_finalizer(sealed=sealed, superseded=True)
    if not cleanup:
        repo.get_inbox_result.side_effect = lambda **kw: None if kw["consumer"] == CLEANUP_CONSUMER else msg
    result = await run(repo, service)
    assert (result is not None) == (sealed and cleanup)
    assert repo.commit_delete_batch.await_count == int(sealed and cleanup)
    assert repo.claim_scan.await_count == int(sealed and cleanup)
    repo.activate_next.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("normalization_revision", 2),
        ("part_count", 1),
        ("equestrian_id", str(uuid4())),
        ("operation_id", str(uuid4())),
    ],
)
async def test_foreign_unknown_revision_or_incomplete_result_never_deletes(field, value):
    repo, revision, _, service = setup_finalizer()
    repo.get_inbox_result.side_effect = None
    repo.get_inbox_result.return_value = result_message(revision, **{field: value})
    with pytest.raises(ValueError):
        await run(repo, service)
    repo.claim_scan.assert_not_awaited()
    repo.commit_delete_batch.assert_not_awaited()


async def test_cleanup_sequence_mismatch_is_not_max_seq_authorization():
    repo, revision, msg, service = setup_finalizer(superseded=True)
    bad = result_message(revision, cleanup=True, operation_id=str(revision.cleanup_operation_id), cleanup_seq=2)
    repo.get_inbox_result.side_effect = lambda **kw: bad if kw["consumer"] == CLEANUP_CONSUMER else msg
    with pytest.raises(ValueError, match="cleanup correlation"):
        await run(repo, service)
    repo.claim_scan.assert_not_awaited()


@pytest.mark.parametrize(
    "status", [RevisionStatus.PENDING, RevisionStatus.SCANNING, RevisionStatus.BLOCKED, RevisionStatus.FINALIZED]
)
async def test_no_successor_activation_or_delete_for_non_executable_state(status):
    repo, revision, _, service = setup_finalizer()
    repo.get_active_revision.return_value = replace(revision, status=status)
    assert await run(repo, service) is None
    repo.get_checkpoint.assert_not_awaited()
    repo.activate_next.assert_not_awaited()


async def test_sealed_without_result_and_leased_work_waits():
    repo, _, _, service = setup_finalizer()
    repo.get_inbox_result.side_effect = None
    repo.get_inbox_result.return_value = None
    assert await run(repo, service) is None
    repo.claim_scan.assert_not_awaited()
    repo, _, _, service = setup_finalizer()
    repo.claim_scan.return_value = None
    assert await run(repo, service) is None
    repo.commit_delete_batch.assert_not_awaited()


@pytest.mark.parametrize("empty", [True, False])
async def test_finalized_exact_result_and_restart_checkpoint(empty):
    repo, revision, msg, service = setup_finalizer()
    if empty:
        revision = replace(revision, part_count=0)
        repo.get_active_revision.return_value = revision
        repo.get_checkpoint.return_value = ScanCheckpoint(high_water=0, sealed=True)
        msg = result_message(revision, part_count=0)
        repo.get_inbox_result.side_effect = None
        repo.get_inbox_result.return_value = msg
    await run(repo, service)
    data = repo.commit_delete_batch.await_args.kwargs
    terminal = MergeFinalized.model_validate_json(data["finalized"].payload_json)
    assert terminal.result_message_id == msg.message_id
    assert terminal.normalization_revision == revision.revision
    assert terminal.message_id == uuid5(
        NAMESPACE_URL, f"eqcms:maintenance:v1:{FINALIZED_SUBJECT}:{revision.operation_id}:finalized:{msg.message_id}"
    )
    assert data["result"] == msg and data["limit"] == 2
    assert "kind" not in json.loads(data["finalized"].payload_json)
    assert sha256(data["finalized"].payload_json.encode()).hexdigest() == data["finalized"].payload_hash
    cp = replace(repo.get_checkpoint.return_value, delete_cursor=9 if not empty else 0)
    repo.get_checkpoint.return_value = cp
    await run(repo, NormalizationFinalizationService(repo, batch_size=2))
    assert repo.commit_delete_batch.await_args.kwargs["expected"] == cp
    repo.activate_next.assert_not_awaited()


@pytest.mark.parametrize(
    "fields",
    [
        dict(schema_version=True),
        dict(part_count=True),
        dict(normalization_revision=0),
        dict(status="completed", part_count=None),
        dict(status="superseded_by_delete"),
        dict(cleanup_seq=1),
        dict(url="https://private"),
        dict(occurred_at="2026-01-01T00:00:00+02:00"),
    ],
)
async def test_invalid_result_safely_rejected_before_inbox(fields):
    repo, revision, _, service = setup_finalizer()
    with pytest.raises(ValueError, match="^invalid maintenance result$"):
        await service.accept_result(message=result_message(revision, **fields), now=NOW)
    repo.record_result.assert_not_awaited()


@pytest.mark.parametrize("change", ["hash", "tenant", "operation", "message", "part", "kind"])
def test_storage_identity_and_hash_validation(change):
    _, _, msg, _ = setup_finalizer()
    fields: dict[str, dict[str, Any]] = dict(
        hash=dict(payload_hash="0" * 64),
        tenant=dict(equestrian_id=uuid4()),
        operation=dict(operation_id=uuid4()),
        message=dict(message_id=uuid4()),
        part=dict(part_index=0),
        kind=dict(kind="unknown"),
    )
    with pytest.raises(ValueError, match="^invalid maintenance result$"):
        parse_result(replace(msg, **fields[change]))


def test_fullness_deferred_until_seal_but_never_bypassed():
    repo, revision, message, _ = setup_finalizer(sealed=False)
    parsed = parse_result(message)
    assert isinstance(parsed, MergeCompleted)
    validate_correlation(revision, repo.get_checkpoint.return_value, parsed)
    with pytest.raises(ValueError, match="fullness"):
        validate_correlation(
            replace(revision, part_count=3),
            ScanCheckpoint(cursor=20, high_water=20, sealed=True, next_part_index=3),
            parsed,
        )


@pytest.fixture
def delete_repo():
    from unittest.mock import MagicMock

    from sqlalchemy.ext.asyncio import AsyncSession

    from core.entities.normalization import PageMapping
    from repositories.normalization_repository import NormalizationRepository

    _, revision, message, service = setup_finalizer()
    storage = service._repository
    session = AsyncMock(spec=AsyncSession)
    session.begin_nested = MagicMock(
        return_value=MagicMock(__aenter__=AsyncMock(), __aexit__=AsyncMock(return_value=False))
    )
    session.execute.return_value = MagicMock()
    session.execute.return_value.first.return_value = (revision.operation_id,)
    session.scalar.return_value = None
    repo: Any = NormalizationRepository(session)
    repo.lock_tenant = AsyncMock()
    repo._fence_scan = AsyncMock()
    repo._delete_result_ready = AsyncMock(return_value=True)
    repo.get_revision = AsyncMock(return_value=revision)
    repo.get_mappings = AsyncMock(return_value=[PageMapping(uuid4(), uuid4(), 7, 0)])
    repo.enqueue_outbox = AsyncMock()
    repo.mark_inbox_processed = AsyncMock()
    return repo, revision, message, storage


async def delete_call(data, *, limit=2):
    repo, revision, msg, storage = data
    # Obtain the real domain-generated finalized wire body, not a test approximation.
    service = NormalizationFinalizationService(storage, batch_size=limit)
    await run(storage, service)
    kwargs = storage.commit_delete_batch.await_args.kwargs
    return await repo.commit_delete_batch(**kwargs)


@pytest.mark.parametrize("count", [0, 1, 2])
async def test_repository_bounded_source_only_delete_atomic_terminal_and_release(delete_repo, count):
    from sqlalchemy.dialects import postgresql

    from core.entities.normalization import PageMapping

    repo, revision, message, storage = delete_repo
    batch = [PageMapping(uuid4(), uuid4(), n + 1, 0) for n in range(count)]
    repo.get_mappings.return_value = batch
    checkpoint = await delete_call(delete_repo)
    assert checkpoint.delete_cursor == count
    repo.get_mappings.assert_awaited_once_with(
        equestrian_id=revision.equestrian_id, operation_id=revision.operation_id, after_seq=0, limit=2
    )
    statements = [
        str(call.args[0].compile(dialect=postgresql.dialect())) for call in repo.session.execute.await_args_list
    ]
    deletes = [sql for sql in statements if sql.startswith("DELETE")]
    assert len(deletes) == bool(count)
    if count:
        assert "pages.equestrian_id =" in deletes[0] and "pages.id IN" in deletes[0]
        assert "normalization_redirects" not in deletes[0]
    assert "lease_token =" in statements[-1] and "clock_timestamp()" in statements[-1]
    assert repo.enqueue_outbox.await_count == int(count < 2)
    assert repo.mark_inbox_processed.await_count == int(count < 2)
    assert storage.commit_delete_batch.await_args.kwargs["result"] == message
    repo.session.commit.assert_not_awaited()


@pytest.mark.parametrize("reason", ["stale", "barrier", "target", "expiry", "outbox"])
async def test_repository_failure_never_finishes_and_uses_savepoint(delete_repo, reason):
    repo, _, _, _ = delete_repo
    if reason == "stale":
        repo._fence_scan.side_effect = ValueError("stale")
    elif reason == "barrier":
        repo._delete_result_ready.return_value = False
    elif reason == "target":
        repo.session.scalar.return_value = uuid4()
    elif reason == "expiry":
        repo.session.execute.return_value.first.return_value = None
    else:
        repo.enqueue_outbox.side_effect = ValueError("outbox")
    with pytest.raises(ValueError):
        await delete_call(delete_repo)
    if reason in ("stale", "barrier", "target"):
        repo.enqueue_outbox.assert_not_awaited()
        repo.mark_inbox_processed.assert_not_awaited()
    assert repo.session.begin_nested.return_value.__aexit__.await_args.args[0] is ValueError
    repo.session.commit.assert_not_awaited()


@pytest.mark.parametrize("duplicate", [False, True])
@pytest.mark.parametrize("superseded", [False, True])
async def test_repository_durable_result_acceptance_and_redelivery(delete_repo, duplicate, superseded):
    repo, revision, _, _ = delete_repo
    options: dict[str, Any] = (
        dict(status="superseded_by_delete", part_count=None, cleanup_operation_id=str(uuid4()), cleanup_seq=4)
        if superseded
        else {}
    )
    message = result_message(revision, **options)
    repo.get_checkpoint = AsyncMock(
        return_value=ScanCheckpoint(cursor=20, high_water=20, sealed=True, next_part_index=2)
    )
    repo.get_inbox_result = AsyncMock(return_value=message if duplicate else None)
    repo.accept_inbox = AsyncMock(return_value=not duplicate)
    assert await repo.record_result(message=message, now=NOW) is not duplicate
    repo.accept_inbox.assert_awaited_once()
    repo.mark_inbox_processed.assert_not_awaited()  # Merge marker belongs to finalization.
    assert repo.session.execute.await_count == int(superseded)


async def test_repository_unknown_and_conflicting_results_are_not_persisted(delete_repo):
    repo, revision, _, _ = delete_repo
    message = result_message(revision)
    repo.get_inbox_result = AsyncMock(return_value=None)
    repo.get_revision.return_value = None
    repo.accept_inbox = AsyncMock()
    with pytest.raises(ValueError, match="unknown"):
        await repo.record_result(message=message, now=NOW)
    repo.accept_inbox.assert_not_awaited()
    repo.get_revision.return_value = revision
    repo.get_inbox_result.return_value = result_message(revision)
    with pytest.raises(ValueError, match="conflicting"):
        await repo.record_result(message=message, now=NOW)
    repo.accept_inbox.assert_not_awaited()


async def test_repository_retains_early_cleanup_as_processed_ledger(delete_repo):
    repo, revision, _, _ = delete_repo
    repo.get_inbox_result = AsyncMock(return_value=None)
    repo.accept_inbox = AsyncMock(return_value=True)
    message = result_message(revision, cleanup=True)
    assert await repo.record_result(message=message, now=NOW)
    repo.mark_inbox_processed.assert_awaited_once_with(consumer=CLEANUP_CONSUMER, message=message, now=NOW)
    repo.get_revision.assert_not_awaited()


@pytest.mark.parametrize("sealed,cleanup", [(False, False), (False, True), (True, False), (True, True)])
async def test_repository_rechecks_both_barriers_without_trusting_runner(sealed, cleanup):
    from repositories.normalization_repository import NormalizationRepository

    repo, revision, msg, _ = setup_finalizer(sealed=sealed, superseded=True)
    storage: Any = NormalizationRepository(AsyncMock())
    storage.get_revision = AsyncMock(return_value=revision)
    storage.get_inbox_result = repo.get_inbox_result
    if not cleanup:
        storage.get_inbox_result.side_effect = lambda **kw: None if kw["consumer"] == CLEANUP_CONSUMER else msg
    assert await storage._delete_result_ready(
        revision.equestrian_id, revision.operation_id, repo.get_checkpoint.return_value, msg
    ) is (sealed and cleanup)


async def test_repository_refuses_unretained_result_and_wrong_cleanup_sequence():
    from repositories.normalization_repository import NormalizationRepository

    repo, revision, msg, _ = setup_finalizer(superseded=True)
    storage: Any = NormalizationRepository(AsyncMock())
    storage.get_revision = AsyncMock(return_value=revision)
    storage.get_inbox_result = AsyncMock(return_value=None)
    with pytest.raises(ValueError, match="unknown merge"):
        await storage._delete_result_ready(
            revision.equestrian_id, revision.operation_id, repo.get_checkpoint.return_value, msg
        )
    bad = result_message(revision, cleanup=True, operation_id=str(revision.cleanup_operation_id), cleanup_seq=3)
    storage.get_inbox_result.side_effect = lambda **kw: bad if kw["consumer"] == CLEANUP_CONSUMER else msg
    with pytest.raises(ValueError, match="cleanup correlation"):
        await storage._delete_result_ready(
            revision.equestrian_id, revision.operation_id, repo.get_checkpoint.return_value, msg
        )


async def test_inbox_lookup_bounded_scoped_and_includes_processed_ledger():
    from unittest.mock import MagicMock

    from sqlalchemy.dialects import postgresql
    from sqlalchemy.ext.asyncio import AsyncSession

    from core.services.normalization_results import MERGE_CONSUMER
    from repositories.normalization_repository import NormalizationRepository

    _, revision, msg, _ = setup_finalizer()
    session = AsyncMock(spec=AsyncSession)
    session.execute.return_value = MagicMock()
    rows = [dict(message_id=msg.message_id, payload=json.loads(msg.payload_json), payload_hash=msg.payload_hash)]
    session.execute.return_value.mappings.return_value.all.return_value = rows
    repo = NormalizationRepository(session)
    found = await repo.get_inbox_result(
        consumer=MERGE_CONSUMER, equestrian_id=revision.equestrian_id, operation_id=revision.operation_id
    )
    assert found == msg
    stmt = session.execute.await_args.args[0]
    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "LIMIT" in sql and "equestrian_id =" in sql and "operation_id =" in sql and "consumer =" in sql
    assert "processed_at IS NULL" not in sql
    assert stmt.compile().params["param_1"] == 2
    session.execute.return_value.mappings.return_value.all.return_value = rows * 2
    with pytest.raises(ValueError, match="conflicting"):
        await repo.get_inbox_result(
            consumer=MERGE_CONSUMER, equestrian_id=revision.equestrian_id, operation_id=revision.operation_id
        )
