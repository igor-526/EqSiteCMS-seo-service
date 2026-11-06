"""UT-N-01/02/04/05: bounded orchestration, no transport or PG substitution."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from core.entities.normalization import (
    NormalizationRevision,
    NormalizationSnapshot,
    RevisionStatus,
    ScanCheckpoint,
    ScanPage,
    StorageLease,
)
from core.services.normalization_scan_service import MERGE_SUBJECT, NormalizationScanService
from core.services.page_normalizer import PageNormalizer

NOW = datetime(2026, 11, 2, tzinfo=UTC)


def setup_scan(*, size=500, body=256 * 1024, watermark=20):
    storage, pages = AsyncMock(), AsyncMock()
    tenant, operation = uuid4(), uuid4()
    revision = NormalizationRevision(
        tenant, operation, 3, RevisionStatus.SCANNING, NormalizationSnapshot(("u",), ("top",))
    )
    storage.get_active_revision.return_value = revision
    storage.get_checkpoint.return_value = ScanCheckpoint(high_water=watermark)
    storage.claim_scan.return_value = StorageLease(uuid4(), NOW + timedelta(seconds=30), 1)
    storage.scan_pages.return_value = []
    storage.resolve_redirect.side_effect = lambda **kw: kw["page_id"]
    return (
        storage,
        pages,
        NormalizationScanService(storage, pages, PageNormalizer(), batch_size=size, max_body_bytes=body),
    )


async def run(storage, service):
    return await service.run_batch(equestrian_id=storage.get_active_revision.return_value.equestrian_id, now=NOW)


def committed(storage):
    return storage.commit_scan_batch.await_args.kwargs


@pytest.mark.parametrize("existing", [True, False])
async def test_normalization_scan_snapshot_changed_and_unchanged_paths(existing):
    storage, pages, service = setup_scan()
    revision = storage.get_active_revision.return_value
    target = uuid4()
    source, unchanged = ScanPage(uuid4(), "/a?u=1#top", 2), ScanPage(uuid4(), "/b", 7)
    storage.scan_pages.return_value = [source, unchanged]
    # PageRepository.resolve owns both find and create, under the same tenant TX.
    pages.resolve.return_value = {"/a": target}
    storage.resolve_redirect.side_effect = None
    storage.resolve_redirect.return_value = target if existing else uuid4()
    result = await run(storage, service)
    assert result == (revision.operation_id, ScanCheckpoint(cursor=20, high_water=20, next_part_index=1))
    pages.resolve.assert_awaited_once_with(equestrian_id=revision.equestrian_id, values=["/a"])
    data = committed(storage)
    assert data["mappings"][0].source_id == source.id
    assert data["mappings"][0].target_id == storage.resolve_redirect.return_value
    assert data["mappings"][0].source_scan_seq == 2
    assert data["expected"] == ScanCheckpoint(high_water=20)
    storage.scan_pages.assert_awaited_once_with(
        equestrian_id=revision.equestrian_id, checkpoint=data["expected"], limit=500
    )
    message = data["messages"][0]
    payload = json.loads(message.payload_json)
    assert set(payload) == {
        "schema_version",
        "message_id",
        "operation_id",
        "equestrian_id",
        "occurred_at",
        "normalization_revision",
        "kind",
        "part_index",
        "mappings",
    }
    assert payload["normalization_revision"] == 3 and payload["occurred_at"].endswith("Z")
    assert payload["mappings"] == [
        {"source_page_id": str(source.id), "target_page_id": str(storage.resolve_redirect.return_value)}
    ]
    assert message.message_id == uuid5(
        NAMESPACE_URL, f"eqcms:maintenance:v1:{MERGE_SUBJECT}:{revision.operation_id}:0"
    )
    assert message.payload_hash == sha256(message.payload_json.encode()).hexdigest()
    assert "/a" not in message.payload_json
    storage.enqueue_outbox.assert_not_awaited()  # only atomic storage entry point


async def test_normalization_scan_bounded_keyset_and_new_claim_on_restart():
    storage, pages, service = setup_scan(size=2, watermark=19)
    batch = [ScanPage(uuid4(), f"/{i}?u=1", i) for i in (3, 8)]
    storage.scan_pages.return_value = batch
    targets = {"/3": uuid4(), "/8": uuid4()}
    pages.resolve.side_effect = lambda **kw: {p: targets[p] for p in kw["values"]}
    await run(storage, service)
    first = committed(storage)
    assert first["checkpoint"] == ScanCheckpoint(cursor=8, high_water=19, next_part_index=1)
    storage.get_checkpoint.return_value = first["checkpoint"]
    storage.claim_scan.return_value = replace(first["lease"], token=uuid4(), attempts=2)
    storage.scan_pages.return_value = [ScanPage(uuid4(), "/z", 19)]
    await run(storage, service)
    second = committed(storage)
    assert second["expected"] == first["checkpoint"]
    assert second["checkpoint"] == ScanCheckpoint(cursor=19, high_water=19, next_part_index=1)
    assert second["mappings"] == [] and second["messages"] == []
    assert second["lease"].token != first["lease"].token
    assert storage.activate_next.await_count == 0


async def test_normalization_scan_byte_split_never_skips_unrecorded_source():
    storage, pages, service = setup_scan(size=5, body=650, watermark=5)
    batch = [ScanPage(uuid4(), f"/{i}?u=1", i) for i in range(1, 6)]
    storage.scan_pages.return_value = batch
    targets = {f"/{i}": uuid4() for i in range(1, 6)}
    pages.resolve.side_effect = lambda **kw: {p: targets[p] for p in kw["values"]}
    await run(storage, service)
    first = committed(storage)
    assert 1 <= len(first["mappings"]) < 5
    assert len(first["messages"][0].payload_json.encode()) <= 650
    assert first["checkpoint"].cursor == first["mappings"][-1].source_scan_seq
    cursor = first["checkpoint"].cursor
    storage.get_checkpoint.return_value = first["checkpoint"]
    storage.scan_pages.return_value = batch[cursor:]
    await run(storage, service)
    second = committed(storage)
    assert second["mappings"][0].source_scan_seq == cursor + 1
    assert second["mappings"][0].part_index == 1
    assert second["messages"][0].message_id != first["messages"][0].message_id


@pytest.mark.parametrize("watermark", [0, 20])
async def test_normalization_empty_or_noop_revision_seals_zero_but_never_finalizes(watermark):
    storage, pages, service = setup_scan(watermark=watermark)
    if watermark:
        storage.scan_pages.return_value = [ScanPage(uuid4(), "/a", 7)]
        await run(storage, service)
        assert committed(storage)["messages"] == []
        storage.get_checkpoint.return_value = committed(storage)["checkpoint"]
    await run(storage, service)
    data = committed(storage)
    assert data["checkpoint"] == ScanCheckpoint(cursor=watermark, high_water=watermark, sealed=True)
    assert data["mappings"] == []
    payload = json.loads(data["messages"][0].payload_json)
    assert payload["kind"] == "seal" and payload["part_count"] == 0
    assert "mappings" not in payload and "part_index" not in payload
    assert data["messages"][0].part_index is None
    storage.get_checkpoint.return_value = data["checkpoint"]
    count = storage.commit_scan_batch.await_count
    assert await run(storage, service) is None
    assert storage.commit_scan_batch.await_count == count
    pages.resolve.assert_not_awaited()


async def test_normalization_nonempty_seal_uses_durable_part_count_and_identity():
    storage, _, service = setup_scan()
    storage.get_checkpoint.return_value = ScanCheckpoint(cursor=20, high_water=20, next_part_index=13)
    await run(storage, service)
    first = committed(storage)["messages"][0]
    await run(storage, service)  # simulate retry of rolled-back batch, same clock
    assert committed(storage)["messages"][0] == first
    assert json.loads(first.payload_json)["part_count"] == 13
    revision = storage.get_active_revision.return_value
    assert first.message_id == uuid5(
        NAMESPACE_URL, f"eqcms:maintenance:v1:{MERGE_SUBJECT}:{revision.operation_id}:seal"
    )
    storage.scan_pages.assert_not_awaited()


@pytest.mark.parametrize("status", [RevisionStatus.AWAITING_RESULT, RevisionStatus.BLOCKED, RevisionStatus.DELETING])
async def test_normalization_unfinished_predecessor_blocks_later_revision(status):
    storage, pages, service = setup_scan()
    storage.get_active_revision.return_value = replace(storage.get_active_revision.return_value, status=status)
    assert await run(storage, service) is None
    storage.activate_next.assert_not_awaited()
    storage.claim_scan.assert_not_awaited()
    storage.commit_scan_batch.assert_not_awaited()
    pages.resolve.assert_not_awaited()


async def test_normalization_pending_activation_and_continuation_under_tenant_lock():
    storage, _, service = setup_scan(watermark=0)
    active = storage.get_active_revision.return_value
    storage.get_active_revision.return_value = replace(active, status=RevisionStatus.PENDING)
    storage.activate_next.return_value = active
    await run(storage, service)
    names = [call[0] for call in storage.mock_calls]
    ordered = [names.index(name) for name in ("lock_tenant", "get_active_revision", "activate_next", "claim_scan")]
    assert ordered == sorted(ordered)
    assert committed(storage)["checkpoint"].sealed


async def test_normalization_cleanup_wait_continues_scan_without_authorizing_delete():
    storage, pages, service = setup_scan()
    storage.get_active_revision.return_value = replace(
        storage.get_active_revision.return_value,
        status=RevisionStatus.AWAITING_CLEANUP,
        cleanup_operation_id=uuid4(),
        cleanup_seq=9,
    )
    storage.scan_pages.return_value = [ScanPage(uuid4(), "/a?u=1", 7)]
    pages.resolve.return_value = {"/a": uuid4()}
    await run(storage, service)
    assert len(committed(storage)["mappings"]) == 1
    storage.activate_next.assert_not_awaited()


async def test_normalization_claim_collision_and_idle_do_not_write():
    storage, _, service = setup_scan()
    tenant = storage.get_active_revision.return_value.equestrian_id
    storage.claim_scan.return_value = None
    assert await run(storage, service) is None
    storage.scan_pages.assert_not_awaited()
    storage.get_active_revision.return_value = None
    assert await service.run_batch(equestrian_id=tenant, now=NOW) is None
    storage.commit_scan_batch.assert_not_awaited()


@pytest.mark.parametrize("failure", ["resolve", "fence", "bytes"])
async def test_normalization_failure_escapes_for_caller_transaction_rollback(failure):
    storage, pages, service = setup_scan(body=400 if failure == "bytes" else 256 * 1024)
    storage.scan_pages.return_value = [ScanPage(uuid4(), "/a?u=1", 1)]
    pages.resolve.return_value = {"/a": uuid4()}
    if failure == "resolve":
        pages.resolve.side_effect = ValueError("target conflict")
    elif failure == "fence":
        storage.commit_scan_batch.side_effect = ValueError("stale lease")
    with pytest.raises(ValueError):
        await run(storage, service)
    if failure != "fence":
        storage.commit_scan_batch.assert_not_awaited()
    storage.enqueue_outbox.assert_not_awaited()


async def test_normalization_terminal_source_equal_target_is_noop():
    storage, pages, service = setup_scan()
    source = ScanPage(uuid4(), "/a?u=1", 1)
    storage.scan_pages.return_value = [source]
    pages.resolve.return_value = {"/a": source.id}
    await run(storage, service)
    assert committed(storage)["mappings"] == []
    assert committed(storage)["checkpoint"].next_part_index == 0


@pytest.mark.parametrize("size,body", [(0, 1000), (501, 1000), (1, 0), (1, 262145)])
def test_normalization_scan_invalid_bounds(size, body):
    with pytest.raises(ValueError):
        setup_scan(size=size, body=body)


async def test_normalization_scan_di_uses_caller_session():
    from dependency_injector import providers

    from containers.repositories import RepositoriesContainer
    from containers.services import ServicesContainer
    from repositories.normalization_repository import NormalizationRepository
    from repositories.page_repository import PageRepository

    repositories = RepositoriesContainer()
    services = ServicesContainer(repositories_container=repositories)
    session = object()
    with repositories.session.override(providers.Object(session)):
        scan = services.normalization_scan_service()
        assert isinstance(scan._repository, NormalizationRepository)
        assert isinstance(scan._pages, PageRepository)
        assert scan._repository.session is session
        assert scan._pages.session is session


async def test_normalization_default_count_bound_collapses_500_sources_to_terminal_target():
    storage, pages, service = setup_scan(watermark=900)
    storage.scan_pages.return_value = [ScanPage(uuid4(), f"/a?u={i}", i) for i in range(1, 501)]
    target, alias = uuid4(), uuid4()
    pages.resolve.return_value = {"/a": alias}
    storage.resolve_redirect.side_effect = None
    storage.resolve_redirect.return_value = target
    await run(storage, service)
    data = committed(storage)
    assert len(data["mappings"]) == 500
    assert data["checkpoint"] == ScanCheckpoint(cursor=500, high_water=900, next_part_index=1)
    assert {item.target_id for item in data["mappings"]} == {target}
    assert len(data["messages"]) == 1
    assert len(data["messages"][0].payload_json.encode()) <= 256 * 1024
    storage.scan_pages.assert_awaited_once()


@pytest.mark.parametrize("now,duration", [(NOW.replace(tzinfo=None), timedelta(seconds=30)), (NOW, timedelta(0))])
async def test_normalization_invalid_time_never_acquires_lock(now, duration):
    storage, _, service = setup_scan()
    with pytest.raises(ValueError):
        await service.run_batch(equestrian_id=uuid4(), now=now, duration=duration)
    storage.lock_tenant.assert_not_awaited()
