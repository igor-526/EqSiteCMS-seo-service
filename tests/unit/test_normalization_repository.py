"""UT-N-01/04/05 storage contracts, not PostgreSQL acceptance."""

from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.normalization import (
    NormalizationRevision,
    NormalizationSnapshot,
    PageMapping,
    RevisionStatus,
    ScanCheckpoint,
)
from repositories.normalization_repository import MAX_BATCH, MAX_REDIRECT_HOPS, NormalizationRepository


def result(rows=()):
    mock = MagicMock()
    mock.mappings.return_value.first.return_value = rows[0] if rows else None
    mock.mappings.return_value.__iter__.return_value = iter(rows)
    mock.tuples.return_value.all.return_value = rows
    return mock


def sql(statement):
    return str(statement.compile(dialect=postgresql.dialect()))


@pytest.fixture
def session():
    return AsyncMock(spec=AsyncSession)


async def test_revision_snapshot_and_scan_share_transaction_without_commit(session):
    session.execute.return_value = result()
    session.execute.return_value.scalar_one.return_value = 7
    tenant, operation = uuid4(), uuid4()
    snapshot = NormalizationSnapshot(("utm",), ("anchor",))
    revision = await NormalizationRepository(session).create_revision(
        equestrian_id=tenant, operation_id=operation, snapshot=snapshot
    )
    assert revision == NormalizationRevision(tenant, operation, 7, RevisionStatus.PENDING, snapshot)
    statements = [sql(call.args[0]) for call in session.execute.await_args_list]
    assert "ON CONFLICT DO NOTHING" in statements[0]
    assert "FOR UPDATE" in statements[1]
    assert "last_revision=(normalization_tenants.last_revision +" in statements[2]
    assert "normalization_snapshots" in statements[4]
    assert "normalization_scans" in statements[5]
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


@pytest.mark.parametrize("state", ["scanning", "blocked", "awaiting_result", "deleting", "awaiting_cleanup"])
async def test_active_revision_blocks_next_without_watermark_capture(session, state):
    session.execute.side_effect = [result(), result(), result([{"operation_id": uuid4(), "status": state}])]
    assert await NormalizationRepository(session).activate_next(equestrian_id=uuid4()) is None
    session.scalar.assert_not_awaited()
    assert session.execute.await_count == 3


async def test_activation_captures_watermark_only_after_lock(session):
    tenant, operation = uuid4(), uuid4()
    session.execute.side_effect = [
        result(),
        result(),
        result([{"operation_id": operation, "status": "pending"}]),
        result(),
        result(),
    ]
    session.scalar.return_value = 19
    repo = NormalizationRepository(session)
    repo.get_revision = AsyncMock(return_value="active")
    assert await repo.activate_next(equestrian_id=tenant) == "active"
    query = sql(session.scalar.await_args.args[0])
    assert "max(pages.scan_seq)" in query and "pages.equestrian_id =" in query
    updates = [call.args[0].compile().params for call in session.execute.await_args_list[3:]]
    assert updates == [
        {"high_water": 19, "equestrian_id_1": tenant, "operation_id_1": operation},
        {"status": "scanning", "equestrian_id_1": tenant, "operation_id_1": operation},
    ]


@pytest.mark.parametrize("limit", [0, -1, MAX_BATCH + 1])
async def test_scan_and_mapping_queries_reject_unbounded_reads(session, limit):
    repo = NormalizationRepository(session)
    with pytest.raises(ValueError):
        await repo.scan_pages(equestrian_id=uuid4(), checkpoint=ScanCheckpoint(high_water=10), limit=limit)
    with pytest.raises(ValueError):
        await repo.get_mappings(equestrian_id=uuid4(), operation_id=uuid4(), limit=limit)
    session.execute.assert_not_awaited()


async def test_scan_keyset_has_tenant_upper_lower_bounds_and_no_offset(session):
    session.execute.return_value = result()
    await NormalizationRepository(session).scan_pages(
        equestrian_id=uuid4(), checkpoint=ScanCheckpoint(cursor=5, high_water=20), limit=3
    )
    query = sql(session.execute.await_args.args[0])
    assert "pages.equestrian_id =" in query
    assert "pages.scan_seq >" in query and "pages.scan_seq <=" in query
    assert "ORDER BY pages.scan_seq" in query and "LIMIT" in query and "OFFSET" not in query
    with pytest.raises(ValueError, match="activated"):
        await NormalizationRepository(session).scan_pages(equestrian_id=uuid4(), checkpoint=ScanCheckpoint(), limit=1)


async def test_redirect_chain_compression_is_scoped_and_never_commits(session):
    tenant, source, middle, target = (uuid4() for _ in range(4))
    session.scalar.side_effect = [middle, target, None, target]
    repo = NormalizationRepository(session)
    repo.lock_tenant = AsyncMock()
    assert await repo.resolve_redirect(equestrian_id=tenant, page_id=source) == target
    repo.lock_tenant.assert_awaited_once_with(equestrian_id=tenant)
    statement = session.execute.await_args.args[0]
    assert statement.compile().params["target_id"] == target
    assert statement.compile().params["source_id_1"] == [source, middle]
    assert "normalization_redirects.equestrian_id =" in sql(statement)
    session.commit.assert_not_awaited()


@pytest.mark.parametrize("kind", ["cycle", "foreign", "too_deep"])
async def test_redirect_guards_before_compression(session, kind):
    source = uuid4()
    if kind == "cycle":
        session.scalar.side_effect = [uuid4(), source]
    elif kind == "foreign":
        session.scalar.side_effect = [uuid4(), None, None]
    else:
        session.scalar.side_effect = [uuid4() for _ in range(MAX_REDIRECT_HOPS + 1)]
    repo = NormalizationRepository(session)
    repo.lock_tenant = AsyncMock()
    with pytest.raises(ValueError):
        await repo.resolve_redirect(equestrian_id=uuid4(), page_id=source)
    session.execute.assert_not_awaited()


@pytest.fixture
def mapping_repo(session):
    tenant, operation = uuid4(), uuid4()
    repo = NormalizationRepository(session)
    repo.lock_tenant = AsyncMock()
    repo.get_revision = AsyncMock(
        return_value=NormalizationRevision(
            tenant, operation, 1, RevisionStatus.SCANNING, NormalizationSnapshot((), ())
        )
    )
    repo.get_checkpoint = AsyncMock(return_value=ScanCheckpoint(high_water=10))
    return repo, tenant, operation


@pytest.mark.parametrize(
    "kind", ["foreign", "wrong_seq", "above_water", "cycle", "chain", "alias_conflict", "mapping_conflict"]
)
async def test_mapping_guards_precede_all_effects(session, mapping_repo, kind):
    repo, tenant, operation = mapping_repo
    source, target, other = uuid4(), uuid4(), uuid4()
    item = PageMapping(source, target, 2, 0)
    membership = [(source, 2), (target, 3)]
    if kind == "foreign":
        membership = [(source, 2)]
    elif kind == "wrong_seq":
        item = replace(item, source_scan_seq=1)
    elif kind == "above_water":
        repo.get_checkpoint.return_value = ScanCheckpoint(high_water=1)
    previous = [dict(source_id=source)] if kind == "mapping_conflict" else []
    alias = [(source, other)] if kind == "alias_conflict" else []
    if kind in ("cycle", "chain"):
        alias = [(target, source if kind == "cycle" else other)]
    session.execute.side_effect = [result(membership), result(previous), result(alias)]
    with pytest.raises(ValueError):
        await repo.store_mappings(equestrian_id=tenant, operation_id=operation, batch=[item])
    assert all(sql(call.args[0]).lstrip().startswith("SELECT") for call in session.execute.await_args_list)


async def test_mapping_exact_retry_is_idempotent_and_bounded(session, mapping_repo):
    repo, tenant, operation = mapping_repo
    source, target = uuid4(), uuid4()
    item = PageMapping(source, target, 2, 0)
    previous = dict(
        equestrian_id=tenant,
        operation_id=operation,
        source_id=source,
        target_id=target,
        source_scan_seq=2,
        part_index=0,
    )
    session.execute.side_effect = [
        result([(source, 2), (target, 3)]),
        result([previous]),
        result([(source, target)]),
        result(),
        result(),
    ]
    await repo.store_mappings(equestrian_id=tenant, operation_id=operation, batch=[item])
    assert session.execute.await_count == 5  # fixed count, independent of batch size
    session.scalar.assert_not_awaited()
    for call in session.execute.await_args_list[-2:]:
        assert "ON CONFLICT ON CONSTRAINT" in sql(call.args[0])
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


@pytest.mark.parametrize("kind", ["empty", "duplicate", "oversize", "sealed", "pending"])
async def test_mapping_admission_guards(session, mapping_repo, kind):
    repo, tenant, operation = mapping_repo
    item = PageMapping(uuid4(), uuid4(), 2, 0)
    batch = [item]
    if kind == "empty":
        batch = []
    elif kind == "duplicate":
        batch *= 2
    elif kind == "oversize":
        batch *= MAX_BATCH + 1
    elif kind == "sealed":
        repo.get_checkpoint.return_value = ScanCheckpoint(cursor=10, high_water=10, sealed=True)
    else:
        repo.get_revision.return_value = replace(repo.get_revision.return_value, status=RevisionStatus.PENDING)
    with pytest.raises(ValueError):
        await repo.store_mappings(equestrian_id=tenant, operation_id=operation, batch=batch)
    session.execute.assert_not_awaited()


async def test_large_mapping_batch_uses_fixed_number_of_bounded_queries(session, mapping_repo):
    repo, tenant, operation = mapping_repo
    repo.get_checkpoint.return_value = ScanCheckpoint(high_water=MAX_BATCH)
    target = uuid4()
    batch = [PageMapping(uuid4(), target, seq, 0) for seq in range(1, MAX_BATCH + 1)]
    membership = [(m.source_id, m.source_scan_seq) for m in batch] + [(target, MAX_BATCH + 1)]
    session.execute.side_effect = [result(membership), result(), result(), result(), result()]
    await repo.store_mappings(equestrian_id=tenant, operation_id=operation, batch=batch)
    assert session.execute.await_count == 5
    session.scalar.assert_not_awaited()
    assert len(session.execute.await_args_list[-2].args[0].compile().params) == MAX_BATCH * 6


async def test_mapping_batch_cannot_introduce_chain_or_cycle(session, mapping_repo):
    repo, tenant, operation = mapping_repo
    a, b, c = uuid4(), uuid4(), uuid4()
    for last_target in (a, c):
        session.execute.reset_mock(side_effect=True)
        session.execute.side_effect = [
            result([(a, 1), (b, 2), (c, 3)] if last_target == c else [(a, 1), (b, 2)]),
            result(),
            result(),
        ]
        with pytest.raises(ValueError, match="chain or cycle"):
            await repo.store_mappings(
                equestrian_id=tenant,
                operation_id=operation,
                batch=[PageMapping(a, b, 1, 0), PageMapping(b, last_target, 2, 0)],
            )
        assert session.execute.await_count == 3


async def test_mapping_keyset_query_and_checkpoint_read_are_tenant_scoped(session):
    tenant, operation = uuid4(), uuid4()
    session.execute.return_value = result()
    repo = NormalizationRepository(session)
    assert await repo.get_mappings(equestrian_id=tenant, operation_id=operation, after_seq=9, limit=2) == []
    statement = session.execute.await_args.args[0]
    query = sql(statement)
    assert "normalization_mappings.equestrian_id =" in query
    assert "normalization_mappings.operation_id =" in query
    assert "normalization_mappings.source_scan_seq >" in query
    assert "ORDER BY normalization_mappings.source_scan_seq" in query
    assert "LIMIT" in query and "OFFSET" not in query
    with pytest.raises(ValueError, match="unknown tenant operation"):
        await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation)
    session.execute.return_value = result([dict(cursor=4, high_water=9, delete_cursor=0, next_part_index=2, sealed=0)])
    assert await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation) == ScanCheckpoint(4, 9, 0, 2)
