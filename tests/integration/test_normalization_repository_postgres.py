"""IT-N-01 repository fixtures; requires migration-backed real PostgreSQL.

Lease/checkpoint CAS and cross-service acceptance belong to S-1c/S-3.
"""

import asyncio
from dataclasses import replace
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from core.entities.normalization import NormalizationSnapshot, PageMapping, RevisionStatus
from models.normalization import normalization_redirects as redirects
from models.normalization import normalization_revisions as revisions
from models.pages import pages
from repositories.normalization_repository import NormalizationRepository

pytestmark = pytest.mark.infrastructure


async def add_page(session: AsyncSession, tenant: UUID, path: str) -> tuple[UUID, int]:
    # Production callers must take this lock BEFORE sequence allocation as well.
    await NormalizationRepository(session).lock_tenant(equestrian_id=tenant)
    row = (
        await session.execute(
            insert(pages)
            .values(id=uuid4(), equestrian_id=tenant, page=path, page_hash=sha256(path.encode()).digest())
            .returning(pages.c.id, pages.c.scan_seq)
        )
    ).one()
    return row[0], row[1]


async def test_keyset_watermark_snapshot_queue_and_tenant_isolation(pg_session: AsyncSession):
    session = pg_session
    repo = NormalizationRepository(session)
    tenant, foreign, first, second = (uuid4() for _ in range(4))
    old = [await add_page(session, tenant, f"/{i}?utm=1") for i in range(9)]
    await add_page(session, foreign, "/foreign")
    snapshot = NormalizationSnapshot(("utm",), ("anchor",))
    one = await repo.create_revision(equestrian_id=tenant, operation_id=first, snapshot=snapshot)
    two = await repo.create_revision(
        equestrian_id=tenant, operation_id=second, snapshot=NormalizationSnapshot(("utm", "x"), ())
    )
    assert (one.revision, two.revision) == (1, 2)
    assert (await repo.get_checkpoint(equestrian_id=tenant, operation_id=second)).high_water is None
    active = await repo.activate_next(equestrian_id=tenant)
    assert active is not None and active.operation_id == first and active.snapshot == snapshot
    checkpoint = await repo.get_checkpoint(equestrian_id=tenant, operation_id=first)
    assert checkpoint.high_water == old[-1][1]
    late = await add_page(session, tenant, "/late")
    found: list[UUID] = []
    while batch := await repo.scan_pages(equestrian_id=tenant, checkpoint=checkpoint, limit=3):
        found.extend(page.id for page in batch)
        checkpoint = replace(checkpoint, cursor=batch[-1].scan_seq)
    assert found == [identity for identity, _ in old]
    assert late[0] not in found
    assert await repo.activate_next(equestrian_id=tenant) is None
    assert await repo.get_revision(equestrian_id=foreign, operation_id=first) is None
    with pytest.raises(ValueError):
        await repo.get_checkpoint(equestrian_id=foreign, operation_id=first)
    await session.execute(update(revisions).where(revisions.c.operation_id == first).values(status="blocked"))
    assert await repo.activate_next(equestrian_id=tenant) is None
    await session.execute(update(revisions).where(revisions.c.operation_id == first).values(status="finalized"))
    next_revision = await repo.activate_next(equestrian_id=tenant)
    assert next_revision is not None and next_revision.operation_id == second
    assert (await repo.get_checkpoint(equestrian_id=tenant, operation_id=second)).high_water == late[1]


async def test_mapping_keyset_retry_conflict_and_foreign_guards(pg_session: AsyncSession):
    session = pg_session
    repo = NormalizationRepository(session)
    tenant, foreign, operation = uuid4(), uuid4(), uuid4()
    source = [await add_page(session, tenant, f"/source/{i}") for i in range(7)]
    target, _ = await add_page(session, tenant, "/canonical")
    foreign_target, _ = await add_page(session, foreign, "/canonical")
    await repo.create_revision(equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot((), ()))
    await repo.activate_next(equestrian_id=tenant)
    items = [PageMapping(identity, target, seq, i // 3) for i, (identity, seq) in enumerate(source)]
    for start in range(0, len(items), 3):
        await repo.store_mappings(equestrian_id=tenant, operation_id=operation, batch=items[start : start + 3])
    await repo.store_mappings(equestrian_id=tenant, operation_id=operation, batch=items[:3])
    found: list[PageMapping] = []
    cursor = 0
    while batch := await repo.get_mappings(equestrian_id=tenant, operation_id=operation, after_seq=cursor, limit=2):
        found.extend(batch)
        cursor = batch[-1].source_scan_seq
    assert found == items
    assert await repo.get_mappings(equestrian_id=foreign, operation_id=operation, limit=2) == []
    for bad in (
        replace(items[0], target_id=foreign_target),
        replace(items[0], part_index=99),
        replace(items[0], source_scan_seq=999),
    ):
        with pytest.raises(ValueError):
            await repo.store_mappings(equestrian_id=tenant, operation_id=operation, batch=[bad])
    assert await repo.get_mappings(equestrian_id=tenant, operation_id=operation, limit=10) == items


async def test_redirect_cycle_chain_compression_and_deleted_alias(pg_session: AsyncSession):
    session = pg_session
    repo = NormalizationRepository(session)
    tenant, foreign = uuid4(), uuid4()
    (a, a_seq), (b, b_seq), (c, c_seq) = [await add_page(session, tenant, f"/{i}") for i in range(3)]
    first, second = uuid4(), uuid4()
    await repo.create_revision(equestrian_id=tenant, operation_id=first, snapshot=NormalizationSnapshot((), ()))
    await repo.activate_next(equestrian_id=tenant)
    with pytest.raises(ValueError, match="cycle"):
        await repo.store_mappings(
            equestrian_id=tenant, operation_id=first, batch=[PageMapping(a, b, a_seq, 0), PageMapping(b, a, b_seq, 0)]
        )
    assert await repo.get_mappings(equestrian_id=tenant, operation_id=first) == []
    await repo.store_mappings(equestrian_id=tenant, operation_id=first, batch=[PageMapping(a, b, a_seq, 0)])
    # Simulate safe physical finalization only for this isolated storage fixture.
    await session.execute(delete(pages).where(pages.c.equestrian_id == tenant, pages.c.id == a))
    await session.execute(update(revisions).where(revisions.c.operation_id == first).values(status="finalized"))
    await repo.create_revision(equestrian_id=tenant, operation_id=second, snapshot=NormalizationSnapshot((), ()))
    await repo.activate_next(equestrian_id=tenant)
    await repo.store_mappings(equestrian_id=tenant, operation_id=second, batch=[PageMapping(b, c, b_seq, 0)])
    assert await repo.resolve_redirect(equestrian_id=tenant, page_id=a) == c
    compressed = await session.scalar(
        select(redirects.c.target_id).where(redirects.c.equestrian_id == tenant, redirects.c.source_id == a)
    )
    assert compressed == c
    with pytest.raises(ValueError):
        await repo.resolve_redirect(equestrian_id=foreign, page_id=a)
    # Existing a -> c means c -> a must be rejected despite a no longer being live.
    with pytest.raises(ValueError):
        await repo.store_mappings(equestrian_id=tenant, operation_id=second, batch=[PageMapping(c, a, c_seq, 0)])


async def test_repository_effects_rollback_with_caller(pg_engine: AsyncEngine):
    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        repo = NormalizationRepository(session)
        await add_page(session, tenant, "/rollback")
        await repo.create_revision(
            equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot((), ())
        )
        active = await repo.activate_next(equestrian_id=tenant)
        assert active is not None and active.status == RevisionStatus.SCANNING
        await session.rollback()
    async with AsyncSession(pg_engine) as session:
        repo = NormalizationRepository(session)
        assert await repo.get_revision(equestrian_id=tenant, operation_id=operation) is None
        assert not await session.scalar(select(pages.c.id).where(pages.c.equestrian_id == tenant))
        revision = await repo.create_revision(
            equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot((), ())
        )
        assert revision.revision == 1


async def test_tenant_lock_serializes_resolve_before_activation_not_other_tenants(pg_engine: AsyncEngine):
    tenant, other, operation = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as setup:
        await NormalizationRepository(setup).create_revision(
            equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot((), ())
        )
        await setup.commit()
    async with AsyncSession(pg_engine) as resolving, AsyncSession(pg_engine) as scanning:
        repo = NormalizationRepository(resolving)
        await repo.lock_tenant(equestrian_id=tenant)
        started = asyncio.Event()

        async def activate():
            started.set()
            return await NormalizationRepository(scanning).activate_next(equestrian_id=tenant)

        task = asyncio.create_task(activate())
        try:
            await asyncio.wait_for(started.wait(), timeout=5)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
            async with AsyncSession(pg_engine) as independent:
                await asyncio.wait_for(
                    NormalizationRepository(independent).lock_tenant(equestrian_id=other), timeout=5
                )
                await independent.rollback()
            _, seq = await add_page(resolving, tenant, "/insert-after-lock-before-activation")
            await resolving.commit()
            active = await asyncio.wait_for(task, timeout=5)
            assert active is not None and active.operation_id == operation
            assert (
                await NormalizationRepository(scanning).get_checkpoint(equestrian_id=tenant, operation_id=operation)
            ).high_water == seq
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await scanning.rollback()


async def test_concurrent_rule_revisions_are_monotonic_and_single_active(pg_engine: AsyncEngine):
    tenant = uuid4()
    first, second = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as one, AsyncSession(pg_engine) as two:
        repo_one, repo_two = NormalizationRepository(one), NormalizationRepository(two)
        revision_one = await repo_one.create_revision(
            equestrian_id=tenant, operation_id=first, snapshot=NormalizationSnapshot(("first",), ())
        )
        started = asyncio.Event()

        async def create_second():
            started.set()
            return await repo_two.create_revision(
                equestrian_id=tenant, operation_id=second, snapshot=NormalizationSnapshot(("second",), ())
            )

        task = asyncio.create_task(create_second())
        try:
            await asyncio.wait_for(started.wait(), timeout=5)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
            await one.commit()
            revision_two = await asyncio.wait_for(task, timeout=5)
            assert (revision_one.revision, revision_two.revision) == (1, 2)
            await two.commit()
            active = await repo_one.activate_next(equestrian_id=tenant)
            assert active is not None and active.operation_id == first
            await one.commit()
            assert await repo_two.activate_next(equestrian_id=tenant) is None
            stored = await repo_two.get_revision(equestrian_id=tenant, operation_id=first)
            assert stored is not None and stored.snapshot.query_params == ("first",)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await two.rollback()
