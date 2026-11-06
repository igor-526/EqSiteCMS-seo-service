"""IT-N-01: migration-backed bounded scan fixtures, not acceptance by collection.

No runtime/scheduler/transport enabled; caller transactions are explicit.
"""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from core.entities.normalization import NormalizationSnapshot, RevisionStatus
from core.services.normalization_scan_service import NormalizationScanService
from core.services.page_normalizer import PageNormalizer
from models.normalization import normalization_mappings as mappings
from models.normalization import normalization_outbox as outbox
from models.normalization import normalization_redirects as redirects
from models.normalization import normalization_revisions as revisions
from models.pages import pages
from repositories.normalization_repository import NormalizationRepository
from repositories.page_repository import PageRepository

pytestmark = pytest.mark.infrastructure


def scan_service(session: AsyncSession, size: int = 2):
    storage = NormalizationRepository(session)
    return storage, NormalizationScanService(storage, PageRepository(session), PageNormalizer(), batch_size=size)


async def test_normalization_scan_multiple_batches_targets_aliases_watermark_and_seal(pg_engine: AsyncEngine):
    tenant, foreign, operation = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        storage, service = scan_service(session)
        async with session.begin():
            await storage.lock_tenant(equestrian_id=tenant)
            source = await PageRepository(session).resolve(
                equestrian_id=tenant, values=[f"/a?u={i}#top" for i in range(5)] + ["/a", "/unchanged"]
            )
            await storage.create_revision(
                equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot(("u",), ("top",))
            )
            await storage.lock_tenant(equestrian_id=foreign)
            other = await PageRepository(session).resolve(equestrian_id=foreign, values=["/a?u=1#top"])
        watermarks = set()
        batches = 0
        while True:
            async with session.begin():
                result = await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
                assert result is not None and result[0] == operation
                watermarks.add(result[1].high_water)
                batches += 1
                if result[1].sealed:
                    break
        assert len(watermarks) == 1 and batches >= 5
        async with session.begin():
            stored = await storage.get_revision(equestrian_id=tenant, operation_id=operation)
            assert stored is not None and stored.status == RevisionStatus.AWAITING_RESULT
            mapped = await storage.get_mappings(equestrian_id=tenant, operation_id=operation)
            assert len(mapped) == 5
            assert {item.target_id for item in mapped} == {source["/a"]}
            for item in mapped:
                assert await storage.resolve_redirect(equestrian_id=tenant, page_id=item.source_id) == source["/a"]
            bodies = (
                (await session.execute(select(outbox.c.payload).where(outbox.c.equestrian_id == tenant)))
                .scalars()
                .all()
            )
            parts = [p for p in bodies if p["kind"] == "mappings"]
            seal = [p for p in bodies if p["kind"] == "seal"]
            assert sorted(p["part_index"] for p in parts) == list(range(len(parts)))
            assert seal[0]["part_count"] == stored.part_count == len(parts)
            assert (
                await session.scalar(select(func.count()).select_from(pages).where(pages.c.equestrian_id == tenant))
                == 7  # noqa: W503
            )
            # Sources are retained until correlated S-3 completion/finalization.
            assert (
                await session.scalar(select(pages.c.id).where(pages.c.equestrian_id == foreign)) == other["/a?u=1#top"]
            )
            assert await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC)) is None


async def test_normalization_scan_new_target_and_all_effects_rollback_on_stale_fence(pg_engine: AsyncEngine):
    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        storage, service = scan_service(session)
        async with session.begin():
            await storage.lock_tenant(equestrian_id=tenant)
            await PageRepository(session).resolve(equestrian_id=tenant, values=["/new?u=1"])
            await storage.create_revision(
                equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot(("u",), ())
            )
            await storage.activate_next(equestrian_id=tenant)
        original = storage.commit_scan_batch

        async def stale(**kwargs):
            kwargs["lease"] = replace(kwargs["lease"], token=uuid4())
            await original(**kwargs)

        with patch.object(storage, "commit_scan_batch", side_effect=stale), pytest.raises(ValueError, match="stale"):
            async with session.begin():
                await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
        async with session.begin():
            assert (await storage.get_checkpoint(equestrian_id=tenant, operation_id=operation)).cursor == 0
            for table in (mappings, redirects, outbox):
                assert (
                    await session.scalar(
                        select(func.count()).select_from(table).where(table.c.equestrian_id == tenant)
                    )
                    == 0  # noqa: W503
                )
            assert (
                await session.scalar(select(func.count()).select_from(pages).where(pages.c.equestrian_id == tenant))
                == 1  # noqa: W503
            )
            result = await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
            assert result is not None and result[1].next_part_index == 1
            watermark = result[1].high_water
            target_seq = await session.scalar(
                select(pages.c.scan_seq).where(pages.c.equestrian_id == tenant, pages.c.page == "/new")
            )
            assert target_seq is not None and watermark is not None
            assert target_seq > watermark
        # Restart with a fresh service/session uses persisted cursor, not all mappings.
    async with AsyncSession(pg_engine) as restarted, restarted.begin():
        storage, service = scan_service(restarted)
        result = await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
        assert result is not None and result[1].sealed
        assert result[1].next_part_index == 1


async def test_normalization_seal_revision_and_outbox_are_one_rollback_boundary(pg_engine: AsyncEngine):
    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        storage, service = scan_service(session)
        async with session.begin():
            await storage.create_revision(
                equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot((), ())
            )
            await storage.activate_next(equestrian_id=tenant)
        result = await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
        assert result is not None and result[1].sealed
        await session.rollback()
        async with session.begin():
            revision = await storage.get_revision(equestrian_id=tenant, operation_id=operation)
            assert revision is not None and revision.status == RevisionStatus.SCANNING and revision.part_count is None
            assert not (await storage.get_checkpoint(equestrian_id=tenant, operation_id=operation)).sealed
            assert (
                await session.scalar(select(func.count()).select_from(outbox).where(outbox.c.equestrian_id == tenant))
                == 0  # noqa: W503
            )
            assert await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC)) is not None
        async with session.begin():
            revision = await storage.get_revision(equestrian_id=tenant, operation_id=operation)
            assert (
                revision is not None and revision.part_count == 0 and revision.status == RevisionStatus.AWAITING_RESULT
            )


async def test_normalization_queued_revision_waits_for_finalized_and_refreshes_watermark(pg_engine: AsyncEngine):
    tenant, first, second = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        storage, service = scan_service(session)
        async with session.begin():
            await storage.create_revision(
                equestrian_id=tenant, operation_id=first, snapshot=NormalizationSnapshot((), ())
            )
            await storage.create_revision(
                equestrian_id=tenant, operation_id=second, snapshot=NormalizationSnapshot(("u",), ())
            )
            result = await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
            assert result is not None and result[0] == first and result[1].sealed
        async with session.begin():
            await storage.lock_tenant(equestrian_id=tenant)
            await PageRepository(session).resolve(equestrian_id=tenant, values=["/later?u=1"])
            assert await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC)) is None
            assert (await storage.get_checkpoint(equestrian_id=tenant, operation_id=second)).high_water is None
            # Simulate S-3 finalization of an empty revision (no sources to delete).
            await session.execute(
                update(revisions)
                .where(revisions.c.equestrian_id == tenant, revisions.c.operation_id == first)
                .values(status="finalized")
            )
        async with session.begin():
            result = await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
            assert result is not None and result[0] == second and result[1].next_part_index == 1
            assert result[1].high_water is not None and result[1].high_water > 0
            stored = await storage.get_revision(equestrian_id=tenant, operation_id=second)
            assert stored is not None and stored.snapshot == NormalizationSnapshot(("u",), ())


async def test_normalization_concurrent_scan_resumes_committed_cursor_without_duplicate_part(pg_engine: AsyncEngine):
    tenant, foreign, operation = uuid4(), uuid4(), uuid4()
    started = asyncio.Event()
    async with AsyncSession(pg_engine) as first:
        storage, service = scan_service(first, size=1)
        async with first.begin():
            await storage.lock_tenant(equestrian_id=tenant)
            await PageRepository(first).resolve(equestrian_id=tenant, values=["/a?u=1", "/b?u=1"])
            await storage.create_revision(
                equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot(("u",), ())
            )
        result = await service.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
        assert result is not None and result[1].next_part_index == 1

        async def concurrent():
            async with AsyncSession(pg_engine) as session, session.begin():
                started.set()
                return await scan_service(session, size=1)[1].run_batch(equestrian_id=tenant, now=datetime.now(UTC))

        task = asyncio.create_task(concurrent())
        try:
            await asyncio.wait_for(started.wait(), 5)
            async with AsyncSession(pg_engine) as other, other.begin():
                assert (
                    await asyncio.wait_for(
                        scan_service(other)[1].run_batch(equestrian_id=foreign, now=datetime.now(UTC)), 5
                    )
                    is None
                )
            await asyncio.sleep(0.05)
            assert not task.done()
            await first.commit()
            second_result = await asyncio.wait_for(task, 5)
            assert second_result is not None and second_result[1].next_part_index == 2
            async with first.begin():
                assert len(await storage.get_mappings(equestrian_id=tenant, operation_id=operation)) == 2
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_normalization_durable_lease_prevents_scan_until_reclaim(pg_engine: AsyncEngine):
    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        storage, service = scan_service(session)
        now = datetime.now(UTC)
        async with session.begin():
            await storage.create_revision(
                equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot((), ())
            )
            await storage.activate_next(equestrian_id=tenant)
            lease = await storage.claim_scan(
                equestrian_id=tenant, operation_id=operation, now=now, duration=timedelta(seconds=30)
            )
            assert lease is not None
        async with session.begin():
            assert await service.run_batch(equestrian_id=tenant, now=now) is None
        async with session.begin():
            # Logical now beyond old lease; new lease still passes real DB clock fence.
            result = await service.run_batch(equestrian_id=tenant, now=now + timedelta(seconds=31))
            assert result is not None and result[1].sealed
