"""IT-N-01 request transaction fixtures; migration-backed PostgreSQL only.

No transport/runtime activation. Collection without TEST_DATABASE_URL is not
acceptance of rollback, lock ordering, or concurrent transactions.
"""

import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from core.entities.normalization import NormalizationSnapshot
from core.exceptions import AlreadyExistsError
from core.schemas.api import BlacklistItemInDto, SeoResolveInDto
from core.services import BlacklistedHashService, BlacklistedQueryParamService, PageNormalizer, PageService
from core.services.normalization_service import NormalizationService
from models import blacklisted_hashes, blacklisted_query_params
from models.normalization import normalization_revisions as revisions
from models.normalization import normalization_scans as scans
from models.normalization import normalization_snapshots as snapshots
from repositories.blacklisted_hash_repository import BlacklistedHashRepository
from repositories.blacklisted_query_param_repository import BlacklistedQueryParamRepository
from repositories.normalization_repository import NormalizationRepository
from repositories.page_repository import PageRepository

pytestmark = pytest.mark.infrastructure


def services(session: AsyncSession):
    storage = NormalizationRepository(session)
    params, hashes = BlacklistedQueryParamRepository(session), BlacklistedHashRepository(session)
    coordinator = NormalizationService(storage, params, hashes)
    return (
        BlacklistedQueryParamService(params, normalization_service=coordinator),
        BlacklistedHashService(hashes, normalization_service=coordinator),
        PageService(PageRepository(session), PageNormalizer(), params, hashes, normalization_service=coordinator),
        storage,
    )


@pytest.mark.parametrize("kind", [0, 1])
async def test_normalization_rule_snapshot_scan_are_one_request_transaction(pg_engine: AsyncEngine, kind: int):
    tenant = uuid4()
    rule_table = blacklisted_query_params if kind == 0 else blacklisted_hashes
    async with AsyncSession(pg_engine) as session:
        query, hashes, _, _ = services(session)
        service = query if kind == 0 else hashes
        await service.create(equestrian_id=tenant, data=BlacklistItemInDto(value="top"))
        # Simulate failure after rule/revision/scan creation, before HTTP commit.
        await session.rollback()
        for table in (rule_table, revisions, snapshots, scans):
            count = await session.scalar(
                select(func.count()).select_from(table).where(table.c.equestrian_id == tenant)
            )
            assert count == 0
        await session.rollback()
        async with session.begin():
            await service.create(equestrian_id=tenant, data=BlacklistItemInDto(value="top"))
        with pytest.raises(AlreadyExistsError):
            async with session.begin():
                await service.create(equestrian_id=tenant, data=BlacklistItemInDto(value="top"))
        for table in (rule_table, revisions, snapshots, scans):
            count = await session.scalar(
                select(func.count()).select_from(table).where(table.c.equestrian_id == tenant)
            )
            assert count == 1
        row = (await session.execute(select(revisions).where(revisions.c.equestrian_id == tenant))).mappings().one()
        revision = await services(session)[3].get_revision(equestrian_id=tenant, operation_id=row["operation_id"])
        assert revision is not None
        assert revision.snapshot == NormalizationSnapshot(("top",) if kind == 0 else (), ("top",) if kind else ())
        assert (
            await services(session)[3].get_checkpoint(equestrian_id=tenant, operation_id=row["operation_id"])
        ).high_water is None


async def test_normalization_resolve_before_post_commit_is_in_activation_watermark(pg_engine: AsyncEngine):
    tenant = uuid4()
    started = asyncio.Event()
    async with AsyncSession(pg_engine) as first:
        _, _, page, _ = services(first)
        old = await page.resolve(equestrian_id=tenant, data=SeoResolveInDto(items=["/a?utm=1"]))

        async def post():
            async with AsyncSession(pg_engine) as second, second.begin():
                query, _, _, storage = services(second)
                started.set()
                await query.create(equestrian_id=tenant, data=BlacklistItemInDto(value="utm"))
                active = await storage.activate_next(equestrian_id=tenant)
                assert active is not None
                checkpoint = await storage.get_checkpoint(equestrian_id=tenant, operation_id=active.operation_id)
                batch = await storage.scan_pages(equestrian_id=tenant, checkpoint=checkpoint, limit=500)
                assert old.items[0].id in {p.id for p in batch}
                return active

        task = asyncio.create_task(post())
        try:
            await asyncio.wait_for(started.wait(), 5)
            await asyncio.sleep(0.05)
            assert not task.done(), "POST must wait for resolve tenant transaction"
            await first.commit()
            active = await asyncio.wait_for(task, 5)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        async with first.begin():
            canonical = await page.resolve(equestrian_id=tenant, data=SeoResolveInDto(items=["/a?utm=1"]))
            assert canonical.items[0].normalized == "/a"
            assert canonical.items[0].id != old.items[0].id
            # A later POST captures current rules but cannot activate while R1 is active.
            _, hash_service, _, storage = services(first)
            await hash_service.create(equestrian_id=tenant, data=BlacklistItemInDto(value="top"))
            assert await storage.activate_next(equestrian_id=tenant) is None
            stored = await storage.get_revision(equestrian_id=tenant, operation_id=active.operation_id)
            assert stored is not None and stored.snapshot == NormalizationSnapshot(("utm",), ())


async def test_normalization_resolve_waits_for_post_and_keeps_tenant_isolation(pg_engine: AsyncEngine):
    tenant, foreign = uuid4(), uuid4()
    started = asyncio.Event()
    async with AsyncSession(pg_engine) as first:
        query, _, _, _ = services(first)
        await query.create(equestrian_id=tenant, data=BlacklistItemInDto(value="utm"))

        async def resolve(other: UUID):
            async with AsyncSession(pg_engine) as session, session.begin():
                started.set()
                return await services(session)[2].resolve(
                    equestrian_id=other, data=SeoResolveInDto(items=["/a?utm=1"])
                )

        task = asyncio.create_task(resolve(tenant))
        try:
            await asyncio.wait_for(started.wait(), 5)
            other = await asyncio.wait_for(resolve(foreign), 5)
            assert other.items[0].normalized == "/a?utm=1"
            assert not task.done()
            await first.commit()
            current = await asyncio.wait_for(task, 5)
            assert current.items[0].normalized == "/a"
            assert current.items[0].id != other.items[0].id
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
