"""IT-N-01: migration-backed finalization/restart/fencing fixtures.

Collection or skip is NOT PostgreSQL acceptance; runtime remains OFF.
"""

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.normalization import MaintenanceMessage, NormalizationSnapshot, RevisionStatus
from core.services.normalization_finalization_service import NormalizationFinalizationService
from core.services.normalization_scan_service import NormalizationScanService
from core.services.page_normalizer import PageNormalizer
from models.normalization import normalization_inbox as inbox
from models.normalization import normalization_mappings as mappings
from models.normalization import normalization_outbox as outbox
from models.normalization import normalization_redirects as redirects
from models.normalization import normalization_scans as scans
from models.pages import pages
from repositories.normalization_repository import NormalizationRepository
from repositories.page_repository import PageRepository

pytestmark = pytest.mark.infrastructure


def report(revision, *, cleanup=False, operation=None, superseded=False, cleanup_operation=None):
    identity = uuid4()
    operation = operation or revision.operation_id
    payload = dict(
        schema_version=1,
        message_id=str(identity),
        operation_id=str(operation),
        equestrian_id=str(revision.equestrian_id),
        occurred_at=datetime.now(UTC).isoformat(),
        status="completed",
    )
    if cleanup:
        payload["cleanup_seq"] = 1
    else:
        payload.update(normalization_revision=revision.revision, part_count=revision.part_count)
        if superseded:
            payload.update(status="superseded_by_delete", cleanup_operation_id=str(cleanup_operation), cleanup_seq=1)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return MaintenanceMessage(
        revision.equestrian_id,
        operation,
        identity,
        "cleanup_completed" if cleanup else "merge_completed",
        encoded,
        sha256(encoded.encode()).hexdigest(),
    )


def services(session):
    repo = NormalizationRepository(session)
    return (
        repo,
        NormalizationScanService(repo, PageRepository(session), PageNormalizer(), batch_size=2),
        NormalizationFinalizationService(repo, batch_size=2),
    )


async def seed(session, *, empty=False):
    tenant, operation = uuid4(), uuid4()
    repo, scan, final = services(session)
    async with session.begin():
        await repo.lock_tenant(equestrian_id=tenant)
        if not empty:
            await PageRepository(session).resolve(
                equestrian_id=tenant, values=[f"/a?u={n}" for n in range(5)] + ["/a"]
            )
        await repo.create_revision(
            equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot(("u",), ())
        )
        await repo.activate_next(equestrian_id=tenant)
    return tenant, operation, repo, scan, final


async def seal(session, scan, tenant):
    for _ in range(10):
        async with session.begin():
            batch = await scan.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
            assert batch is not None
            if batch[1].sealed:
                return
    pytest.fail("bounded fixture scan did not seal")


@pytest.mark.parametrize("empty", [False, True])
async def test_finalization_restart_source_only_redirects_successor_and_redelivery(pg_engine, empty):
    async with AsyncSession(pg_engine) as session:
        tenant, operation, repo, scan, final = await seed(session, empty=empty)
        foreign = uuid4()
        async with session.begin():
            await repo.lock_tenant(equestrian_id=foreign)
            foreign_pages = await PageRepository(session).resolve(equestrian_id=foreign, values=["/a?u=0"])
        await seal(session, scan, tenant)
        async with session.begin():
            revision = await repo.get_revision(equestrian_id=tenant, operation_id=operation)
            msg = report(revision)
            assert await final.accept_result(message=msg, now=datetime.now(UTC))
            await repo.create_revision(
                equestrian_id=tenant, operation_id=uuid4(), snapshot=NormalizationSnapshot(("u", "v"), ())
            )
            assert await repo.activate_next(equestrian_id=tenant) is None
        # Crash before batch commit leaves physical sources and checkpoint intact.
        async with session.begin():
            await final.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
            await session.rollback()
        async with session.begin():
            cp = await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation)
            assert cp.delete_cursor == 0
    # Fresh session/service after every commit must advance durable delete keyset.
    for _ in range(4):
        async with AsyncSession(pg_engine) as session, session.begin():
            repo, _, final = services(session)
            active = await repo.get_revision(equestrian_id=tenant, operation_id=operation)
            assert active is not None
            if active.status == RevisionStatus.FINALIZED:
                break
            assert await final.run_batch(equestrian_id=tenant, now=datetime.now(UTC)) is not None
    async with AsyncSession(pg_engine) as session, session.begin():
        repo, _, final = services(session)
        revision = await repo.get_revision(equestrian_id=tenant, operation_id=operation)
        assert revision is not None
        assert revision.status == RevisionStatus.FINALIZED
        assert not await final.accept_result(message=msg, now=datetime.now(UTC))
        for table, count in (
            (pages, 0 if empty else 1),
            (mappings, 0 if empty else 5),
            (redirects, 0 if empty else 5),
        ):
            assert (
                await session.scalar(select(func.count()).select_from(table).where(table.c.equestrian_id == tenant))
                == count  # noqa: W503
            )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(outbox)
                .where(outbox.c.equestrian_id == tenant, outbox.c.kind == "finalized")
            )
            == 1  # noqa: W503
        )
        assert await session.scalar(select(inbox.c.processed_at).where(inbox.c.message_id == msg.message_id))
        successor = await repo.activate_next(equestrian_id=tenant)
        assert successor is not None
        assert successor.revision == 2
        assert (
            await session.scalar(select(pages.c.id).where(pages.c.equestrian_id == foreign)) == foreign_pages["/a?u=0"]
        )
        if not empty:
            target = await session.scalar(select(pages.c.id).where(pages.c.equestrian_id == tenant))
            ids = (
                (await session.execute(select(redirects.c.source_id).where(redirects.c.equestrian_id == tenant)))
                .scalars()
                .all()
            )
            for identity in ids:
                assert await repo.resolve_redirect(equestrian_id=tenant, page_id=identity) == target


@pytest.mark.parametrize("cleanup_first", [False, True])
async def test_superseded_early_cleanup_and_report_wait_for_both_barriers(pg_engine, cleanup_first):
    async with AsyncSession(pg_engine) as session:
        tenant, operation, repo, scan, final = await seed(session)
        cleanup_operation = uuid4()
        async with session.begin():
            revision = await repo.get_revision(equestrian_id=tenant, operation_id=operation)
            cancelled = report(revision, superseded=True, cleanup_operation=cleanup_operation)
            cleanup = report(revision, cleanup=True, operation=cleanup_operation)
            if cleanup_first:
                await final.accept_result(message=cleanup, now=datetime.now(UTC))
            await final.accept_result(message=cancelled, now=datetime.now(UTC))
            assert await final.run_batch(equestrian_id=tenant, now=datetime.now(UTC)) is None
            assert (
                await session.scalar(select(func.count()).select_from(pages).where(pages.c.equestrian_id == tenant))
                == 6  # noqa: W503
            )
        await seal(session, scan, tenant)
        async with session.begin():
            if not cleanup_first:
                assert await final.run_batch(equestrian_id=tenant, now=datetime.now(UTC)) is None
                await final.accept_result(message=cleanup, now=datetime.now(UTC))
            assert await final.run_batch(equestrian_id=tenant, now=datetime.now(UTC)) is not None


@pytest.mark.parametrize("empty", [False, True])
async def test_final_lease_expiry_rolls_back_delete_checkpoint_marker_and_outbox(pg_engine, empty):
    async with AsyncSession(pg_engine) as session:
        tenant, operation, repo, scan, final = await seed(session, empty=empty)
        final = NormalizationFinalizationService(repo, batch_size=10)
        await seal(session, scan, tenant)
        async with session.begin():
            revision = await repo.get_revision(equestrian_id=tenant, operation_id=operation)
            msg = report(revision)
            await final.accept_result(message=msg, now=datetime.now(UTC))
        enqueue = repo.enqueue_outbox

        async def expire(*, message):
            await enqueue(message=message)
            await session.execute(
                update(scans)
                .where(scans.c.operation_id == operation)
                .values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
            )

        async with session.begin():
            with patch.object(repo, "enqueue_outbox", side_effect=expire), pytest.raises(ValueError, match="expired"):
                await final.run_batch(equestrian_id=tenant, now=datetime.now(UTC))
            revision = await repo.get_revision(equestrian_id=tenant, operation_id=operation)
            assert revision is not None
            assert revision.status == RevisionStatus.AWAITING_RESULT
            assert await session.scalar(
                select(func.count()).select_from(pages).where(pages.c.equestrian_id == tenant)
            ) == (0 if empty else 6)
            assert not await session.scalar(
                select(outbox.c.message_id).where(outbox.c.equestrian_id == tenant, outbox.c.kind == "finalized")
            )
            assert not await session.scalar(select(inbox.c.processed_at).where(inbox.c.message_id == msg.message_id))
        # Claim exists until expiry after a caught savepoint error. Caller should rollback on errors.
