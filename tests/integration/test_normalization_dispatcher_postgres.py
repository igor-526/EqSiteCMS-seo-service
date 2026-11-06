"""IT-N-01 discovery/restart fixtures, require real migration-backed PostgreSQL.

Relay below records intent only: these tests are NOT real NATS acceptance.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import insert, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.entities.normalization import NormalizationSnapshot, RevisionStatus
from core.services.normalization_dispatcher import NormalizationDispatcher
from core.services.page_normalizer import PageNormalizer
from depends.normalization_runtime import NormalizationBatchTransactions
from models.normalization import normalization_outbox as outbox
from models.normalization import normalization_revisions as revisions
from repositories.normalization_repository import NormalizationRepository
from repositories.normalization_transaction import NormalizationTransaction
from repositories.page_repository import PageRepository

pytestmark = pytest.mark.infrastructure


async def test_keyset_discovery_includes_finalized_pending_outbox_and_excludes_future_leased_published(pg_engine):
    now = datetime.now(UTC)
    identities = [UUID(int=i) for i in range(1, 8)]
    async with AsyncSession(pg_engine) as session, session.begin():
        repo = NormalizationRepository(session)
        for index, tenant in enumerate(identities):
            operation = uuid4()
            await repo.create_revision(
                equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot((), ())
            )
            # First tenant remains unfinished; others only discovered by outbox.
            if index:
                await session.execute(
                    update(revisions).where(revisions.c.operation_id == operation).values(status="finalized")
                )
            if index in (1, 2, 3, 4, 5):
                await session.execute(
                    insert(outbox).values(
                        equestrian_id=tenant,
                        operation_id=operation,
                        message_id=uuid4(),
                        kind="seal",
                        payload={},
                        payload_hash="unused",
                        next_attempt_at=now + timedelta(seconds=60) if index == 2 else now,
                        published_at=now if index == 3 else None,
                        lease_token=uuid4() if index in (4, 5) else None,
                        lease_until=now + timedelta(seconds=60) if index == 4 else now if index == 5 else None,
                    )
                )
        assert list(await repo.list_work_tenants(now=now, limit=2)) == identities[:2]
        assert list(await repo.list_work_tenants(now=now, after=identities[1], limit=2)) == [identities[5]]
        assert not await repo.list_work_tenants(now=now, after=identities[5], limit=2)


async def test_dispatcher_restart_uses_committed_checkpoint_and_keeps_successor_pending(pg_engine):
    tenant, operation, successor = uuid4(), uuid4(), uuid4()
    factory = async_sessionmaker(pg_engine, expire_on_commit=False, autoflush=False)
    async with factory() as session, session.begin():
        repo = NormalizationRepository(session)
        await repo.lock_tenant(equestrian_id=tenant)
        await PageRepository(session).resolve(equestrian_id=tenant, values=["/a?u=1", "/a?u=2"])
        for op in (operation, successor):
            await repo.create_revision(
                equestrian_id=tenant, operation_id=op, snapshot=NormalizationSnapshot(("u",), ())
            )

    def build():
        batches = NormalizationBatchTransactions(factory, normalizer=PageNormalizer(), scan_batch_size=1)
        return NormalizationDispatcher(
            transaction=NormalizationTransaction(factory),
            scan=batches.scan,
            finalization=batches.finalization,
            relay=AsyncMock(),
            tenant_page_size=10,
        )

    assert await build().tick() == 0
    async with factory() as session, session.begin():
        checkpoint = await NormalizationRepository(session).get_checkpoint(
            equestrian_id=tenant, operation_id=operation
        )
        assert checkpoint.cursor > 0 and checkpoint.next_part_index == 1
    # Recreate dependencies each time, as after process restart. Exactly two
    # mapping parts and a seal survive; no later revision before finalization.
    for _ in range(3):
        assert await build().tick() == 0
    async with factory() as session, session.begin():
        repo = NormalizationRepository(session)
        current = await repo.get_revision(equestrian_id=tenant, operation_id=operation)
        queued = await repo.get_revision(equestrian_id=tenant, operation_id=successor)
        checkpoint = await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation)
        assert checkpoint.sealed and checkpoint.next_part_index == 2
        assert current is not None and current.status == RevisionStatus.AWAITING_RESULT
        assert queued is not None and queued.status == RevisionStatus.PENDING
