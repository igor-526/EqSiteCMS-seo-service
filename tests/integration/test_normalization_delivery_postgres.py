"""IT-N-01/UT-N-04 storage acceptance fixtures; require real migration-backed PG."""

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from core.entities.normalization import MaintenanceMessage, NormalizationSnapshot, PageMapping
from models.normalization import normalization_inbox as inbox
from models.normalization import normalization_outbox as outbox
from models.normalization import normalization_redirects as redirects
from models.normalization import normalization_scans as scans
from models.pages import pages
from repositories.normalization_repository import NormalizationRepository

pytestmark = pytest.mark.infrastructure
DURATION = timedelta(minutes=5)


def message(tenant: UUID, operation: UUID, part: int | None = None) -> MaintenanceMessage:
    encoded = json.dumps({"operation_id": str(operation), "version": 1}, sort_keys=True, separators=(",", ":"))
    return MaintenanceMessage(
        tenant,
        operation,
        uuid4(),
        "mappings" if part is not None else "seal",
        encoded,
        sha256(encoded.encode()).hexdigest(),
        part,
    )


async def setup_operation(session: AsyncSession, tenant: UUID, operation: UUID) -> list[tuple[UUID, int]]:
    repo = NormalizationRepository(session)
    await repo.lock_tenant(equestrian_id=tenant)
    values = []
    for index in range(10):
        path = f"/page/{index}"
        row = (
            await session.execute(
                insert(pages)
                .values(id=uuid4(), equestrian_id=tenant, page=path, page_hash=sha256(path.encode()).digest())
                .returning(pages.c.id, pages.c.scan_seq)
            )
        ).one()
        values.append((row[0], row[1]))
    await repo.create_revision(equestrian_id=tenant, operation_id=operation, snapshot=NormalizationSnapshot((), ()))
    await repo.activate_next(equestrian_id=tenant)
    return values


async def test_atomic_mapping_redirect_outbox_checkpoint_rollback_restart(pg_engine: AsyncEngine):
    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        values = await setup_operation(session, tenant, operation)
        await session.commit()
    for rollback in (True, False):
        async with AsyncSession(pg_engine) as session:
            repo = NormalizationRepository(session)
            now = datetime.now(UTC)
            lease = await repo.claim_scan(equestrian_id=tenant, operation_id=operation, now=now, duration=DURATION)
            assert lease is not None
            expected = await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation)
            checkpoint = replace(expected, cursor=values[2][1], next_part_index=1)
            batch = [PageMapping(identity, values[-1][0], seq, 0) for identity, seq in values[:3]]
            await repo.commit_scan_batch(
                equestrian_id=tenant,
                operation_id=operation,
                lease=lease,
                expected=expected,
                checkpoint=checkpoint,
                mappings=batch,
                messages=[message(tenant, operation, 0)],
                now=now,
            )
            if rollback:
                await session.rollback()
            else:
                await session.commit()
        async with AsyncSession(pg_engine) as fresh:
            repo = NormalizationRepository(fresh)
            found = await repo.get_mappings(equestrian_id=tenant, operation_id=operation)
            assert len(found) == (0 if rollback else 3)
            cp = await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation)
            assert cp.cursor == (0 if rollback else values[2][1])
            for table in (redirects, outbox):
                count = await fresh.scalar(
                    select(func.count()).select_from(table).where(table.c.equestrian_id == tenant)
                )
                assert count == (0 if rollback else (1 if table is outbox else 3))


async def test_final_lease_loss_rolls_back_entire_savepoint_even_if_caught(pg_session: AsyncSession, monkeypatch):
    session = pg_session
    tenant, operation = uuid4(), uuid4()
    values = await setup_operation(session, tenant, operation)
    repo = NormalizationRepository(session)
    now = datetime.now(UTC)
    lease = await repo.claim_scan(equestrian_id=tenant, operation_id=operation, now=now, duration=DURATION)
    assert lease is not None
    expected = await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation)
    enqueue = repo.enqueue_outbox

    async def lose_lease(*, message):
        await enqueue(message=message)
        await session.execute(
            update(scans).where(scans.c.operation_id == operation).values(lease_until=now - timedelta(seconds=1))
        )

    monkeypatch.setattr(repo, "enqueue_outbox", lose_lease)
    with pytest.raises(ValueError, match="expired during batch"):
        await repo.commit_scan_batch(
            equestrian_id=tenant,
            operation_id=operation,
            lease=lease,
            expected=expected,
            checkpoint=replace(expected, cursor=values[0][1], next_part_index=1),
            mappings=[PageMapping(values[0][0], values[-1][0], values[0][1], 0)],
            messages=[message(tenant, operation, 0)],
            now=now,
        )
    assert await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation) == expected
    assert await repo.get_mappings(equestrian_id=tenant, operation_id=operation) == []
    assert not await session.scalar(select(outbox.c.message_id).where(outbox.c.equestrian_id == tenant))
    assert not await session.scalar(select(redirects.c.source_id).where(redirects.c.equestrian_id == tenant))
    # Savepoint rolled back the artificial expiry as well; outer transaction is usable.
    assert await session.scalar(select(scans.c.lease_token).where(scans.c.operation_id == operation)) == lease.token


async def test_stale_lease_checkpoint_bounded_batches_and_seal(pg_engine: AsyncEngine):
    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as setup:
        values = await setup_operation(setup, tenant, operation)
        await setup.commit()
    stale = None
    for index in range(3):
        async with AsyncSession(pg_engine) as session:
            repo = NormalizationRepository(session)
            now = datetime.now(UTC)
            lease = await repo.claim_scan(equestrian_id=tenant, operation_id=operation, now=now, duration=DURATION)
            assert lease is not None
            expected = await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation)
            batch = [
                PageMapping(identity, values[-1][0], seq, index) for identity, seq in values[index * 3 : index * 3 + 3]
            ]
            cp = replace(expected, cursor=batch[-1].source_scan_seq, next_part_index=index + 1)
            if stale is not None:
                with pytest.raises(ValueError, match="stale"):
                    await repo.commit_scan_batch(
                        equestrian_id=tenant,
                        operation_id=operation,
                        lease=stale,
                        expected=expected,
                        checkpoint=cp,
                        mappings=batch,
                        messages=[message(tenant, operation, index)],
                        now=now,
                    )
            await repo.commit_scan_batch(
                equestrian_id=tenant,
                operation_id=operation,
                lease=lease,
                expected=expected,
                checkpoint=cp,
                mappings=batch,
                messages=[message(tenant, operation, index)],
                now=now,
            )
            stale = lease
            await session.commit()
    async with AsyncSession(pg_engine) as session:
        repo = NormalizationRepository(session)
        now = datetime.now(UTC)
        lease = await repo.claim_scan(equestrian_id=tenant, operation_id=operation, now=now, duration=DURATION)
        assert lease is not None
        expected = await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation)
        assert expected.high_water is not None
        sealed = replace(expected, cursor=expected.high_water, sealed=True)
        await repo.commit_scan_batch(
            equestrian_id=tenant,
            operation_id=operation,
            lease=lease,
            expected=expected,
            checkpoint=sealed,
            mappings=[],
            messages=[message(tenant, operation)],
            now=now,
        )
        assert await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation) == sealed
        assert len(await repo.get_mappings(equestrian_id=tenant, operation_id=operation)) == 9
        assert (
            await session.scalar(select(func.count()).select_from(outbox).where(outbox.c.equestrian_id == tenant)) == 4
        )


async def test_inbox_concurrent_duplicate_conflict_and_early_result(pg_engine: AsyncEngine):
    msg = message(uuid4(), uuid4())  # Early result has no known SEO revision yet.
    async with AsyncSession(pg_engine) as first, AsyncSession(pg_engine) as second:
        one, two = NormalizationRepository(first), NormalizationRepository(second)
        assert await one.accept_inbox(consumer="seo", message=msg)
        task = asyncio.create_task(two.accept_inbox(consumer="seo", message=msg))
        try:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), 0.1)
            await first.commit()
            assert not await asyncio.wait_for(task, 5)
            await second.commit()
            for bad in (replace(msg, equestrian_id=uuid4()), replace(msg, operation_id=uuid4())):
                with pytest.raises(ValueError, match="conflict"):
                    await two.accept_inbox(consumer="seo", message=bad)
            await two.mark_inbox_processed(consumer="seo", message=msg, now=datetime.now(UTC))
            await second.rollback()
            # ACK lost / processing crash leaves durable inbox unprocessed for replay.
            assert (
                await second.scalar(select(inbox.c.processed_at).where(inbox.c.message_id == msg.message_id)) is None
            )
            assert not await two.accept_inbox(consumer="seo", message=msg)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_outbox_restart_concurrent_skip_locked_expired_claim_and_puback(pg_engine: AsyncEngine):
    tenant, operation, foreign = uuid4(), uuid4(), uuid4()
    msg = message(tenant, operation, 0)
    async with AsyncSession(pg_engine) as setup:
        await setup_operation(setup, tenant, operation)
        await NormalizationRepository(setup).enqueue_outbox(message=msg)
        await setup.commit()
    async with AsyncSession(pg_engine) as first, AsyncSession(pg_engine) as second:
        one, two = NormalizationRepository(first), NormalizationRepository(second)
        now = datetime.now(UTC)
        claims = await one.claim_outbox(equestrian_id=tenant, now=now, duration=DURATION, limit=1)
        assert len(claims) == 1
        assert await two.claim_outbox(equestrian_id=tenant, now=now, duration=DURATION, limit=1) == []
        assert await two.claim_outbox(equestrian_id=foreign, now=now, duration=DURATION, limit=1) == []
        await first.commit()  # crash after claim, or PubAck before local mark
        await second.execute(
            update(outbox).where(outbox.c.message_id == msg.message_id).values(lease_until=now - timedelta(seconds=1))
        )
        await second.commit()
        fresh = await two.claim_outbox(equestrian_id=tenant, now=now, duration=DURATION, limit=1)
        assert len(fresh) == 1 and fresh[0].lease.attempts == 2
        assert fresh[0].message == msg
        await second.commit()
        assert not await one.finish_outbox(claim=claims[0], now=now, published=True)
        await first.rollback()
        assert await two.finish_outbox(claim=fresh[0], now=now, published=True)
        await second.commit()
        assert await one.claim_outbox(equestrian_id=tenant, now=now, duration=DURATION, limit=1) == []


async def test_concurrent_operation_claim_expiry_recovery_and_checkpoint_cas(pg_engine: AsyncEngine):
    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as setup:
        await setup_operation(setup, tenant, operation)
        await setup.commit()
    async with AsyncSession(pg_engine) as first, AsyncSession(pg_engine) as second:
        one, two = NormalizationRepository(first), NormalizationRepository(second)
        now = datetime.now(UTC)
        old = await one.claim_scan(equestrian_id=tenant, operation_id=operation, now=now, duration=DURATION)
        assert old is not None
        task = asyncio.create_task(
            two.claim_scan(equestrian_id=tenant, operation_id=operation, now=now, duration=DURATION)
        )
        try:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), 0.1)
            await first.commit()
            assert await asyncio.wait_for(task, 5) is None
            await second.rollback()
            await first.execute(
                update(scans).where(scans.c.operation_id == operation).values(lease_until=now - timedelta(seconds=1))
            )
            await first.commit()
            fresh = await two.claim_scan(equestrian_id=tenant, operation_id=operation, now=now, duration=DURATION)
            assert fresh is not None and fresh.token != old.token and fresh.attempts == old.attempts + 1
            await second.commit()
            expected = await one.get_checkpoint(equestrian_id=tenant, operation_id=operation)
            for lease, bad_checkpoint in ((old, expected), (fresh, replace(expected, next_part_index=1))):
                with pytest.raises(ValueError, match="stale"):
                    await one.commit_scan_batch(
                        equestrian_id=tenant,
                        operation_id=operation,
                        lease=lease,
                        expected=bad_checkpoint,
                        checkpoint=bad_checkpoint,
                        mappings=[],
                        messages=[],
                        now=now,
                    )
            await first.rollback()
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_outbox_conflict_after_mapping_rolls_back_to_checkpoint(pg_session: AsyncSession):
    session = pg_session
    tenant, operation = uuid4(), uuid4()
    values = await setup_operation(session, tenant, operation)
    repo = NormalizationRepository(session)
    original = message(tenant, operation, 0)
    await repo.enqueue_outbox(message=original)
    now = datetime.now(UTC)
    lease = await repo.claim_scan(equestrian_id=tenant, operation_id=operation, now=now, duration=DURATION)
    assert lease is not None
    expected = await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation)
    with pytest.raises(ValueError, match="outbox identity conflict"):
        await repo.commit_scan_batch(
            equestrian_id=tenant,
            operation_id=operation,
            lease=lease,
            expected=expected,
            checkpoint=replace(expected, cursor=values[0][1], next_part_index=1),
            mappings=[PageMapping(values[0][0], values[-1][0], values[0][1], 0)],
            messages=[replace(original, message_id=uuid4())],
            now=now,
        )
    assert await repo.get_mappings(equestrian_id=tenant, operation_id=operation) == []
    assert await repo.get_checkpoint(equestrian_id=tenant, operation_id=operation) == expected
    stored_id = await session.scalar(select(outbox.c.message_id).where(outbox.c.equestrian_id == tenant))
    assert stored_id == original.message_id
