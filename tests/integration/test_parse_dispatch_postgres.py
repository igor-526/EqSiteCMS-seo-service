"""IT-PS-02: independently committed ledger claims on real PostgreSQL."""

import asyncio
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from core.entities.parse_dispatch import ParseDispatchStatus, ParseDispatchTrigger
from core.protocols.repositories import ParseDispatchRepositoryProtocol
from models.parse_dispatches import parse_dispatches
from repositories import ParseDispatchRepository

pytestmark = pytest.mark.infrastructure
NOW = datetime(2026, 10, 28, 3, tzinfo=UTC)
LEASE = timedelta(minutes=5)


def repository(engine: AsyncEngine) -> ParseDispatchRepositoryProtocol:
    return ParseDispatchRepository(async_sessionmaker(engine, expire_on_commit=False))


def slot() -> dict:
    return dict(
        job_id=uuid4(),
        provider="demo",
        equestrian_id=uuid4(),
        schedule_date=date(2026, 10, 28),
        max_attempts=2,
        lease=LEASE,
    )


async def test_it_ps_02_concurrent_claim_commits_before_publish(pg_engine: AsyncEngine) -> None:
    args = slot()
    results = await asyncio.gather(*(repository(pg_engine).claim_schedule(**args, now=NOW) for _ in range(8)))
    claimed = [r for r in results if r is not None]
    assert len(claimed) == 1
    assert claimed[0].id == args["job_id"] and claimed[0].attempts == 1
    assert claimed[0].status == ParseDispatchStatus.PENDING
    # A separate connection sees the committed claim, no network operation holds a lock.
    async with pg_engine.connect() as connection:
        assert await connection.scalar(select(func.count()).select_from(parse_dispatches)) == 1
    repo = repository(pg_engine)
    assert await repo.mark_published(job_id=claimed[0].id, attempts=1, now=NOW)
    assert await repo.claim_schedule(**args, now=NOW + timedelta(days=1)) is None
    assert not await repo.mark_failed(job_id=claimed[0].id, attempts=1, error_code="nats_unavailable")


async def test_it_ps_02_failed_retry_cap_and_lease_fencing(pg_engine: AsyncEngine) -> None:
    repo = repository(pg_engine)
    args = slot()
    first = await repo.claim_schedule(**args, now=NOW)
    assert first is not None
    assert await repo.claim_schedule(**args, now=NOW + LEASE) is None  # strict expiry
    # A stale pending claim is reclaimed even if the failed-attempt cap is reached (D5).
    reclaimed = await repo.claim_schedule(**(args | {"max_attempts": 1}), now=NOW + LEASE + timedelta(seconds=1))
    assert reclaimed is not None and reclaimed.attempts == 2 and reclaimed.id == first.id
    assert not await repo.mark_published(job_id=first.id, attempts=1, now=NOW)
    assert not await repo.mark_failed(job_id=first.id, attempts=1, error_code="stale_worker")
    assert await repo.mark_failed(job_id=first.id, attempts=2, error_code="nats_unavailable", now=NOW + LEASE)
    assert await repo.claim_schedule(**args, now=NOW + LEASE * 2) is None
    retried = await repo.claim_schedule(**(args | {"max_attempts": 3}), now=NOW + LEASE * 2)
    assert retried is not None and retried.attempts == 3 and retried.last_error_code is None
    assert retried.created_at == NOW
    assert await repo.mark_published(job_id=retried.id, attempts=3, now=NOW + LEASE * 2)
    async with pg_engine.connect() as conn:
        row = (await conn.execute(select(parse_dispatches))).mappings().one()
        assert row["status"] == "published" and row["published_at"] == NOW + LEASE * 2
        assert row["last_error_code"] is None


async def test_it_ps_02_manual_and_other_slots_independent(pg_engine: AsyncEngine) -> None:
    repo = repository(pg_engine)
    args = slot()
    for _ in range(2):
        manual = await repo.record_manual(
            job_id=uuid4(),
            provider=args["provider"],
            equestrian_id=args["equestrian_id"],
            now=NOW,
        )
        assert manual.trigger == ParseDispatchTrigger.MANUAL and manual.schedule_date is None
        assert manual.status == ParseDispatchStatus.PUBLISHED and manual.published_at == NOW
    assert await repo.claim_schedule(**args, now=NOW) is not None
    for overrides in (
        {"provider": "other"},
        {"equestrian_id": uuid4()},
        {"schedule_date": date(2026, 10, 29)},
    ):
        assert await repo.claim_schedule(**(args | overrides | {"job_id": uuid4()}), now=NOW) is not None
    async with pg_engine.connect() as conn:
        assert await conn.scalar(select(func.count()).select_from(parse_dispatches)) == 6


async def test_invalid_error_code_never_persists_messages(pg_engine: AsyncEngine) -> None:
    with pytest.raises(ValueError, match="machine code"):
        await repository(pg_engine).mark_failed(job_id=uuid4(), attempts=1, error_code="broker secret://token")


async def test_di_ledger_transaction_survives_request_rollback(pg_engine: AsyncEngine) -> None:
    from containers.repositories import RepositoriesContainer

    container = RepositoriesContainer()
    repo: ParseDispatchRepositoryProtocol = container.parse_dispatch_repository(
        session_factory=async_sessionmaker(pg_engine, expire_on_commit=False)
    )
    assert not container.session.initialized
    args = slot()
    async with async_sessionmaker(pg_engine)() as request_session:
        await request_session.execute(select(func.count()).select_from(parse_dispatches))
        assert await repo.claim_schedule(**args, now=NOW) is not None
        await request_session.rollback()
    assert await repo.claim_schedule(**args, now=NOW) is None
    assert not container.session.initialized
