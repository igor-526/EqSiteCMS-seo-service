from datetime import UTC, date, datetime, timedelta
from uuid import UUID

from sqlalchemy import and_, or_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.entities.parse_dispatch import ParseDispatch, ParseDispatchStatus, ParseDispatchTrigger
from models.parse_dispatches import parse_dispatches as table
from repositories.abstract_repository import repository_errors


class ParseDispatchRepository:
    """Ledger owns short, isolated transactions, never the request transaction."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    async def claim_schedule(
        self,
        *,
        job_id: UUID,
        provider: str,
        equestrian_id: UUID,
        schedule_date: date,
        max_attempts: int,
        lease: timedelta,
        now: datetime | None = None,
    ) -> ParseDispatch | None:
        if max_attempts < 1 or lease <= timedelta(0):
            raise ValueError("max_attempts and lease must be positive")
        now = now or datetime.now(UTC)
        statement = (
            insert(table)
            .values(
                id=job_id,
                provider=provider,
                equestrian_id=equestrian_id,
                trigger=ParseDispatchTrigger.SCHEDULE,
                schedule_date=schedule_date,
                status=ParseDispatchStatus.PENDING,
                attempts=1,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_update(
                index_elements=[table.c.provider, table.c.equestrian_id, table.c.schedule_date],
                index_where=table.c.trigger == ParseDispatchTrigger.SCHEDULE,
                set_={
                    "status": ParseDispatchStatus.PENDING,
                    "attempts": table.c.attempts + 1,
                    "updated_at": now,
                    "last_error_code": None,
                },
                where=or_(
                    and_(table.c.status == ParseDispatchStatus.FAILED, table.c.attempts < max_attempts),
                    and_(table.c.status == ParseDispatchStatus.PENDING, table.c.updated_at < now - lease),
                ),
            )
            .returning(table)
        )
        with repository_errors():
            # Concurrent inserts of the deterministic job UUID can race on the
            # non-arbiter primary key before PostgreSQL sees the partial index.
            # Retry only that conflict, in a new transaction; the winner's row
            # is then visible and the same atomic UPSERT decides eligibility.
            for retry in range(2):
                try:
                    async with self.session_factory.begin() as session:
                        result = await session.execute(statement)
                        row = result.mappings().first()
                        return ParseDispatch.model_validate(dict(row)) if row is not None else None
                except IntegrityError as exc:
                    cause = getattr(exc.orig, "__cause__", None)
                    if any(
                        (
                            retry != 0,
                            getattr(exc.orig, "sqlstate", None) != "23505",
                            getattr(cause, "constraint_name", None) != "parse_dispatches_pkey",
                        )
                    ):
                        raise
        return None

    async def mark_published(self, *, job_id: UUID, attempts: int, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        return await self._mark(
            job_id=job_id,
            attempts=attempts,
            status=ParseDispatchStatus.PUBLISHED,
            now=now,
            error_code=None,
        )

    async def mark_failed(
        self,
        *,
        job_id: UUID,
        attempts: int,
        error_code: str,
        now: datetime | None = None,
    ) -> bool:
        # Only stable machine codes belong in the ledger, never driver/broker messages.
        if any(
            (
                not error_code,
                len(error_code) > 32,
                not all(c.isascii() and (c.isalnum() or c == "_") for c in error_code),
            )
        ):
            raise ValueError("error_code must be a machine code of at most 32 characters")
        return await self._mark(
            job_id=job_id,
            attempts=attempts,
            status=ParseDispatchStatus.FAILED,
            now=now or datetime.now(UTC),
            error_code=error_code,
        )

    async def _mark(
        self,
        *,
        job_id: UUID,
        attempts: int,
        status: ParseDispatchStatus,
        now: datetime,
        error_code: str | None,
    ) -> bool:
        with repository_errors():
            async with self.session_factory.begin() as session:
                result = await session.execute(
                    update(table)
                    .where(
                        table.c.id == job_id,
                        table.c.status == ParseDispatchStatus.PENDING,
                        table.c.attempts == attempts,
                    )
                    .values(
                        status=status,
                        updated_at=now,
                        last_error_code=error_code,
                        published_at=now if status == ParseDispatchStatus.PUBLISHED else None,
                    )
                    .returning(table.c.id)
                )
                return result.scalar_one_or_none() is not None

    async def record_manual(
        self,
        *,
        job_id: UUID,
        provider: str,
        equestrian_id: UUID,
        now: datetime | None = None,
    ) -> ParseDispatch:
        """Call only after PubAck. No scheduled slot is occupied."""
        now = now or datetime.now(UTC)
        with repository_errors():
            async with self.session_factory.begin() as session:
                result = await session.execute(
                    insert(table)
                    .values(
                        id=job_id,
                        provider=provider,
                        equestrian_id=equestrian_id,
                        trigger=ParseDispatchTrigger.MANUAL,
                        schedule_date=None,
                        status=ParseDispatchStatus.PUBLISHED,
                        attempts=1,
                        created_at=now,
                        updated_at=now,
                        published_at=now,
                    )
                    .returning(table)
                )
                return ParseDispatch.model_validate(dict(result.mappings().one()))
