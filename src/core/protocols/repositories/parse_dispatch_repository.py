from datetime import date, datetime, timedelta
from typing import Protocol
from uuid import UUID

from core.entities.parse_dispatch import ParseDispatch


class ParseDispatchRepositoryProtocol(Protocol):
    """Each operation commits independently, before any external publication.

    Pass the returned attempts to mark methods as a fencing token: a worker
    whose lease was reclaimed must not overwrite the newer worker's result.
    Claim returns None when ineligible; mark returns False for a stale token or
    an already finalized row. Failed claims obey max_attempts; stale pending
    claims may be reclaimed regardless of the cap (D5). now must be UTC-aware.
    record_manual is an audit insert after successful PubAck, never before.
    """

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
    ) -> ParseDispatch | None: ...

    async def mark_published(self, *, job_id: UUID, attempts: int, now: datetime | None = None) -> bool: ...

    async def mark_failed(
        self,
        *,
        job_id: UUID,
        attempts: int,
        error_code: str,
        now: datetime | None = None,
    ) -> bool: ...

    async def record_manual(
        self,
        *,
        job_id: UUID,
        provider: str,
        equestrian_id: UUID,
        now: datetime | None = None,
    ) -> ParseDispatch: ...
