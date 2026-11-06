"""Storage boundary. Implementations must not commit caller-owned transactions."""

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Protocol
from uuid import UUID

from core.entities.normalization import (
    MaintenanceMessage,
    NormalizationRevision,
    NormalizationSnapshot,
    OutboxClaim,
    PageMapping,
    ScanCheckpoint,
    ScanPage,
    StorageLease,
)


class NormalizationRepositoryProtocol(Protocol):
    """All operation mutations are tenant scoped and acquire lock_tenant.

    Inbox dedup uses its global consumer/message unique key; outbox claims use
    tenant-scoped SKIP LOCKED rows (no tenant lock held over network calls).
    Outer transaction/commit/rollback always belong to the caller.

    Rule POST + create_revision share one caller transaction. Resolve takes the
    same tenant row lock *before* insert allocates pages.scan_seq. Activate only
    the oldest pending revision, after its predecessor is finalized. Watermark
    is captured at activation under that lock, not while a revision is queued.
    """

    async def list_work_tenants(self, *, now: datetime, after: UUID | None = None, limit: int = 100) -> Sequence[UUID]:
        """Bounded UUID keyset: unfinished revisions OR due, unleased outbox.

        Discovery is only a hint, never a claim. Include finalized revisions'
        undelivered outbox; no tenant lock or network during enumeration.
        """
        ...

    async def lock_tenant(self, *, equestrian_id: UUID) -> None: ...

    async def create_revision(
        self,
        *,
        equestrian_id: UUID,
        operation_id: UUID,
        snapshot: NormalizationSnapshot,
    ) -> NormalizationRevision: ...

    async def get_active_revision(self, *, equestrian_id: UUID) -> NormalizationRevision | None:
        """Oldest unfinished revision, including pending/blocked/result-wait states.

        Caller holds the tenant lock for activation/scan decisions. A later
        revision is never returned until its predecessor is finalized.
        """
        ...

    async def activate_next(self, *, equestrian_id: UUID) -> NormalizationRevision | None: ...

    async def get_revision(
        self,
        *,
        equestrian_id: UUID,
        operation_id: UUID,
    ) -> NormalizationRevision | None: ...

    async def get_checkpoint(self, *, equestrian_id: UUID, operation_id: UUID) -> ScanCheckpoint: ...

    async def scan_pages(
        self,
        *,
        equestrian_id: UUID,
        checkpoint: ScanCheckpoint,
        limit: int,
    ) -> Sequence[ScanPage]: ...

    async def resolve_redirect(self, *, equestrian_id: UUID, page_id: UUID) -> UUID:
        """Follow/compress chain under tenant lock; reject cycles and foreign targets."""
        ...

    async def store_mappings(
        self,
        *,
        equestrian_id: UUID,
        operation_id: UUID,
        batch: Sequence[PageMapping],
    ) -> None:
        """At most 500 mappings + redirects, without commit/checkpoint.

        Caller fences the scan lease before this primitive in the same
        transaction. Direct canonical targets only; exact retries are no-ops.
        """
        ...

    async def get_mappings(
        self,
        *,
        equestrian_id: UUID,
        operation_id: UUID,
        after_seq: int = 0,
        limit: int = 500,
    ) -> Sequence[PageMapping]: ...

    async def claim_scan(
        self,
        *,
        equestrian_id: UUID,
        operation_id: UUID,
        now: datetime,
        duration: timedelta,
    ) -> StorageLease | None: ...

    async def commit_scan_batch(
        self,
        *,
        equestrian_id: UUID,
        operation_id: UUID,
        lease: StorageLease,
        expected: ScanCheckpoint,
        checkpoint: ScanCheckpoint,
        mappings: Sequence[PageMapping],
        messages: Sequence[MaintenanceMessage],
        now: datetime,
    ) -> None:
        """CAS lease/checkpoint; validate tenant membership, uniqueness and acyclicity.

        Mapping + redirects + outbox + checkpoint are indivisible (savepoint).
        Seal also sets revision.part_count and awaiting_result, preserving an
        existing awaiting_cleanup/correlation. It never finalizes or deletes.
        Batch releases the lease; claim again in the next transaction. A stale lease
        raises before any effect; all methods use the same caller transaction.
        """
        ...

    async def accept_inbox(self, *, consumer: str, message: MaintenanceMessage) -> bool:
        """Same identity/hash is a no-op; mismatched hash/tenant raises, never overwrites."""
        ...

    async def mark_inbox_processed(self, *, consumer: str, message: MaintenanceMessage, now: datetime) -> None: ...

    async def enqueue_outbox(self, *, message: MaintenanceMessage) -> None: ...

    async def claim_outbox(
        self, *, equestrian_id: UUID, now: datetime, duration: timedelta, limit: int = 100
    ) -> Sequence[OutboxClaim]: ...

    async def finish_outbox(
        self, *, claim: OutboxClaim, now: datetime, published: bool, retry_after: timedelta = timedelta(seconds=1)
    ) -> bool:
        """CAS claim; published only after PubAck. False means expired/stolen claim."""
        ...

    async def get_inbox_result(
        self, *, consumer: str, equestrian_id: UUID, operation_id: UUID
    ) -> MaintenanceMessage | None:
        """Durable result, including processed rows; never interpret duplicate as processed."""
        ...

    async def record_result(self, *, message: MaintenanceMessage, now: datetime) -> bool:
        """Validate and retain result atomically; True=new, False=exact redelivery.

        Early cleanup has no revision FK. Merge must match a known active revision.
        Superseded changes to awaiting_cleanup without stopping the unsealed scan.
        No physical deletion in the inbox handler. Caller commits before ACK.
        """
        ...

    async def commit_delete_batch(
        self,
        *,
        equestrian_id: UUID,
        operation_id: UUID,
        lease: StorageLease,
        expected: ScanCheckpoint,
        result: MaintenanceMessage,
        finalized: MaintenanceMessage,
        now: datetime,
        limit: int = 500,
    ) -> ScanCheckpoint:
        """Recheck BOTH barriers; source delete/checkpoint/finalized outbox in a fenced savepoint.

        At most limit durable mappings; retain redirects. Empty remainder finalizes.
        Release lease after each batch; outer commit/rollback is caller-owned.
        """
        ...
