"""Infrastructure-free durable normalization contracts (not HTTP DTOs)."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID


class RevisionStatus(StrEnum):
    PENDING = "pending"
    SCANNING = "scanning"
    AWAITING_RESULT = "awaiting_result"
    AWAITING_CLEANUP = "awaiting_cleanup"
    DELETING = "deleting"
    BLOCKED = "blocked"
    FINALIZED = "finalized"


@dataclass(frozen=True, slots=True)
class NormalizationSnapshot:
    query_params: tuple[str, ...]
    hashes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class NormalizationRevision:
    equestrian_id: UUID
    operation_id: UUID
    revision: int
    status: RevisionStatus
    snapshot: NormalizationSnapshot
    part_count: int | None = None
    cleanup_operation_id: UUID | None = None
    cleanup_seq: int | None = None

    def __post_init__(self) -> None:
        if self.revision < 1 or (self.part_count is not None and self.part_count < 0):
            raise ValueError("invalid revision bounds")
        if (self.cleanup_operation_id is None) != (self.cleanup_seq is None):
            raise ValueError("cleanup identity must be complete")
        if self.cleanup_seq is not None and self.cleanup_seq < 1:
            raise ValueError("cleanup sequence must be positive")


@dataclass(frozen=True, slots=True)
class ScanCheckpoint:
    cursor: int = 0
    high_water: int | None = None
    delete_cursor: int = 0
    next_part_index: int = 0
    sealed: bool = False

    def __post_init__(self) -> None:
        if min(self.cursor, self.delete_cursor, self.next_part_index) < 0:
            raise ValueError("negative checkpoint")
        if self.high_water is not None and self.high_water < self.cursor:
            raise ValueError("cursor beyond high-water")
        if self.sealed and (self.high_water is None or self.cursor != self.high_water):
            raise ValueError("cannot seal incomplete scan")


@dataclass(frozen=True, slots=True)
class ScanPage:
    id: UUID
    page: str
    scan_seq: int


@dataclass(frozen=True, slots=True)
class PageMapping:
    source_id: UUID
    target_id: UUID
    source_scan_seq: int
    part_index: int

    def __post_init__(self) -> None:
        if self.source_id == self.target_id:
            raise ValueError("source equals target")
        if self.source_scan_seq < 1 or self.part_index < 0:
            raise ValueError("invalid mapping bounds")


@dataclass(frozen=True, slots=True)
class UUIDRedirect:
    source_id: UUID
    target_id: UUID
    operation_id: UUID

    def __post_init__(self) -> None:
        if self.source_id == self.target_id:
            raise ValueError("source equals target")


@dataclass(frozen=True, slots=True)
class StorageLease:
    token: UUID
    until: datetime
    attempts: int

    def __post_init__(self) -> None:
        if self.until.utcoffset() is None or self.attempts < 1:
            raise ValueError("lease requires timezone and positive attempts")


@dataclass(frozen=True, slots=True)
class MaintenanceMessage:
    """Wire JSON with canonical-content SHA-256; transport validates UUID-only schema."""

    equestrian_id: UUID
    operation_id: UUID
    message_id: UUID
    kind: str
    payload_json: str
    payload_hash: str
    part_index: int | None = None

    def __post_init__(self) -> None:
        if not self.kind or len(self.kind) > 32:
            raise ValueError("invalid message identity")
        if len(self.payload_hash) != 64 or any(c not in "0123456789abcdef" for c in self.payload_hash):
            raise ValueError("payload hash must be SHA-256 hex")
        if self.part_index is not None and self.part_index < 0:
            raise ValueError("negative part index")


@dataclass(frozen=True, slots=True)
class OutboxClaim:
    message: MaintenanceMessage
    lease: StorageLease
