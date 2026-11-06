"""Narrow boundaries for a bounded tick; no transport or storage implementation."""

from datetime import datetime
from typing import Protocol
from uuid import UUID

from core.entities.normalization import ScanCheckpoint


class NormalizationBatchProtocol(Protocol):
    async def run_batch(self, *, equestrian_id: UUID, now: datetime) -> tuple[UUID, ScanCheckpoint] | None: ...


class NormalizationRelayProtocol(Protocol):
    async def run_batch(self, *, equestrian_id: UUID) -> int: ...
