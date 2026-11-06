"""Maintenance publication boundary; never regenerates persisted envelopes."""

from typing import Protocol

from core.entities.normalization import MaintenanceMessage


class MaintenancePublisherProtocol(Protocol):
    async def publish(self, *, message: MaintenanceMessage) -> bool:
        """Return PubAck.duplicate; raise on failure (False is a new delivery)."""
        ...
