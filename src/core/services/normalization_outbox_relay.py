"""Bounded outbox delivery; caller supplies short commit/rollback contexts.

The transaction factory must commit on successful exit, rollback on failure.
No database transaction or tenant lock is held over a network publish.
"""

import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from uuid import UUID

from core.protocols.clients.maintenance import MaintenancePublisherProtocol
from core.protocols.repositories.normalization_repository import NormalizationRepositoryProtocol

logger = logging.getLogger(__name__)


class NormalizationOutboxRelay:
    def __init__(
        self,
        *,
        transaction: Callable[[], AbstractAsyncContextManager[NormalizationRepositoryProtocol]],
        publisher: MaintenancePublisherProtocol,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        batch_size: int = 100,
        lease_duration: timedelta = timedelta(seconds=30),
    ) -> None:
        if not 1 <= batch_size <= 500 or lease_duration <= timedelta(0):
            raise ValueError("invalid relay bounds")
        self._transaction = transaction
        self._publisher = publisher
        self._clock = clock
        self._batch_size = batch_size
        self._lease_duration = lease_duration

    async def run_batch(self, *, equestrian_id: UUID) -> int:
        async with self._transaction() as repository:
            claims = await repository.claim_outbox(
                equestrian_id=equestrian_id,
                now=self._clock(),
                duration=self._lease_duration,
                limit=self._batch_size,
            )
        delivered = 0
        for claim in claims:
            # Claims may have expired while earlier messages were in flight.
            if self._clock() >= claim.lease.until:
                continue
            published = False
            try:
                await self._publisher.publish(message=claim.message)
                published = True
            except Exception:
                log = logger.error if claim.lease.attempts >= 5 else logger.warning
                log("Maintenance publication retry: message_id=%s", claim.message.message_id)
            # Cancellation propagates: the committed claim expires and is replayed.
            # A mark/commit failure propagates too: never mask uncertain delivery.
            retry_after = timedelta(seconds=2 ** min(claim.lease.attempts - 1, 6))
            retry_after = min(retry_after, timedelta(seconds=60))
            async with self._transaction() as repository:
                marked = await repository.finish_outbox(
                    claim=claim, now=self._clock(), published=published, retry_after=retry_after
                )
            delivered += int(published and marked)
        return delivered
