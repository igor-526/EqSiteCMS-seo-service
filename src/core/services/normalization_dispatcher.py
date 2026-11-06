"""One bounded tenant page per tick, independent of the parse scheduler.

This is not a process entrypoint or a readiness signal. A supervisor must await
its ticks and arrange backoff on discovery failures. No network runs in a batch
transaction. Restart discards only the fair-discovery cursor, never checkpoints.
"""

import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from uuid import UUID

from core.protocols.normalization_runtime import NormalizationBatchProtocol, NormalizationRelayProtocol
from core.protocols.repositories.normalization_repository import NormalizationRepositoryProtocol

logger = logging.getLogger(__name__)


class NormalizationDispatcher:
    def __init__(
        self,
        *,
        transaction: Callable[[], AbstractAsyncContextManager[NormalizationRepositoryProtocol]],
        scan: Callable[[], AbstractAsyncContextManager[NormalizationBatchProtocol]],
        finalization: Callable[[], AbstractAsyncContextManager[NormalizationBatchProtocol]],
        relay: NormalizationRelayProtocol,
        tenant_page_size: int = 100,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 1 <= tenant_page_size <= 500:
            raise ValueError("invalid tenant page size")
        self._transaction = transaction
        self._scan = scan
        self._finalization = finalization
        self._relay = relay
        self._page_size = tenant_page_size
        self._clock = clock
        self._after: UUID | None = None

    async def tick(self) -> int:
        """Visit at most page_size tenants, one scan/delete/relay batch each.

        Discovery failures (including commit) propagate to the supervisor. Stage
        failures roll back independently and are counted for caller health/backoff;
        never log exception text/payload. Cancellation always propagates.
        """
        async with self._transaction() as repository:
            tenants = await repository.list_work_tenants(now=self._clock(), after=self._after, limit=self._page_size)
        failures = 0
        for tenant in tenants:
            # Each context commits before the next stage starts. A failed scan
            # cannot accidentally commit its changes through the finalizer.
            for stage, factory in (("scan", self._scan), ("finalization", self._finalization)):
                try:
                    async with factory() as batch:
                        await batch.run_batch(equestrian_id=tenant, now=self._clock())
                except Exception:
                    failures += 1
                    logger.error("Normalization batch failed: stage=%s tenant=%s", stage, tenant)
            try:
                await self._relay.run_batch(equestrian_id=tenant)
            except Exception:
                failures += 1
                logger.error("Normalization batch failed: stage=relay tenant=%s", tenant)
            self._after = tenant
        # Wrap on short/empty pages; a full page wraps at the following empty
        # tick. Newly inserted lower UUIDs are seen on the next pass, not lost.
        if len(tenants) < self._page_size:
            self._after = None
        return failures
