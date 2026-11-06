"""Request-side normalization coordination; no scan or network I/O.

All repositories must share the caller's transaction. Locks are released only
by its commit/rollback, never by this service.
"""

from uuid import UUID, uuid4

from core.entities.normalization import NormalizationSnapshot
from core.protocols.repositories import (
    BlacklistedHashRepositoryProtocol,
    BlacklistedQueryParamRepositoryProtocol,
)
from core.protocols.repositories.normalization_repository import NormalizationRepositoryProtocol


class NormalizationService:
    def __init__(
        self,
        normalization_repository: NormalizationRepositoryProtocol,
        blacklisted_query_param_repository: BlacklistedQueryParamRepositoryProtocol,
        blacklisted_hash_repository: BlacklistedHashRepositoryProtocol,
    ) -> None:
        self._repository = normalization_repository
        self._params = blacklisted_query_param_repository
        self._hashes = blacklisted_hash_repository

    async def lock(self, *, equestrian_id: UUID) -> None:
        await self._repository.lock_tenant(equestrian_id=equestrian_id)

    async def plan(self, *, equestrian_id: UUID) -> None:
        """Called after a successful rule insert, under the same tenant lock.

        A pending revision + immutable snapshot + scan row is the durable scan
        intention. Activation/high-water belongs to the bounded worker, not POST.
        """
        snapshot = NormalizationSnapshot(
            query_params=tuple(sorted(await self._params.get_values(equestrian_id=equestrian_id))),
            hashes=tuple(sorted(await self._hashes.get_values(equestrian_id=equestrian_id))),
        )
        await self._repository.create_revision(equestrian_id=equestrian_id, operation_id=uuid4(), snapshot=snapshot)

    async def canonical_ids(self, *, equestrian_id: UUID, ids: dict[str, UUID]) -> dict[str, UUID]:
        # Resolve each UUID once, preserving URL labels rather than persisting URL
        # rewrites. A later DELETE rule may legitimately admit the old path again.
        canonical = {
            page_id: await self._repository.resolve_redirect(equestrian_id=equestrian_id, page_id=page_id)
            for page_id in dict.fromkeys(ids.values())
        }
        return {value: canonical[page_id] for value, page_id in ids.items()}
