from typing import Protocol
from uuid import UUID

from core.entities import BlacklistedHash

from .base_repository import BaseRepositoryProtocol


class BlacklistedHashRepositoryProtocol(BaseRepositoryProtocol[BlacklistedHash], Protocol):
    async def get_values(self, *, equestrian_id: UUID) -> frozenset[str]: ...

    async def get_by_value(self, value: str, *, equestrian_id: UUID) -> BlacklistedHash | None: ...
