from typing import Protocol
from uuid import UUID

from core.entities import BlacklistedQueryParam

from .base_repository import BaseRepositoryProtocol


class BlacklistedQueryParamRepositoryProtocol(BaseRepositoryProtocol[BlacklistedQueryParam], Protocol):
    async def get_values(self, *, equestrian_id: UUID) -> frozenset[str]: ...

    async def get_by_value(self, value: str, *, equestrian_id: UUID) -> BlacklistedQueryParam | None: ...
