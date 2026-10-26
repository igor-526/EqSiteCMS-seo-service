from collections.abc import Sequence
from typing import Literal, Protocol
from uuid import UUID

from core.entities import SearchQuery


class SearchQueryRepositoryProtocol(Protocol):
    async def resolve(self, *, equestrian_id: UUID, values: Sequence[str]) -> dict[str, UUID]: ...

    async def get_filtered(
        self,
        *,
        equestrian_id: UUID,
        search: str | None = None,
        ordering: Literal["query", "-query"] | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[SearchQuery]: ...

    async def count_filtered(self, *, equestrian_id: UUID, search: str | None = None) -> int: ...
