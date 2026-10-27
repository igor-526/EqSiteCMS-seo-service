from collections.abc import Sequence
from typing import Literal, Protocol
from uuid import UUID

from core.entities import Page


class PageRepositoryProtocol(Protocol):
    async def resolve(self, *, equestrian_id: UUID, values: Sequence[str]) -> dict[str, UUID]: ...

    async def get_filtered(
        self,
        *,
        equestrian_id: UUID,
        search: str | None = None,
        ordering: Literal["page", "-page", "created_at", "-created_at"] | None = None,
        limit: int = 20,
        offset: int = 0,
        ranked_page_ids: Sequence[UUID] | None = None,
    ) -> list[Page]: ...

    async def count_filtered(self, *, equestrian_id: UUID, search: str | None = None) -> int: ...
