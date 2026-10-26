from typing import Literal
from uuid import UUID

from core.entities import PaginatedEntities
from core.exceptions import InvalidValueError
from core.protocols.repositories import SearchQueryRepositoryProtocol
from core.schemas.api import SearchQueryOutDto, SeoResolveInDto, SeoResolveItemOutDto, SeoResolveOutDto

from .search_query_normalizer import SearchQueryNormalizer


class SearchQueryService:
    def __init__(
        self,
        search_query_repository: SearchQueryRepositoryProtocol,
        search_query_normalizer: SearchQueryNormalizer,
    ) -> None:
        self._search_query_repository = search_query_repository
        self._search_query_normalizer = search_query_normalizer

    async def resolve(self, *, equestrian_id: UUID, data: SeoResolveInDto) -> SeoResolveOutDto:
        normalized = []
        for index, source in enumerate(data.items):
            try:
                normalized.append(self._search_query_normalizer.normalize(source))
            except InvalidValueError as exc:
                raise exc.at_index(index) from None
        ids = await self._search_query_repository.resolve(
            equestrian_id=equestrian_id, values=list(dict.fromkeys(normalized))
        )
        return SeoResolveOutDto(
            items=[
                SeoResolveItemOutDto(source=source, normalized=value, id=ids[value])
                for source, value in zip(data.items, normalized, strict=True)
            ]
        )

    async def list_filtered(
        self,
        *,
        equestrian_id: UUID,
        search: str | None = None,
        ordering: Literal["query", "-query"] | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> PaginatedEntities[SearchQueryOutDto]:
        search = search.strip() or None if search is not None else None
        entities = await self._search_query_repository.get_filtered(
            equestrian_id=equestrian_id,
            search=search,
            ordering=ordering,
            limit=max(1, min(100, limit)),
            offset=max(0, offset),
        )
        total = await self._search_query_repository.count_filtered(equestrian_id=equestrian_id, search=search)
        return PaginatedEntities[SearchQueryOutDto](
            items=[SearchQueryOutDto.model_validate(entity) for entity in entities], total=total
        )
