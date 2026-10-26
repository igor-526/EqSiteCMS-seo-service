from typing import Literal
from uuid import UUID

from core.entities import PaginatedEntities
from core.exceptions import InvalidValueError
from core.protocols.repositories import (
    BlacklistedHashRepositoryProtocol,
    BlacklistedQueryParamRepositoryProtocol,
    PageRepositoryProtocol,
)
from core.schemas.api import PageOutDto, SeoResolveInDto, SeoResolveItemOutDto, SeoResolveOutDto

from .page_normalizer import PageNormalizer


class PageService:
    def __init__(
        self,
        page_repository: PageRepositoryProtocol,
        page_normalizer: PageNormalizer,
        blacklisted_query_param_repository: BlacklistedQueryParamRepositoryProtocol,
        blacklisted_hash_repository: BlacklistedHashRepositoryProtocol,
    ) -> None:
        self._page_repository = page_repository
        self._page_normalizer = page_normalizer
        self._blacklisted_query_param_repository = blacklisted_query_param_repository
        self._blacklisted_hash_repository = blacklisted_hash_repository

    async def resolve(self, *, equestrian_id: UUID, data: SeoResolveInDto) -> SeoResolveOutDto:
        params = await self._blacklisted_query_param_repository.get_values(equestrian_id=equestrian_id)
        hashes = await self._blacklisted_hash_repository.get_values(equestrian_id=equestrian_id)
        normalized = []
        for index, source in enumerate(data.items):
            try:
                normalized.append(
                    self._page_normalizer.normalize(source, blacklisted_params=params, blacklisted_hashes=hashes)
                )
            except InvalidValueError as exc:
                raise exc.at_index(index) from None
        ids = await self._page_repository.resolve(equestrian_id=equestrian_id, values=list(dict.fromkeys(normalized)))
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
        ordering: Literal["page", "-page"] | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> PaginatedEntities[PageOutDto]:
        search = search.strip() or None if search is not None else None
        entities = await self._page_repository.get_filtered(
            equestrian_id=equestrian_id,
            search=search,
            ordering=ordering,
            limit=max(1, min(100, limit)),
            offset=max(0, offset),
        )
        total = await self._page_repository.count_filtered(equestrian_id=equestrian_id, search=search)
        return PaginatedEntities[PageOutDto](
            items=[PageOutDto.model_validate(entity) for entity in entities], total=total
        )
