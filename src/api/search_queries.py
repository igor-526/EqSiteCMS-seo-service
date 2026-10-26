from typing import Annotated, Literal
from uuid import UUID

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Query

from api.route import TenantTransactionalRoute
from containers.services import ServicesContainer
from core.entities import PaginatedEntities
from core.schemas.api import SearchQueryOutDto, SeoResolveInDto, SeoResolveOutDto
from core.services import SearchQueryService
from depends.tenant import get_equestrian_id

router = APIRouter(route_class=TenantTransactionalRoute)


@router.post("/resolve", response_model=SeoResolveOutDto)
@inject
async def resolve(
    data: SeoResolveInDto,
    equestrian_id: Annotated[UUID, Depends(get_equestrian_id)],
    service: Annotated[SearchQueryService, Depends(Provide[ServicesContainer.search_query_service])],
) -> SeoResolveOutDto:
    return await service.resolve(equestrian_id=equestrian_id, data=data)


@router.get("", response_model=PaginatedEntities[SearchQueryOutDto])
@inject
async def list_filtered(
    equestrian_id: Annotated[UUID, Depends(get_equestrian_id)],
    service: Annotated[SearchQueryService, Depends(Provide[ServicesContainer.search_query_service])],
    search: Annotated[str | None, Query()] = None,
    ordering: Annotated[Literal["query", "-query"] | None, Query()] = None,
    limit: Annotated[int, Query()] = 20,
    offset: Annotated[int, Query()] = 0,
) -> PaginatedEntities[SearchQueryOutDto]:
    return await service.list_filtered(
        equestrian_id=equestrian_id,
        search=search,
        ordering=ordering,
        limit=limit,
        offset=offset,
    )
