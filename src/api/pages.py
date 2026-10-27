from datetime import date
from typing import Annotated
from uuid import UUID

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Query

from api.route import TenantTransactionalRoute
from containers.services import ServicesContainer
from core.schemas.api import SeoResolveInDto, SeoResolveOutDto
from core.schemas.api.pages import PageListOutDto
from core.services import PageService
from depends.tenant import get_equestrian_id

router = APIRouter(route_class=TenantTransactionalRoute)


@router.post("/resolve", response_model=SeoResolveOutDto)
@inject
async def resolve(
    data: SeoResolveInDto,
    equestrian_id: Annotated[UUID, Depends(get_equestrian_id)],
    service: Annotated[PageService, Depends(Provide[ServicesContainer.page_service])],
) -> SeoResolveOutDto:
    return await service.resolve(equestrian_id=equestrian_id, data=data)


@router.get("", response_model=PageListOutDto)
@inject
async def list_filtered(
    equestrian_id: Annotated[UUID, Depends(get_equestrian_id)],
    service: Annotated[PageService, Depends(Provide[ServicesContainer.page_service])],
    search: Annotated[str | None, Query()] = None,
    ordering: Annotated[str | None, Query()] = None,
    mode: Annotated[str, Query()] = "day",
    metric: Annotated[str, Query()] = "visits",
    date_from: Annotated[date | None, Query()] = None,
    date_to: Annotated[date | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> PageListOutDto:
    return await service.list_filtered(
        equestrian_id=equestrian_id,
        search=search,
        ordering=ordering,
        mode=mode,
        metric=metric,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )
