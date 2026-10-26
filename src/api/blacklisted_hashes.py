from typing import Annotated
from uuid import UUID

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Response

from api.route import TenantTransactionalRoute
from containers.services import ServicesContainer
from core.entities import PaginatedEntities
from core.schemas.api import BlacklistItemInDto, BlacklistItemOutDto
from core.services import BlacklistedHashService
from depends.tenant import get_equestrian_id

router = APIRouter(route_class=TenantTransactionalRoute)


@router.get("", response_model=PaginatedEntities[BlacklistItemOutDto])
@inject
async def list_all(
    equestrian_id: Annotated[UUID, Depends(get_equestrian_id)],
    service: Annotated[BlacklistedHashService, Depends(Provide[ServicesContainer.blacklisted_hash_service])],
) -> PaginatedEntities[BlacklistItemOutDto]:
    return await service.list_all(equestrian_id=equestrian_id)


@router.post("", response_model=BlacklistItemOutDto, status_code=201)
@inject
async def create(
    data: BlacklistItemInDto,
    equestrian_id: Annotated[UUID, Depends(get_equestrian_id)],
    service: Annotated[BlacklistedHashService, Depends(Provide[ServicesContainer.blacklisted_hash_service])],
) -> BlacklistItemOutDto:
    return await service.create(equestrian_id=equestrian_id, data=data)


@router.patch("/{item_id}", response_model=BlacklistItemOutDto)
@inject
async def update(
    item_id: UUID,
    data: BlacklistItemInDto,
    equestrian_id: Annotated[UUID, Depends(get_equestrian_id)],
    service: Annotated[BlacklistedHashService, Depends(Provide[ServicesContainer.blacklisted_hash_service])],
) -> BlacklistItemOutDto:
    return await service.update(equestrian_id=equestrian_id, item_id=item_id, data=data)


@router.delete("/{item_id}", status_code=204, response_class=Response)
@inject
async def delete(
    item_id: UUID,
    equestrian_id: Annotated[UUID, Depends(get_equestrian_id)],
    service: Annotated[BlacklistedHashService, Depends(Provide[ServicesContainer.blacklisted_hash_service])],
) -> Response:
    await service.delete(equestrian_id=equestrian_id, item_id=item_id)
    return Response(status_code=204)
