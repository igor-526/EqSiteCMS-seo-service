from typing import Annotated
from uuid import UUID

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends

from api.route import TenantTransactionalRoute
from containers.services import ServicesContainer
from core.schemas.api import ParseJobInDto, ParseJobOutDto
from core.services.parsing import ParseDispatchService
from depends.tenant import get_equestrian_id

router = APIRouter(route_class=TenantTransactionalRoute)


@router.post("", status_code=202, response_model=ParseJobOutDto)
@inject
async def create(
    data: ParseJobInDto,
    equestrian_id: Annotated[UUID, Depends(get_equestrian_id)],
    service: Annotated[ParseDispatchService, Depends(Provide[ServicesContainer.parse_dispatch_service])],
) -> ParseJobOutDto:
    result = await service.dispatch_manual(provider=data.provider, equestrian_id=equestrian_id)
    return ParseJobOutDto.model_validate(result)
