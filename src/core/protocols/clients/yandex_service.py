from typing import Protocol
from uuid import UUID

from core.schemas.clients.yandex_service import MetricaBinding, MetricaBindingList


class YandexServiceClientProtocol(Protocol):
    async def list_metrica_bindings(self, *, offset: int, limit: int) -> MetricaBindingList: ...

    async def get_metrica_binding(self, *, equestrian_id: UUID) -> MetricaBinding | None: ...
