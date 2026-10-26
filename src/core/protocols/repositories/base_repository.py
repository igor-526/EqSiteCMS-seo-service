from typing import Protocol
from uuid import UUID

from core.entities import Entity


class BaseRepositoryProtocol[E: Entity](Protocol):
    """CRUD настроек: selector обязателен, включая операции с известным UUID."""

    async def get_all(self, *, equestrian_id: UUID) -> list[E]: ...

    async def get_by_id(self, id: UUID, *, equestrian_id: UUID) -> E | None: ...

    async def create(self, entity: E, *, equestrian_id: UUID) -> E: ...

    async def update(self, entity: E, *, equestrian_id: UUID) -> E: ...

    async def delete(self, id: UUID, *, equestrian_id: UUID) -> None: ...
