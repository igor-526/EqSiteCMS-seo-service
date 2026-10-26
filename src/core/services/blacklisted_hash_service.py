import unicodedata
from datetime import UTC, datetime
from uuid import UUID

from core.entities import BlacklistedHash, PaginatedEntities
from core.exceptions import AlreadyExistsError, ClientError, NotFoundError
from core.protocols.repositories import BlacklistedHashRepositoryProtocol
from core.schemas.api import BlacklistItemInDto, BlacklistItemOutDto


class BlacklistedHashService:
    def __init__(self, blacklisted_hash_repository: BlacklistedHashRepositoryProtocol) -> None:
        self._blacklisted_hash_repository = blacklisted_hash_repository

    @staticmethod
    def _normalize(value: str) -> str:
        value = value.strip().removeprefix("#")
        invalid_chars = any(char.isspace() or unicodedata.category(char) == "Cc" or char == "#" for char in value)
        if not value or len(value) > 255 or invalid_chars:
            raise ClientError("Некорректное значение элемента черного списка")
        return value

    async def list_all(self, *, equestrian_id: UUID) -> PaginatedEntities[BlacklistItemOutDto]:
        entities = await self._blacklisted_hash_repository.get_all(equestrian_id=equestrian_id)
        return PaginatedEntities[BlacklistItemOutDto](
            items=[BlacklistItemOutDto.model_validate(entity) for entity in entities], total=len(entities)
        )

    async def create(self, *, equestrian_id: UUID, data: BlacklistItemInDto) -> BlacklistItemOutDto:
        value = self._normalize(data.value)
        if await self._blacklisted_hash_repository.get_by_value(value, equestrian_id=equestrian_id):
            raise AlreadyExistsError()
        entity = await self._blacklisted_hash_repository.create(
            BlacklistedHash(equestrian_id=equestrian_id, value=value), equestrian_id=equestrian_id
        )
        return BlacklistItemOutDto.model_validate(entity)

    async def update(self, *, equestrian_id: UUID, item_id: UUID, data: BlacklistItemInDto) -> BlacklistItemOutDto:
        entity = await self._blacklisted_hash_repository.get_by_id(item_id, equestrian_id=equestrian_id)
        if entity is None:
            raise NotFoundError()
        value = self._normalize(data.value)
        duplicate = await self._blacklisted_hash_repository.get_by_value(value, equestrian_id=equestrian_id)
        if duplicate is not None and duplicate.id != item_id:
            raise AlreadyExistsError()
        updated = entity.model_copy(update={"value": value, "updated_at": datetime.now(UTC)})
        saved = await self._blacklisted_hash_repository.update(updated, equestrian_id=equestrian_id)
        return BlacklistItemOutDto.model_validate(saved)

    async def delete(self, *, equestrian_id: UUID, item_id: UUID) -> None:
        if await self._blacklisted_hash_repository.get_by_id(item_id, equestrian_id=equestrian_id) is None:
            raise NotFoundError()
        await self._blacklisted_hash_repository.delete(item_id, equestrian_id=equestrian_id)
