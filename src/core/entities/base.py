from datetime import UTC, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Entity(BaseModel):
    id: UUID = Field(default_factory=uuid4)


class TimestampMixin(BaseModel):
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime | None = None


class PaginatedEntities[T](BaseModel):
    """Пагинированный список сущностей или DTO."""

    items: list[T] = Field(default_factory=list, description="Список элементов")
    total: int = Field(default=0, description="Общее количество элементов")
