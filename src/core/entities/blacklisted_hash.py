from uuid import UUID

from pydantic import Field

from .base import Entity, TimestampMixin


class BlacklistedHash(Entity, TimestampMixin):
    equestrian_id: UUID
    value: str = Field(max_length=255)
