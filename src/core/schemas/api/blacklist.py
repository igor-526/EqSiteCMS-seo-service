from datetime import datetime
from uuid import UUID

from pydantic import ConfigDict

from core.schemas.base import Schema


class BlacklistItemInDto(Schema):
    model_config = ConfigDict(extra="forbid")

    value: str


class BlacklistItemOutDto(Schema):
    id: UUID
    value: str
    created_at: datetime
