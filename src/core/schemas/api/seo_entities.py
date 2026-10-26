from uuid import UUID

from pydantic import ConfigDict, Field

from core.schemas.base import Schema


class SeoResolveInDto(Schema):
    model_config = ConfigDict(extra="forbid")

    items: list[str] = Field(min_length=1, max_length=1000)


class SeoResolveItemOutDto(Schema):
    source: str
    normalized: str
    id: UUID


class SeoResolveOutDto(Schema):
    items: list[SeoResolveItemOutDto]
