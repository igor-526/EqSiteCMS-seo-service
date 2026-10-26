from datetime import datetime
from uuid import UUID

from core.schemas.base import Schema


class SearchQueryOutDto(Schema):
    id: UUID
    query: str
    created_at: datetime
