from datetime import datetime
from uuid import UUID

from core.schemas.base import Schema


class PageOutDto(Schema):
    id: UUID
    page: str
    created_at: datetime
