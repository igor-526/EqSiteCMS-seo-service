from datetime import date, datetime
from uuid import UUID

from core.schemas.api.metrics_table import MetricsTableOutDto
from core.schemas.base import Schema


class PageOutDto(Schema):
    id: UUID
    page: str
    created_at: datetime


class PageListInDto(Schema):
    search: str | None = None
    ordering: str | None = None
    mode: str = "day"
    metric: str = "visits"
    date_from: date | None = None
    date_to: date | None = None
    limit: int = 20
    offset: int = 0


class PageListOutDto(Schema):
    items: list[PageOutDto]
    total: int
    metrics_table: MetricsTableOutDto
