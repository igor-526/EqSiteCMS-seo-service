import re
from datetime import date, datetime
from typing import Annotated
from uuid import UUID

from pydantic import BeforeValidator

from core.schemas.api.metrics_table import MetricsTableOutDto
from core.schemas.base import Schema


def _validate_wire_date(value: object) -> str:
    # HTTP dates must not be coerced from epoch seconds or midnight datetimes.
    if not isinstance(value, str) or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None:
        raise ValueError("Expected YYYY-MM-DD")
    return value


SearchQueryWireDate = Annotated[date, BeforeValidator(_validate_wire_date)]


class SearchQueryOutDto(Schema):
    id: UUID
    query: str
    created_at: datetime


class SearchQueryListInDto(Schema):
    search: str | None = None
    ordering: str | None = None
    mode: str = "day"
    metric: str = "visits"
    date_from: date | None = None
    date_to: date | None = None
    limit: int = 20
    offset: int = 0


class SearchQueryListOutDto(Schema):
    items: list[SearchQueryOutDto]
    total: int
    metrics_table: MetricsTableOutDto
