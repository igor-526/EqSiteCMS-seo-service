from datetime import date
from typing import Literal

from pydantic import Field

from core.schemas.base import Schema

PeriodMode = Literal["day", "week", "month", "year"]


class MetricsPeriodOutDto(Schema):
    mode: PeriodMode
    date_from: date
    date_to: date
    requested_date_from: date | None = None
    requested_date_to: date | None = None
    defaulted: bool = False
    adjusted: bool = False
    clamped: bool = False
    today: date


class MetricOutDto(Schema):
    key: str
    title: str
    value_type: str


class MetricCellOutDto(Schema):
    text: str
    value: int | float | None
    hint: str | None = None


class MetricColumnOutDto(Schema):
    key: str
    title: str
    hint: str | None = None
    sortable: bool = True
    value_type: str
    align: Literal["left", "right"] = "right"
    empty_text: str = "—"
    values: dict[str, MetricCellOutDto] = Field(default_factory=dict)


class MetricsTableOutDto(Schema):
    version: int = 1
    row_key: str = "id"
    period: MetricsPeriodOutDto
    metric: MetricOutDto
    ordering: str | None = None
    columns: list[MetricColumnOutDto]
