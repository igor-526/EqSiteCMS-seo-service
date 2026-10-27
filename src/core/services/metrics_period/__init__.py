import calendar
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import cast

from core.exceptions import ClientError
from core.schemas.api.metrics_table import MetricsPeriodOutDto, PeriodMode

LIMITS = {"day": 31, "week": 16, "month": 12, "year": 5}
DEFAULT_LENGTHS = {"day": 7, "week": 4, "month": 6, "year": 2}


def start_of(mode: PeriodMode, value: date) -> date:
    if mode == "week":
        return value - timedelta(days=value.weekday())
    if mode == "month":
        return value.replace(day=1)
    if mode == "year":
        return value.replace(month=1, day=1)
    return value


def add_units(mode: PeriodMode, value: date, count: int) -> date:
    if mode in {"day", "week"}:
        ordinal = value.toordinal() + count * (7 if mode == "week" else 1)
        return date.fromordinal(min(date.max.toordinal(), max(1, ordinal)))
    months = (value.year - 1) * 12 + value.month - 1 + count * (12 if mode == "year" else 1)
    if months >= 9999 * 12:
        return date.max
    year, month = divmod(max(0, months), 12)
    return date(year + 1, month + 1, 1)


def end_of(mode: PeriodMode, value: date) -> date:
    if mode == "day":
        return value
    if mode == "week":
        return date.fromordinal(min(date.max.toordinal(), value.toordinal() + 6 - value.weekday()))
    if mode == "month":
        return value.replace(day=calendar.monthrange(value.year, value.month)[1])
    return value.replace(month=12, day=31)


@dataclass(frozen=True)
class PeriodColumn:
    key: str
    date_from: date
    date_to: date


@dataclass(frozen=True)
class ResolvedMetricsPeriod:
    period: MetricsPeriodOutDto
    columns: tuple[PeriodColumn, ...]


class MetricsPeriodResolver:
    def __init__(self, *, utc_offset_hours: int = 3, clock: Callable[[], datetime] | None = None) -> None:
        if type(utc_offset_hours) is not int or not -12 <= utc_offset_hours <= 14:
            raise ValueError("Invalid UTC offset")
        self._utc_offset_hours = utc_offset_hours
        self._clock = clock or (lambda: datetime.now(UTC))

    def resolve(
        self, *, mode: str = "day", date_from: date | None = None, date_to: date | None = None
    ) -> ResolvedMetricsPeriod:
        if mode not in LIMITS:
            raise ClientError("Некорректный режим периода")
        unit = cast(PeriodMode, mode)
        if (date_from is None) != (date_to is None):
            raise ClientError("Даты периода должны передаваться вместе")
        if date_from is not None and date_to is not None and date_from > date_to:
            raise ClientError("Начало периода позже конца")
        today = (self._clock().astimezone(UTC) + timedelta(hours=self._utc_offset_hours)).date()
        defaulted = date_from is None
        adjusted = clamped = False
        if date_from is None or date_to is None:
            start = add_units(unit, start_of(unit, today), -(DEFAULT_LENGTHS[unit] - 1))
            end = today
        else:
            start = start_of(unit, min(date_from, today))
            end = end_of(unit, date_to)
            adjusted = start != date_from or end != date_to
            max_end = end_of(unit, add_units(unit, start, LIMITS[unit] - 1))
            if end > max_end:
                end = max_end
                clamped = True
            if end > today:
                end = today
                adjusted = True
        columns: list[PeriodColumn] = []
        cursor = start
        while cursor <= end:
            column_end = min(end_of(unit, cursor), end)
            columns.append(PeriodColumn(key=cursor.isoformat(), date_from=cursor, date_to=column_end))
            if column_end == end:
                break
            cursor = add_units(unit, cursor, 1)
        return ResolvedMetricsPeriod(
            period=MetricsPeriodOutDto(
                mode=unit,
                date_from=start,
                date_to=end,
                requested_date_from=date_from,
                requested_date_to=date_to,
                defaulted=defaulted,
                adjusted=adjusted,
                clamped=clamped,
                today=today,
            ),
            columns=tuple(columns),
        )
