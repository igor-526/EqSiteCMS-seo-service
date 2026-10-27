from dataclasses import dataclass
from datetime import date

from core.schemas.api.metrics_table import MetricsPeriodOutDto
from core.services.metrics_period import PeriodColumn, end_of

MONTHS = ("янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек")


@dataclass(frozen=True)
class PeriodHeader:
    title: str
    hint: str


class PeriodHeaderFormatter:
    def format(self, column: PeriodColumn, period: MetricsPeriodOutDto) -> PeriodHeader:
        start, actual_end = column.date_from, column.date_to
        full_end = end_of(period.mode, start)
        month = MONTHS[start.month - 1]
        suffix = f" {start.year % 100:02d}" if start.year != period.today.year else ""
        if period.mode == "day":
            title = f"{start.day} {month}"
        elif period.mode == "month":
            title = month + suffix
        elif period.mode == "year":
            title = str(start.year)
        elif start.year != full_end.year:
            title = f"{self._day(start, year=True)} – {self._day(full_end, year=True)}"
        elif start.month == full_end.month:
            title = f"{start.day}–{full_end.day} {month}{suffix}"
        else:
            title = f"{self._day(start)} – {self._day(full_end)}{suffix}"
        hint = start.strftime("%d.%m.%Y")
        if start != actual_end:
            hint += " – " + actual_end.strftime("%d.%m.%Y")
        return PeriodHeader(title=title, hint=hint)

    @staticmethod
    def _day(value: date, *, year: bool = False) -> str:
        return f"{value.day} {MONTHS[value.month - 1]}" + (f" {value.year % 100:02d}" if year else "")
