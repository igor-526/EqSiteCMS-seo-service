from datetime import date
from decimal import Decimal

import pytest

from core.schemas.api.metrics_table import MetricsPeriodOutDto, MetricsTableOutDto, PeriodMode
from core.services.metrics_period import PeriodColumn
from core.services.metrics_table.header_formatter import PeriodHeaderFormatter
from core.services.metrics_table.registry import MetricRegistry
from core.services.metrics_table.value_formatter import MetricValueFormatter


@pytest.mark.parametrize(
    ("value", "value_type", "expected"),
    [
        (12345, "integer", "12\u00a0345"),
        (0, "integer", "0"),
        (Decimal("1.755"), "decimal", "1,76"),
        (2.0, "decimal", "2"),
        (Decimal("1.005"), "decimal", "1,01"),
        (Decimal("0.004"), "decimal", "0"),
        (16.66666667, "percent", "16,67%"),
        (0, "percent", "0%"),
        (114.86666667, "duration", "1 мин 55 с"),
        (42, "duration", "42 с"),
        (7500, "duration", "2 ч 5 мин"),
        (7509, "duration", "2 ч 5 мин"),
        (3605, "duration", "1 ч 5 с"),
        (0, "duration", "0 с"),
        (Decimal("59.5"), "duration", "1 мин"),
        (Decimal("0.5"), "duration", "1 с"),
        (None, "integer", "—"),
    ],
)
def test_values_ut_sp_06(value: int | float | Decimal | None, value_type: str, expected: str) -> None:
    assert MetricValueFormatter().format(value, value_type) == expected


@pytest.mark.parametrize(
    ("key", "title", "value_type"),
    [
        ("visits", "Визиты", "integer"),
        ("users", "Посетители", "integer"),
        ("pageviews", "Просмотры", "integer"),
        ("bounces", "Отказы", "integer"),
        ("bounce_rate", "Доля отказов", "percent"),
        ("page_depth", "Глубина просмотра", "decimal"),
        ("avg_visit_duration_seconds", "Время на сайте", "duration"),
        ("visit_duration_seconds", "Суммарное время визитов", "duration"),
    ],
)
def test_registry_ut_sp_06(key: str, title: str, value_type: str) -> None:
    assert MetricRegistry().get(key).model_dump() == {"key": key, "title": title, "value_type": value_type}


@pytest.mark.parametrize(
    ("mode", "start", "end", "title", "hint"),
    [
        ("day", "2025-12-31", "2025-12-31", "31 дек", "31.12.2025"),
        ("week", "2026-10-19", "2026-10-25", "19–25 окт", "19.10.2026 – 25.10.2026"),
        ("week", "2025-10-20", "2025-10-26", "20–26 окт 25", "20.10.2025 – 26.10.2025"),
        ("week", "2026-10-26", "2026-11-01", "26 окт – 1 ноя", "26.10.2026 – 01.11.2026"),
        ("week", "2026-10-26", "2026-10-28", "26 окт – 1 ноя", "26.10.2026 – 28.10.2026"),
        ("week", "2026-10-26", "2026-10-26", "26 окт – 1 ноя", "26.10.2026"),
        ("week", "2025-12-29", "2026-01-04", "29 дек 25 – 4 янв 26", "29.12.2025 – 04.01.2026"),
        ("week", "2025-10-27", "2025-11-02", "27 окт – 2 ноя 25", "27.10.2025 – 02.11.2025"),
        ("month", "2025-03-01", "2025-03-31", "мар 25", "01.03.2025 – 31.03.2025"),
        ("month", "2026-10-01", "2026-10-26", "окт", "01.10.2026 – 26.10.2026"),
        ("year", "2025-01-01", "2025-12-31", "2025", "01.01.2025 – 31.12.2025"),
    ],
)
def test_headers_ut_sp_07(mode: PeriodMode, start: str, end: str, title: str, hint: str) -> None:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    period = MetricsPeriodOutDto(mode=mode, date_from=first, date_to=last, today=date(2026, 10, 28))
    column = PeriodColumn(key=start, date_from=first, date_to=last)
    result = PeriodHeaderFormatter().format(column, period)
    assert result.title == title
    assert result.hint == hint


def test_header_uses_period_year_ut_sp_07() -> None:
    first = date(2025, 3, 1)
    period = MetricsPeriodOutDto(mode="month", date_from=first, date_to=first, today=first)
    assert PeriodHeaderFormatter().format(PeriodColumn("2025-03-01", first, first), period).title == "мар"


def test_empty_table_dto() -> None:
    first = date(2026, 10, 26)
    period = MetricsPeriodOutDto(mode="day", date_from=first, date_to=first, today=first)
    table = MetricsTableOutDto(period=period, metric=MetricRegistry().get(), columns=[])
    assert table.version == 1 and table.row_key == "id" and table.ordering is None
    assert table.model_dump(mode="json")["period"]["today"] == "2026-10-26"
