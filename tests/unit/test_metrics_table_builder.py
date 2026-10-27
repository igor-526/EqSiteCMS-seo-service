from datetime import UTC, datetime
from uuid import uuid4

import pytest

from core.schemas.clients.ya_metrica import PageStatsTable
from core.services.metrics_period import MetricsPeriodResolver
from core.services.metrics_table.builder import MetricsTableBuilder
from core.services.metrics_table.header_formatter import PeriodHeaderFormatter
from core.services.metrics_table.registry import MetricRegistry
from core.services.metrics_table.value_formatter import MetricValueFormatter


def builder() -> MetricsTableBuilder:
    return MetricsTableBuilder(
        registry=MetricRegistry(), value_formatter=MetricValueFormatter(), header_formatter=PeriodHeaderFormatter()
    )


def test_ordered_sparse_table_and_metadata() -> None:
    page, empty, foreign = uuid4(), uuid4(), uuid4()
    period = MetricsPeriodResolver(clock=lambda: datetime(2026, 10, 26, tzinfo=UTC)).resolve(mode="week")
    first, last = period.columns[0], period.columns[-1]
    stats = PageStatsTable.model_validate(
        {
            "metric": "bounce_rate",
            "values": [
                {"page_id": page, "bucket": first.key, "value": 16.66666667},
                {"page_id": page, "bucket": last.key, "value": 0},
                {"page_id": foreign, "bucket": first.key, "value": 12},
                {"page_id": page, "bucket": "2020-01-01", "value": 12},
            ],
        }
    )
    ordering = "-metric:" + first.key
    table = builder().build(
        period=period, metric="bounce_rate", page_ids=[page, empty], stats=stats, ordering=ordering
    )
    assert table.version == 1 and table.row_key == "id"
    assert table.period == period.period and table.ordering == ordering
    assert table.metric.model_dump() == {"key": "bounce_rate", "title": "Доля отказов", "value_type": "percent"}
    assert [column.key for column in table.columns] == ["metric:" + column.key for column in period.columns]
    assert table.columns[0].values[str(page)].text == "16,67%"
    assert table.columns[0].values[str(page)].value == 16.66666667
    assert set(table.columns[0].values) == {str(page)}
    assert table.columns[-1].values[str(page)].text == "0%"
    assert table.columns[1].values == {}
    assert table.columns[-1].title == "26 окт – 1 ноя"
    assert table.columns[-1].hint == "26.10.2026"
    for column in table.columns:
        assert column.sortable is True and column.align == "right"
        assert column.empty_text == "—" and column.value_type == "percent"
        assert str(empty) not in column.values and str(foreign) not in column.values


@pytest.mark.parametrize(
    "metric",
    [
        "visits",
        "users",
        "pageviews",
        "bounces",
        "bounce_rate",
        "page_depth",
        "avg_visit_duration_seconds",
        "visit_duration_seconds",
    ],
)
def test_empty_table_keeps_columns(metric: str) -> None:
    period = MetricsPeriodResolver(clock=lambda: datetime(2026, 10, 26, tzinfo=UTC)).resolve()
    table = builder().build(period=period, metric=metric, page_ids=[])
    assert len(table.columns) == 7 and table.ordering is None
    assert all(column.values == {} for column in table.columns)
    assert table.metric.key == metric


def test_integer_cell_and_applied_non_metric_ordering() -> None:
    page = uuid4()
    period = MetricsPeriodResolver(clock=lambda: datetime(2026, 10, 26, tzinfo=UTC)).resolve()
    stats = PageStatsTable.model_validate(
        {
            "metric": "visits",
            "values": [
                {"page_id": page, "bucket": period.columns[0].key, "value": 12345},
            ],
        }
    )
    table = builder().build(period=period, metric="visits", page_ids=[page], stats=stats, ordering="-page")
    assert table.ordering == "-page"
    assert table.columns[0].values[str(page)].text == "12\u00a0345"
    assert table.columns[0].values[str(page)].value == 12345


def test_mismatched_metric_is_rejected() -> None:
    period = MetricsPeriodResolver().resolve()
    with pytest.raises(ValueError, match="Metric mismatch"):
        builder().build(period=period, metric="visits", page_ids=[], stats=PageStatsTable(metric="users", values=[]))
