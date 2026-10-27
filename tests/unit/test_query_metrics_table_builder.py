"""UT-SQ-02: neutral adapter uses the exact same formatter and builder as pages."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from core.schemas.clients.ya_metrica import PageStatsTable, SearchQueryStatsTable
from core.services.metrics_period import MetricsPeriodResolver
from core.services.metrics_table.input import MetricsValues
from tests.unit.test_metrics_table_builder import builder


@pytest.mark.parametrize("mode", ["day", "week", "month", "year"])
@pytest.mark.parametrize(
    ("metric", "value", "text"),
    [
        ("visits", 12345, "12\u00a0345"),
        ("users", 0, "0"),
        ("pageviews", 12, "12"),
        ("bounces", 0, "0"),
        ("bounce_rate", 16.66666667, "16,67%"),
        ("page_depth", 1.755, "1,76"),
        ("avg_visit_duration_seconds", 114.86666667, "1 мин 55 с"),
        ("visit_duration_seconds", 0, "0 с"),
    ],
)
def test_query_page_parity(mode: str, metric: str, value: int | float, text: str) -> None:
    row, missing, foreign = uuid4(), uuid4(), uuid4()
    period = MetricsPeriodResolver(clock=lambda: datetime(2026, 1, 2, tzinfo=UTC)).resolve(mode=mode)
    wire = {
        "metric": metric,
        "future_field": True,
        "values": [
            {"search_query_id": row, "bucket": period.columns[0].key, "value": value, "extra": "ignored"},
            {"search_query_id": foreign, "bucket": period.columns[0].key, "value": 999},
            {"search_query_id": row, "bucket": "1900-01-01", "value": 999},
        ],
    }
    stats = SearchQueryStatsTable.model_validate(wire)
    neutral = MetricsValues.from_queries(stats)
    assert neutral.values[0].row_id == row
    page_stats = PageStatsTable.model_validate(
        {
            "metric": metric,
            "values": [dict(page_id=v.search_query_id, bucket=v.bucket, value=v.value) for v in stats.values],
        }
    )
    table = builder().build_rows(period=period, metric=metric, row_ids=[row, missing], stats=neutral, ordering="query")
    assert table == builder().build(
        period=period, metric=metric, page_ids=[row, missing], stats=page_stats, ordering="query"
    )
    assert table.columns[0].values[str(row)].text == text
    assert set(table.columns[0].values) == {str(row)}
    assert all(set(column.values) <= {str(row)} for column in table.columns)
    empty = builder().build_rows(period=period, metric=metric, row_ids=[], stats=neutral)
    assert all(column.values == {} for column in empty.columns)


def test_neutral_metric_mismatch() -> None:
    with pytest.raises(ValueError, match="Metric mismatch"):
        builder().build_rows(
            period=MetricsPeriodResolver().resolve(),
            metric="visits",
            row_ids=[],
            stats=MetricsValues.from_queries(SearchQueryStatsTable(metric="users", values=[])),
        )
