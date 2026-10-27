from collections.abc import Sequence
from uuid import UUID

from core.schemas.api.metrics_table import MetricCellOutDto, MetricColumnOutDto, MetricsTableOutDto
from core.schemas.clients.ya_metrica import PageStatsTable
from core.services.metrics_period import ResolvedMetricsPeriod
from core.services.metrics_table.header_formatter import PeriodHeaderFormatter
from core.services.metrics_table.input import MetricsValues
from core.services.metrics_table.registry import MetricRegistry
from core.services.metrics_table.value_formatter import MetricValueFormatter


class MetricsTableBuilder:
    def __init__(
        self,
        *,
        registry: MetricRegistry,
        value_formatter: MetricValueFormatter,
        header_formatter: PeriodHeaderFormatter,
    ) -> None:
        self._registry = registry
        self._value_formatter = value_formatter
        self._header_formatter = header_formatter

    def build(
        self,
        *,
        period: ResolvedMetricsPeriod,
        metric: str,
        page_ids: Sequence[UUID],
        stats: PageStatsTable | None = None,
        ordering: str | None = None,
    ) -> MetricsTableOutDto:
        return self.build_rows(
            period=period,
            metric=metric,
            row_ids=page_ids,
            stats=MetricsValues.from_pages(stats) if stats is not None else None,
            ordering=ordering,
        )

    def build_rows(
        self,
        *,
        period: ResolvedMetricsPeriod,
        metric: str,
        row_ids: Sequence[UUID],
        stats: MetricsValues | None = None,
        ordering: str | None = None,
    ) -> MetricsTableOutDto:
        metadata = self._registry.get(metric)
        if stats is not None and stats.metric != metric:
            raise ValueError("Metric mismatch")
        allowed_row_ids = set(row_ids)
        columns: list[MetricColumnOutDto] = []
        by_bucket: dict[str, dict[str, MetricCellOutDto]] = {}
        for column in period.columns:
            header = self._header_formatter.format(column, period.period)
            values: dict[str, MetricCellOutDto] = {}
            columns.append(
                MetricColumnOutDto(
                    key=f"metric:{column.key}",
                    title=header.title,
                    hint=header.hint,
                    value_type=metadata.value_type,
                    values=values,
                )
            )
            # Pydantic may copy dict inputs; use the actual DTO's mapping below.
            by_bucket[column.key] = columns[-1].values
        if stats is not None:
            for item in stats.values:
                bucket_values = by_bucket.get(item.bucket.isoformat())
                if bucket_values is not None and item.row_id in allowed_row_ids:
                    bucket_values[str(item.row_id)] = MetricCellOutDto(
                        text=self._value_formatter.format(item.value, metadata.value_type),
                        value=item.value,
                    )
        return MetricsTableOutDto(period=period.period, metric=metadata, ordering=ordering, columns=columns)
