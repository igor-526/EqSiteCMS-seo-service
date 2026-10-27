"""Entity-neutral input for the shared metrics table builder."""

from dataclasses import dataclass
from datetime import date
from uuid import UUID

from core.schemas.clients.ya_metrica import PageStatsTable, SearchQueryStatsTable


@dataclass(frozen=True)
class MetricsValue:
    row_id: UUID
    bucket: date
    value: int | float


@dataclass(frozen=True)
class MetricsValues:
    metric: str
    values: tuple[MetricsValue, ...]

    @classmethod
    def from_pages(cls, stats: PageStatsTable) -> MetricsValues:
        return cls(stats.metric, tuple(MetricsValue(v.page_id, v.bucket, v.value) for v in stats.values))

    @classmethod
    def from_queries(cls, stats: SearchQueryStatsTable) -> MetricsValues:
        return cls(stats.metric, tuple(MetricsValue(v.search_query_id, v.bucket, v.value) for v in stats.values))
