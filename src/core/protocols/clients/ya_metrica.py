from collections.abc import Sequence
from datetime import date
from typing import Protocol
from uuid import UUID

from core.schemas.clients.ya_metrica import (
    PageStatsRanking,
    PageStatsTable,
    RankingDirection,
    SearchQueryStatsRanking,
    SearchQueryStatsTable,
)
from core.services.metrics_period import PeriodColumn


class YaMetricaClientProtocol(Protocol):
    async def table(
        self, *, equestrian_id: UUID, metric: str, columns: Sequence[PeriodColumn], page_ids: Sequence[UUID]
    ) -> PageStatsTable: ...

    async def ranking(
        self, *, equestrian_id: UUID, metric: str, date_from: date, date_to: date, direction: RankingDirection
    ) -> PageStatsRanking: ...

    async def query_table(
        self, *, equestrian_id: UUID, metric: str, columns: Sequence[PeriodColumn], search_query_ids: Sequence[UUID]
    ) -> SearchQueryStatsTable: ...

    async def query_ranking(
        self, *, equestrian_id: UUID, metric: str, date_from: date, date_to: date, direction: RankingDirection
    ) -> SearchQueryStatsRanking: ...
