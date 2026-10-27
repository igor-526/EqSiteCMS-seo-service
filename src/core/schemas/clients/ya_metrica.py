from datetime import date
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StrictFloat, StrictInt

MetricKey = Literal[
    "visits",
    "users",
    "pageviews",
    "bounces",
    "bounce_rate",
    "page_depth",
    "avg_visit_duration_seconds",
    "visit_duration_seconds",
]
RankingDirection = Literal["asc", "desc"]


class PageStatsValue(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    page_id: UUID
    bucket: date
    value: StrictInt | StrictFloat


class PageStatsTable(BaseModel):
    metric: MetricKey
    values: list[PageStatsValue]


class PageStatsRanking(BaseModel):
    metric: MetricKey
    direction: RankingDirection
    page_ids: list[UUID]


class SearchQueryStatsValue(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    search_query_id: UUID
    bucket: date
    value: StrictInt | StrictFloat


class SearchQueryStatsTable(BaseModel):
    metric: MetricKey
    values: list[SearchQueryStatsValue]


class SearchQueryStatsRanking(BaseModel):
    metric: MetricKey
    direction: RankingDirection
    search_query_ids: list[UUID]
