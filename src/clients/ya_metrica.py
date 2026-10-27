import logging
from collections.abc import Sequence
from contextvars import ContextVar
from datetime import date
from typing import TypeVar
from uuid import UUID

import httpx
from pydantic import BaseModel

from core.exceptions import MetricaUnavailableError
from core.schemas.clients.ya_metrica import (
    PageStatsRanking,
    PageStatsTable,
    RankingDirection,
    SearchQueryStatsRanking,
    SearchQueryStatsTable,
)
from core.services.metrics_period import PeriodColumn

ResponseDto = TypeVar("ResponseDto", bound=BaseModel)
_request_active: ContextVar[bool] = ContextVar("metrica_request_active", default=False)


class _PeerLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not _request_active.get()


# HTTPX INFO and httpcore DEBUG can include peer URLs and response headers.
# Filters are task-local: unrelated requests in parallel are not suppressed.
for _logger in ("httpx", "httpcore.connection", "httpcore.http11", "httpcore.http2", "httpcore.proxy"):
    logging.getLogger(_logger).addFilter(_PeerLogFilter())


class YaMetricaClient:
    def __init__(
        self, *, base_url: str, timeout: float = 4.0, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._transport = transport

    async def _get(self, path: str, params: list[tuple[str, str]], schema: type[ResponseDto]) -> ResponseDto:
        token = _request_active.set(True)
        try:
            # A fresh client cannot retain cookies from earlier responses; environment
            # proxies and .netrc auth are deliberately ignored. Redirects are not followed.
            url = httpx.URL(f"{self._base_url}{path}")
            if url.username or url.password:
                raise MetricaUnavailableError()
            async with httpx.AsyncClient(
                timeout=self._timeout, transport=self._transport, trust_env=False, follow_redirects=False
            ) as client:
                response = await client.get(url, params=httpx.QueryParams(tuple(params)))
                if response.status_code != 200:
                    raise MetricaUnavailableError()
                return schema.model_validate(response.json())
        except httpx.HTTPError, ValueError, TypeError:
            raise MetricaUnavailableError() from None
        finally:
            _request_active.reset(token)

    async def table(
        self, *, equestrian_id: UUID, metric: str, columns: Sequence[PeriodColumn], page_ids: Sequence[UUID]
    ) -> PageStatsTable:
        params = [("equestrian_id", str(equestrian_id)), ("metric", metric)]
        params.extend(
            ("bucket", f"{column.date_from.isoformat()}..{column.date_to.isoformat()}") for column in columns
        )
        params.extend(("page_ids", str(page_id)) for page_id in page_ids)
        result = await self._get("/api/metrica/page-stats/table", params, PageStatsTable)
        if result.metric != metric:
            raise MetricaUnavailableError()
        return result

    async def ranking(
        self, *, equestrian_id: UUID, metric: str, date_from: date, date_to: date, direction: RankingDirection
    ) -> PageStatsRanking:
        result = await self._get(
            "/api/metrica/page-stats/ranking",
            [
                ("equestrian_id", str(equestrian_id)),
                ("metric", metric),
                ("date_from", date_from.isoformat()),
                ("date_to", date_to.isoformat()),
                ("direction", direction),
            ],
            PageStatsRanking,
        )
        if result.metric != metric or result.direction != direction:
            raise MetricaUnavailableError()
        return result

    async def query_table(
        self, *, equestrian_id: UUID, metric: str, columns: Sequence[PeriodColumn], search_query_ids: Sequence[UUID]
    ) -> SearchQueryStatsTable:
        params = [("equestrian_id", str(equestrian_id)), ("metric", metric)]
        params.extend(
            ("bucket", f"{column.date_from.isoformat()}..{column.date_to.isoformat()}") for column in columns
        )
        params.extend(("search_query_ids", str(search_query_id)) for search_query_id in search_query_ids)
        result = await self._get("/api/metrica/search-query-stats/table", params, SearchQueryStatsTable)
        if result.metric != metric:
            raise MetricaUnavailableError()
        return result

    async def query_ranking(
        self, *, equestrian_id: UUID, metric: str, date_from: date, date_to: date, direction: RankingDirection
    ) -> SearchQueryStatsRanking:
        result = await self._get(
            "/api/metrica/search-query-stats/ranking",
            [
                ("equestrian_id", str(equestrian_id)),
                ("metric", metric),
                ("date_from", date_from.isoformat()),
                ("date_to", date_to.isoformat()),
                ("direction", direction),
            ],
            SearchQueryStatsRanking,
        )
        if result.metric != metric or result.direction != direction:
            raise MetricaUnavailableError()
        return result
