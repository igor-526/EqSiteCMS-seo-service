import re
from datetime import date
from typing import Literal, cast
from uuid import UUID

from pydantic import ValidationError

from core.exceptions import ClientError, InvalidValueError
from core.protocols.clients.ya_metrica import YaMetricaClientProtocol
from core.protocols.repositories import (
    BlacklistedHashRepositoryProtocol,
    BlacklistedQueryParamRepositoryProtocol,
    PageRepositoryProtocol,
)
from core.schemas.api import PageOutDto, SeoResolveInDto, SeoResolveItemOutDto, SeoResolveOutDto
from core.schemas.api.pages import PageListInDto, PageListOutDto
from core.services.metrics_period import MetricsPeriodResolver
from core.services.metrics_table.builder import MetricsTableBuilder
from core.services.metrics_table.registry import MetricRegistry

from .page_normalizer import PageNormalizer


class PageService:
    def __init__(
        self,
        page_repository: PageRepositoryProtocol,
        page_normalizer: PageNormalizer,
        blacklisted_query_param_repository: BlacklistedQueryParamRepositoryProtocol,
        blacklisted_hash_repository: BlacklistedHashRepositoryProtocol,
        *,
        ya_metrica_client: YaMetricaClientProtocol | None = None,
        metrics_period_resolver: MetricsPeriodResolver | None = None,
        metrics_table_builder: MetricsTableBuilder | None = None,
    ) -> None:
        self._ya_metrica_client = ya_metrica_client
        self._metrics_period_resolver = metrics_period_resolver
        self._metrics_table_builder = metrics_table_builder
        self._page_repository = page_repository
        self._page_normalizer = page_normalizer
        self._blacklisted_query_param_repository = blacklisted_query_param_repository
        self._blacklisted_hash_repository = blacklisted_hash_repository

    async def resolve(self, *, equestrian_id: UUID, data: SeoResolveInDto) -> SeoResolveOutDto:
        params = await self._blacklisted_query_param_repository.get_values(equestrian_id=equestrian_id)
        hashes = await self._blacklisted_hash_repository.get_values(equestrian_id=equestrian_id)
        normalized = []
        for index, source in enumerate(data.items):
            try:
                normalized.append(
                    self._page_normalizer.normalize(source, blacklisted_params=params, blacklisted_hashes=hashes)
                )
            except InvalidValueError as exc:
                raise exc.at_index(index) from None
        ids = await self._page_repository.resolve(equestrian_id=equestrian_id, values=list(dict.fromkeys(normalized)))
        return SeoResolveOutDto(
            items=[
                SeoResolveItemOutDto(source=source, normalized=value, id=ids[value])
                for source, value in zip(data.items, normalized, strict=True)
            ]
        )

    async def list_filtered(
        self,
        *,
        equestrian_id: UUID,
        search: str | None = None,
        ordering: str | None = None,
        mode: str = "day",
        metric: str = "visits",
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> PageListOutDto:
        try:
            query = PageListInDto(
                search=search,
                ordering=ordering,
                mode=mode,
                metric=metric,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                offset=offset,
            )
        except ValidationError:
            raise ClientError() from None
        search, ordering = query.search, query.ordering
        mode, metric = query.mode, query.metric
        date_from, date_to = query.date_from, query.date_to
        limit, offset = query.limit, query.offset
        MetricRegistry().get(metric)
        if not 1 <= query.limit <= 100 or query.offset < 0:
            raise ClientError()
        metric_key = None
        if ordering not in (None, "page", "-page", "created_at", "-created_at"):
            match = re.fullmatch(r"-?metric:([0-9]{4}-[0-9]{2}-[0-9]{2})", ordering)
            if match is None:
                raise ClientError()
            metric_key = match.group(1)
            try:
                date.fromisoformat(metric_key)
            except ValueError:
                raise ClientError() from None
        # Resolve-only callers do not need the metrics dependencies.
        if self._metrics_period_resolver is None:
            raise RuntimeError("Metrics period resolver is not configured")
        if self._metrics_table_builder is None or self._ya_metrica_client is None:
            raise RuntimeError("Metrics dependencies are not configured")
        period = self._metrics_period_resolver.resolve(mode=mode, date_from=date_from, date_to=date_to)
        ranked_page_ids = None
        sql_ordering = cast(
            Literal["page", "-page", "created_at", "-created_at"] | None,
            ordering if metric_key is None else None,
        )
        if metric_key is not None:
            sql_ordering = None
            column = next((column for column in period.columns if column.key == metric_key), None)
            if column is None:
                ordering = None
            else:
                ranking = await self._ya_metrica_client.ranking(
                    equestrian_id=equestrian_id,
                    metric=metric,
                    date_from=column.date_from,
                    date_to=column.date_to,
                    direction="desc" if ordering and ordering.startswith("-") else "asc",
                )
                ranked_page_ids = ranking.page_ids
        search = search.strip() or None if search is not None else None
        entities = await self._page_repository.get_filtered(
            equestrian_id=equestrian_id,
            search=search,
            ordering=sql_ordering,
            limit=limit,
            offset=offset,
            ranked_page_ids=ranked_page_ids,
        )
        total = await self._page_repository.count_filtered(equestrian_id=equestrian_id, search=search)
        page_ids = [entity.id for entity in entities]
        stats = None
        if page_ids:
            stats = await self._ya_metrica_client.table(
                equestrian_id=equestrian_id,
                metric=metric,
                columns=period.columns,
                page_ids=page_ids,
            )
        return PageListOutDto(
            items=[PageOutDto.model_validate(entity) for entity in entities],
            total=total,
            metrics_table=self._metrics_table_builder.build(
                period=period,
                metric=metric,
                page_ids=page_ids,
                stats=stats,
                ordering=ordering,
            ),
        )
