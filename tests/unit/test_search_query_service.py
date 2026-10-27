from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.exceptions import ClientError
from core.schemas.api import SeoResolveInDto
from core.services import SearchQueryNormalizer, SearchQueryService


async def test_resolve_preserves_order_deduplicates_and_is_idempotent() -> None:
    tenant, horse_id, stable_id = uuid4(), uuid4(), uuid4()
    repository = AsyncMock()
    repository.resolve.return_value = {"Лошадь": horse_id, "Денник": stable_id}
    service = SearchQueryService(repository, SearchQueryNormalizer())
    data = SeoResolveInDto(items=["лошадь", "ДЕННИК", "ЛОШАДЬ", " лошадь "])
    first = await service.resolve(equestrian_id=tenant, data=data)
    second = await service.resolve(equestrian_id=tenant, data=data)
    assert first == second
    assert [item.source for item in first.items] == data.items
    assert [item.id for item in first.items] == [horse_id, stable_id, horse_id, horse_id]
    assert [item.normalized for item in first.items] == ["Лошадь", "Денник", "Лошадь", "Лошадь"]
    assert repository.resolve.await_count == 2
    repository.resolve.assert_awaited_with(equestrian_id=tenant, values=["Лошадь", "Денник"])


async def test_invalid_middle_item_does_not_write_or_expose_source() -> None:
    repository = AsyncMock()
    service = SearchQueryService(repository, SearchQueryNormalizer())
    with pytest.raises(ClientError, match=r"items\[1\]") as error:
        await service.resolve(equestrian_id=uuid4(), data=SeoResolveInDto(items=["лошадь", " \t ", "денник"]))
    assert str(error.value) == "Некорректное значение элемента items[1]"
    repository.resolve.assert_not_awaited()


async def test_resolve_max_batch() -> None:
    repository = AsyncMock()
    values = [f"Запрос {index}" for index in range(1000)]
    repository.resolve.return_value = {value: uuid4() for value in values}
    result = await SearchQueryService(repository, SearchQueryNormalizer()).resolve(
        equestrian_id=uuid4(), data=SeoResolveInDto(items=values)
    )
    assert len(result.items) == 1000
    assert len(repository.resolve.call_args.kwargs["values"]) == 1000


def make_listing_service(repository=None, client=None):
    from datetime import UTC, datetime

    from containers import services_container
    from core.services.metrics_period import MetricsPeriodResolver

    repository = repository if repository is not None else AsyncMock()
    client = client if client is not None else AsyncMock()
    from core.schemas.clients.ya_metrica import SearchQueryStatsTable

    if isinstance(client, AsyncMock):
        client.query_table.return_value = SearchQueryStatsTable(metric="visits", values=[])
    resolver = MetricsPeriodResolver(clock=lambda: datetime(2026, 10, 26, 12, tzinfo=UTC))
    return (
        SearchQueryService(
            repository,
            SearchQueryNormalizer(),
            ya_metrica_client=client,
            metrics_period_resolver=resolver,
            metrics_table_builder=services_container.metrics_table_builder(),
        ),
        repository,
        client,
    )


@pytest.mark.parametrize("size", [0, 5, 50, 100])
async def test_ut_sq_05_table_once_for_whole_selection(size):
    from core.entities import SearchQuery

    service, repository, client = make_listing_service()
    tenant = uuid4()
    entities = [SearchQuery(equestrian_id=tenant, query=f"query {i}") for i in range(size)]
    repository.get_filtered.return_value = entities
    repository.count_filtered.return_value = 123
    result = await service.list_filtered(equestrian_id=tenant, limit=max(5, size))
    assert result.total == 123 and len(result.items) == size
    assert len(result.metrics_table.columns) == 7
    client.query_ranking.assert_not_awaited()
    if size:
        client.query_table.assert_awaited_once()
        args = client.query_table.call_args.kwargs
        assert args["equestrian_id"] == tenant and args["metric"] == "visits"
        assert args["search_query_ids"] == [item.id for item in entities]
        assert [c.key for c in args["columns"]] == [f"2026-10-{i}" for i in range(20, 27)]
    else:
        client.query_table.assert_not_awaited()
        assert all(c.values == {} for c in result.metrics_table.columns)
    repository.get_filtered.assert_awaited_once()
    repository.count_filtered.assert_awaited_once()


@pytest.mark.parametrize("ordering,direction", [("metric:2026-10-26", "asc"), ("-metric:2026-10-26", "desc")])
async def test_ut_sq_05_ranking_sql_table_sequence(ordering, direction):
    from datetime import date
    from unittest.mock import Mock

    from core.entities import SearchQuery
    from core.schemas.clients.ya_metrica import SearchQueryStatsRanking

    service, repository, client = make_listing_service()
    tenant, search_query_id = uuid4(), uuid4()
    repository.get_filtered.return_value = [SearchQuery(id=search_query_id, equestrian_id=tenant, query="a")]
    repository.count_filtered.return_value = 1
    client.query_ranking.return_value = SearchQueryStatsRanking(
        metric="visits", direction=direction, search_query_ids=[search_query_id]
    )
    calls = Mock()
    calls.attach_mock(client.query_ranking, "ranking")
    calls.attach_mock(repository.get_filtered, "sql")
    calls.attach_mock(repository.count_filtered, "count")
    calls.attach_mock(client.query_table, "table")
    result = await service.list_filtered(equestrian_id=tenant, mode="week", ordering=ordering, search=" a ")
    client.query_ranking.assert_awaited_once_with(
        equestrian_id=tenant,
        metric="visits",
        date_from=date(2026, 10, 26),
        date_to=date(2026, 10, 26),
        direction=direction,
    )
    repository.get_filtered.assert_awaited_once_with(
        equestrian_id=tenant,
        search="a",
        ordering=None,
        limit=20,
        offset=0,
        ranked_search_query_ids=[search_query_id],
    )
    assert [call[0] for call in calls.mock_calls] == ["ranking", "sql", "count", "table"]
    assert result.metrics_table.ordering == ordering


async def test_ut_sq_05_missing_column_falls_back_without_ranking():
    service, repository, client = make_listing_service()
    repository.get_filtered.return_value = []
    repository.count_filtered.return_value = 0
    result = await service.list_filtered(equestrian_id=uuid4(), ordering="metric:2026-09-01")
    assert result.metrics_table.ordering is None
    assert repository.get_filtered.call_args.kwargs["ordering"] is None
    assert repository.get_filtered.call_args.kwargs["ranked_search_query_ids"] is None
    client.query_ranking.assert_not_awaited()
    client.query_table.assert_not_awaited()


@pytest.mark.parametrize(
    "params",
    [
        {"ordering": "page"},
        {"ordering": "-page"},
        {"ordering": "query,created_at"},
        {"ordering": "metric:2026-13-40"},
        {"ordering": "metric:20261020"},
        {"mode": "quarter"},
        {"metric": "secret"},
        {"date_from": "2026-10-20"},
        {"date_from": "2026-10-22", "date_to": "2026-10-20"},
        {"date_to": "not-a-date"},
        {"limit": "abc"},
        {"offset": "abc"},
    ],
)
async def test_ut_sq_01_invalid_before_io(params):
    service, repository, peer = make_listing_service()
    with pytest.raises(ClientError):
        await service.list_filtered(equestrian_id=uuid4(), **params)
    assert repository.mock_calls == [] and peer.mock_calls == []


@pytest.mark.parametrize("mode,count", [("day", 7), ("week", 4), ("month", 6), ("year", 2)])
@pytest.mark.parametrize(
    "metric",
    [
        "visits",
        "users",
        "pageviews",
        "bounces",
        "visit_duration_seconds",
        "bounce_rate",
        "page_depth",
        "avg_visit_duration_seconds",
    ],
)
async def test_ut_sq_01_defaults_shared_resolver(mode, count, metric):
    service, repository, peer = make_listing_service()
    repository.get_filtered.return_value = []
    repository.count_filtered.return_value = 0
    result = await service.list_filtered(equestrian_id=uuid4(), mode=mode, metric=metric)
    expected = service._metrics_period_resolver.resolve(mode=mode)
    assert result.metrics_table.period == expected.period
    assert len(result.metrics_table.columns) == count
    assert result.metrics_table.metric.key == metric
    assert peer.mock_calls == []


@pytest.mark.parametrize("ordering", [None, "query", "-query", "created_at", "-created_at"])
async def test_ut_sq_01_clamp_and_plain_ordering(ordering):
    service, repository, peer = make_listing_service()
    repository.get_filtered.return_value = []
    repository.count_filtered.return_value = 123
    result = await service.list_filtered(
        equestrian_id=uuid4(), ordering=ordering, limit=500, offset=-10, date_from="2026-09-01", date_to="2026-10-26"
    )
    assert result.total == 123
    assert result.metrics_table.ordering == ordering
    assert result.metrics_table.period.clamped is True
    assert len(result.metrics_table.columns) == 31
    assert repository.get_filtered.call_args.kwargs["limit"] == 100
    assert repository.get_filtered.call_args.kwargs["offset"] == 0
    assert repository.get_filtered.call_args.kwargs["ordering"] == ordering
    assert peer.mock_calls == []
