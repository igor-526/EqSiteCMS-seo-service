from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.exceptions import ClientError
from core.schemas.api import SeoResolveInDto
from core.services import PageNormalizer, PageService


async def test_resolve_loads_both_tenant_blacklists_once_and_preserves_order() -> None:
    tenant, first_id, second_id = uuid4(), uuid4(), uuid4()
    pages, params, hashes = AsyncMock(), AsyncMock(), AsyncMock()
    params.get_values.return_value = frozenset({"utm_source"})
    hashes.get_values.return_value = frozenset({"top"})
    pages.resolve.return_value = {"/a": first_id, "/b#keep": second_id}
    service = PageService(pages, PageNormalizer(), params, hashes)
    data = SeoResolveInDto(items=["https://site.ru/a?utm_source=ya#top", "/b#keep", "/a"])
    result = await service.resolve(equestrian_id=tenant, data=data)
    params.get_values.assert_awaited_once_with(equestrian_id=tenant)
    hashes.get_values.assert_awaited_once_with(equestrian_id=tenant)
    pages.resolve.assert_awaited_once_with(equestrian_id=tenant, values=["/a", "/b#keep"])
    assert [item.source for item in result.items] == data.items
    assert [item.normalized for item in result.items] == ["/a", "/b#keep", "/a"]
    assert [item.id for item in result.items] == [first_id, second_id, first_id]


async def test_invalid_middle_page_never_writes() -> None:
    pages, params, hashes = AsyncMock(), AsyncMock(), AsyncMock()
    params.get_values.return_value = hashes.get_values.return_value = frozenset()
    with pytest.raises(ClientError) as error:
        await PageService(pages, PageNormalizer(), params, hashes).resolve(
            equestrian_id=uuid4(), data=SeoResolveInDto(items=["/a", "ftp://private.invalid/secret", "/b"])
        )
    assert str(error.value) == "Некорректное значение элемента items[1]"
    pages.resolve.assert_not_awaited()


async def test_blacklists_are_reloaded_for_each_tenant_batch() -> None:
    pages, params, hashes = AsyncMock(), AsyncMock(), AsyncMock()
    params.get_values.side_effect = [frozenset({"track"}), frozenset()]
    hashes.get_values.return_value = frozenset()
    pages.resolve.return_value = {"/a": uuid4(), "/a?track=x": uuid4()}
    service = PageService(pages, PageNormalizer(), params, hashes)
    first_tenant, second_tenant = uuid4(), uuid4()
    data = SeoResolveInDto(items=["/a?track=x"])
    first = await service.resolve(equestrian_id=first_tenant, data=data)
    second = await service.resolve(equestrian_id=second_tenant, data=data)
    assert first.items[0].normalized == "/a"
    assert second.items[0].normalized == "/a?track=x"
    assert params.get_values.await_count == hashes.get_values.await_count == 2
    assert params.get_values.await_args_list[0].kwargs == {"equestrian_id": first_tenant}
    assert params.get_values.await_args_list[1].kwargs == {"equestrian_id": second_tenant}


async def test_max_page_batch() -> None:
    pages, params, hashes = AsyncMock(), AsyncMock(), AsyncMock()
    params.get_values.return_value = hashes.get_values.return_value = frozenset()
    values = [f"/page/{index}" for index in range(1000)]
    pages.resolve.return_value = {value: uuid4() for value in values}
    result = await PageService(pages, PageNormalizer(), params, hashes).resolve(
        equestrian_id=uuid4(), data=SeoResolveInDto(items=values)
    )
    assert len(result.items) == 1000
    assert params.get_values.await_count == hashes.get_values.await_count == 1


def make_listing_service(repository=None, client=None):
    from datetime import UTC, datetime

    from containers import services_container
    from core.services.metrics_period import MetricsPeriodResolver

    repository = repository if repository is not None else AsyncMock()
    client = client if client is not None else AsyncMock()
    from core.schemas.clients.ya_metrica import PageStatsTable

    client.table.return_value = PageStatsTable(metric="visits", values=[])
    resolver = MetricsPeriodResolver(clock=lambda: datetime(2026, 10, 26, 12, tzinfo=UTC))
    return (
        PageService(
            repository,
            PageNormalizer(),
            AsyncMock(),
            AsyncMock(),
            ya_metrica_client=client,
            metrics_period_resolver=resolver,
            metrics_table_builder=services_container.metrics_table_builder(),
        ),
        repository,
        client,
    )


@pytest.mark.parametrize("size", [0, 1, 50])
async def test_ut_sp_11_table_once_for_whole_selection(size):
    from core.entities import Page

    service, repository, client = make_listing_service()
    tenant = uuid4()
    entities = [Page(equestrian_id=tenant, page=f"/{i}") for i in range(size)]
    repository.get_filtered.return_value = entities
    repository.count_filtered.return_value = 123
    result = await service.list_filtered(equestrian_id=tenant, limit=50)
    assert result.total == 123 and len(result.items) == size
    assert len(result.metrics_table.columns) == 7
    client.ranking.assert_not_awaited()
    if size:
        client.table.assert_awaited_once()
        args = client.table.call_args.kwargs
        assert args["equestrian_id"] == tenant and args["metric"] == "visits"
        assert args["page_ids"] == [item.id for item in entities]
        assert [c.key for c in args["columns"]] == [f"2026-10-{i}" for i in range(20, 27)]
    else:
        client.table.assert_not_awaited()
        assert all(c.values == {} for c in result.metrics_table.columns)
    repository.get_filtered.assert_awaited_once()
    repository.count_filtered.assert_awaited_once()


@pytest.mark.parametrize("ordering,direction", [("metric:2026-10-26", "asc"), ("-metric:2026-10-26", "desc")])
async def test_ut_sp_12_ranking_sql_table_sequence(ordering, direction):
    from datetime import date
    from unittest.mock import Mock

    from core.entities import Page
    from core.schemas.clients.ya_metrica import PageStatsRanking

    service, repository, client = make_listing_service()
    tenant, page_id = uuid4(), uuid4()
    repository.get_filtered.return_value = [Page(id=page_id, equestrian_id=tenant, page="/a")]
    repository.count_filtered.return_value = 1
    client.ranking.return_value = PageStatsRanking(metric="visits", direction=direction, page_ids=[page_id])
    calls = Mock()
    calls.attach_mock(client.ranking, "ranking")
    calls.attach_mock(repository.get_filtered, "sql")
    calls.attach_mock(repository.count_filtered, "count")
    calls.attach_mock(client.table, "table")
    result = await service.list_filtered(equestrian_id=tenant, mode="week", ordering=ordering, search=" a ")
    client.ranking.assert_awaited_once_with(
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
        ranked_page_ids=[page_id],
    )
    assert [call[0] for call in calls.mock_calls] == ["ranking", "sql", "count", "table"]
    assert result.metrics_table.ordering == ordering


async def test_ut_sp_12_missing_column_falls_back_without_ranking():
    service, repository, client = make_listing_service()
    repository.get_filtered.return_value = []
    repository.count_filtered.return_value = 0
    result = await service.list_filtered(equestrian_id=uuid4(), ordering="metric:2026-09-01")
    assert result.metrics_table.ordering is None
    assert repository.get_filtered.call_args.kwargs["ordering"] is None
    assert repository.get_filtered.call_args.kwargs["ranked_page_ids"] is None
    client.ranking.assert_not_awaited()
    client.table.assert_not_awaited()
