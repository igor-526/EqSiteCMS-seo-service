from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.entities import Page, SearchQuery
from core.services import PageService, SearchQueryService
from tests.unit.test_page_service import make_listing_service
from tests.unit.test_search_query_service import make_listing_service as make_query_listing_service


@pytest.mark.parametrize("kind", ["query", "page"])
@pytest.mark.parametrize(
    "limit,offset,expected_limit,expected_offset", [(0, -1, 1, 0), (500, 7, 100, 7), (20, 0, 20, 0)]
)
@pytest.mark.parametrize("search,expected_search", [("  ", None), (None, None), (" ЛОШАДЬ_%\\ ", "ЛОШАДЬ_%\\")])
async def test_listing_clamps_pagination_trims_search_and_keeps_total(
    kind: str,
    limit: int,
    offset: int,
    expected_limit: int,
    expected_offset: int,
    search: str | None,
    expected_search: str | None,
) -> None:
    if kind == "page" and (limit < 1 or limit > 100 or offset < 0):
        from core.exceptions import ClientError

        invalid_service, invalid_repository, invalid_client = make_listing_service()
        with pytest.raises(ClientError):
            await invalid_service.list_filtered(equestrian_id=uuid4(), limit=limit, offset=offset)
        assert invalid_repository.mock_calls == [] and invalid_client.mock_calls == []
        return
    tenant = uuid4()
    repository = AsyncMock()
    service: SearchQueryService | PageService
    if kind == "query":
        service = make_query_listing_service(repository)[0]
        repository.get_filtered.return_value = [SearchQuery(equestrian_id=tenant, query="Лошадь")]
    else:
        service = make_listing_service(repository)[0]
        repository.get_filtered.return_value = [Page(equestrian_id=tenant, page="/a")]
    repository.count_filtered.return_value = 42
    result = await service.list_filtered(equestrian_id=tenant, search=search, limit=limit, offset=offset)
    assert result.total == 42 and len(result.items) == 1
    assert set(result.items[0].model_dump()) == {"id", kind, "created_at"}
    extra = {"ranked_page_ids": None} if kind == "page" else {"ranked_search_query_ids": None}
    repository.get_filtered.assert_awaited_once_with(
        equestrian_id=tenant,
        search=expected_search,
        ordering=None,
        limit=expected_limit,
        offset=expected_offset,
        **extra,
    )
    repository.count_filtered.assert_awaited_once_with(equestrian_id=tenant, search=expected_search)


@pytest.mark.parametrize("descending", [False, True])
async def test_listing_passes_typed_ordering_and_empty_result(descending: bool) -> None:
    tenant = uuid4()
    queries, pages = AsyncMock(), AsyncMock()
    for repository in (queries, pages):
        repository.get_filtered.return_value = []
        repository.count_filtered.return_value = 0
    query_result = await make_query_listing_service(queries)[0].list_filtered(
        equestrian_id=tenant, ordering="-query" if descending else "query"
    )
    page_result = await make_listing_service(pages)[0].list_filtered(
        equestrian_id=tenant, ordering="-page" if descending else "page"
    )
    assert query_result.items == page_result.items == []
    assert query_result.total == page_result.total == 0
    assert queries.get_filtered.call_args.kwargs["ordering"] == ("-query" if descending else "query")
    assert pages.get_filtered.call_args.kwargs["ordering"] == ("-page" if descending else "page")


@pytest.mark.parametrize(
    "params",
    [
        {"ordering": "metric:2026-13-40"},
        {"ordering": "metric:20261020"},
        {"ordering": "id"},
        {"mode": "quarter"},
        {"metric": "secret"},
        {"date_from": "2026-10-20"},
        {"date_from": "2026-10-22", "date_to": "2026-10-20"},
        {"date_to": "not-a-date"},
        {"limit": 0},
        {"limit": 101},
        {"offset": -1},
    ],
)
async def test_ut_sp_13_validation_before_io(params):
    from datetime import date

    from core.exceptions import ClientError

    params = {
        key: date.fromisoformat(value) if key.startswith("date_") and value != "not-a-date" else value
        for key, value in params.items()
    }
    service, repository, client = make_listing_service()
    with pytest.raises(ClientError):
        await service.list_filtered(equestrian_id=uuid4(), **params)
    assert repository.mock_calls == [] and client.mock_calls == []


@pytest.mark.parametrize("ordering", ["created_at", "-created_at"])
async def test_ut_sp_13_created_at_allowed(ordering):
    service, repository, client = make_listing_service()
    repository.get_filtered.return_value = []
    repository.count_filtered.return_value = 0
    result = await service.list_filtered(equestrian_id=uuid4(), ordering=ordering)
    assert repository.get_filtered.call_args.kwargs["ordering"] == ordering
    assert result.metrics_table.ordering == ordering
    client.ranking.assert_not_awaited()
