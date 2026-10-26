from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.entities import Page, SearchQuery
from core.services import PageNormalizer, PageService, SearchQueryNormalizer, SearchQueryService


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
    tenant = uuid4()
    repository = AsyncMock()
    service: SearchQueryService | PageService
    if kind == "query":
        service = SearchQueryService(repository, SearchQueryNormalizer())
        repository.get_filtered.return_value = [SearchQuery(equestrian_id=tenant, query="Лошадь")]
    else:
        service = PageService(repository, PageNormalizer(), AsyncMock(), AsyncMock())
        repository.get_filtered.return_value = [Page(equestrian_id=tenant, page="/a")]
    repository.count_filtered.return_value = 42
    result = await service.list_filtered(equestrian_id=tenant, search=search, limit=limit, offset=offset)
    assert result.total == 42 and len(result.items) == 1
    assert set(result.items[0].model_dump()) == {"id", kind, "created_at"}
    repository.get_filtered.assert_awaited_once_with(
        equestrian_id=tenant, search=expected_search, ordering=None, limit=expected_limit, offset=expected_offset
    )
    repository.count_filtered.assert_awaited_once_with(equestrian_id=tenant, search=expected_search)


@pytest.mark.parametrize("descending", [False, True])
async def test_listing_passes_typed_ordering_and_empty_result(descending: bool) -> None:
    tenant = uuid4()
    queries, pages = AsyncMock(), AsyncMock()
    for repository in (queries, pages):
        repository.get_filtered.return_value = []
        repository.count_filtered.return_value = 0
    query_result = await SearchQueryService(queries, SearchQueryNormalizer()).list_filtered(
        equestrian_id=tenant, ordering="-query" if descending else "query"
    )
    page_result = await PageService(pages, PageNormalizer(), AsyncMock(), AsyncMock()).list_filtered(
        equestrian_id=tenant, ordering="-page" if descending else "page"
    )
    assert query_result.items == page_result.items == []
    assert query_result.total == page_result.total == 0
    assert queries.get_filtered.call_args.kwargs["ordering"] == ("-query" if descending else "query")
    assert pages.get_filtered.call_args.kwargs["ordering"] == ("-page" if descending else "page")
