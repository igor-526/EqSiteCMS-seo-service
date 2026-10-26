"""UT-SEO-17: HTTP → real services/normalizers, repositories are isolated mocks."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.services import PageNormalizer, PageService, SearchQueryNormalizer, SearchQueryService
from tests.api.conftest import TENANT_ID, RuntimeApi


@pytest.fixture(params=["search-queries", "pages"])
def resolve_case(request, runtime_api: RuntimeApi):
    pages = request.param == "pages"
    repository = AsyncMock()
    params, hashes = AsyncMock(), AsyncMock()
    params.get_values.return_value = frozenset({"utm_source"})
    hashes.get_values.return_value = frozenset({"top"})
    service = (
        PageService(repository, PageNormalizer(), params, hashes)
        if pages
        else SearchQueryService(repository, SearchQueryNormalizer())
    )
    runtime_api.services["page_service" if pages else "search_query_service"].resolve.side_effect = service.resolve
    return request.param, repository, pages


async def test_resolve_order_duplicates_and_public_fields(runtime_api: RuntimeApi, resolve_case) -> None:
    path, repository, pages = resolve_case
    sources = ["https://site.ru/a?utm_source=ya#top", "/b", "/a"] if pages else [" ЛОШАДЬ ", "денник", "лошадь"]
    normalized = ["/a", "/b", "/a"] if pages else ["Лошадь", "Денник", "Лошадь"]
    ids = {value: uuid4() for value in normalized}
    repository.resolve.return_value = ids
    async with runtime_api.client() as client:
        for _ in range(2):
            response = await client.post(
                f"/api/{path}/resolve", headers={"X-Equestrian-Id": str(TENANT_ID)}, json={"items": sources}
            )
            assert response.status_code == 200
            assert response.json() == {
                "items": [
                    {"source": source, "normalized": value, "id": str(ids[value])}
                    for source, value in zip(sources, normalized, strict=True)
                ]
            }
    assert repository.resolve.await_args.kwargs == {
        "equestrian_id": TENANT_ID,
        "values": list(dict.fromkeys(normalized)),
    }
    assert runtime_api.events == ["open", "begin", "commit", "close"] * 2


@pytest.mark.parametrize("items", [[], ["/a"] * 1001, ["/a", 123], ["/a", None], ["/a", "   "]])
async def test_invalid_batch_rolls_back_without_write(runtime_api: RuntimeApi, resolve_case, items) -> None:
    path, repository, _ = resolve_case
    async with runtime_api.client() as client:
        response = await client.post(
            f"/api/{path}/resolve", headers={"X-Equestrian-Id": str(TENANT_ID)}, json={"items": items}
        )
    assert response.status_code == 400
    assert set(response.json()) == {"detail"}
    repository.resolve.assert_not_awaited()
    assert runtime_api.events == ["open", "begin", "rollback", "close"]


async def test_maximum_batch(runtime_api: RuntimeApi, resolve_case) -> None:
    path, repository, pages = resolve_case
    sources = [f"/a/{index}" if pages else f"Запрос {index}" for index in range(1000)]
    ids = {value: uuid4() for value in sources}
    repository.resolve.return_value = ids
    async with runtime_api.client() as client:
        response = await client.post(
            f"/api/{path}/resolve", headers={"X-Equestrian-Id": str(TENANT_ID)}, json={"items": sources}
        )
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["items"]] == [str(ids[value]) for value in sources]
