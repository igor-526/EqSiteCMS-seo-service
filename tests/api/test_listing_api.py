"""UT-SEO-18: list serialization and HTTP parameter validation."""

import pytest

from tests.api.conftest import ITEM_ID, TENANT_ID, RuntimeApi


@pytest.mark.parametrize(
    "path,service_name,field", [("search-queries", "search_query_service", "query"), ("pages", "page_service", "page")]
)
@pytest.mark.parametrize("ordering", [None, "", "-"])
async def test_listing_contract(runtime_api: RuntimeApi, path, service_name, field, ordering) -> None:
    service = runtime_api.services[service_name]
    service.list_filtered.return_value = {
        "items": [
            {
                "id": str(ITEM_ID),
                field: "значение",
                "created_at": "2026-01-01T00:00:00Z",
                "equestrian_id": str(TENANT_ID),
                "query_hash": "private",
                "page_hash": "private",
            }
        ],
        "total": 7,
    }
    params = (
        {} if ordering is None else {"ordering": ordering + field, "search": "ЛОШАДЬ", "limit": "5", "offset": "2"}
    )
    async with runtime_api.client() as client:
        response = await client.get(f"/api/{path}", headers={"X-Equestrian-Id": str(TENANT_ID)}, params=params)
    assert response.status_code == 200
    assert set(response.json()) == {"items", "total"}
    assert response.json()["total"] == 7
    assert set(response.json()["items"][0]) == {"id", field, "created_at"}
    service.list_filtered.assert_awaited_once_with(
        equestrian_id=TENANT_ID,
        search=None if ordering is None else "ЛОШАДЬ",
        ordering=None if ordering is None else ordering + field,
        limit=20 if ordering is None else 5,
        offset=0 if ordering is None else 2,
    )
    assert runtime_api.events == ["open", "begin", "commit", "close"]


@pytest.mark.parametrize("path", ["search-queries", "pages"])
@pytest.mark.parametrize(
    "params", [{"ordering": "created_at"}, {"offset": "abc"}, {"limit": "abc"}, {"ordering": "id"}]
)
async def test_invalid_list_parameters(runtime_api: RuntimeApi, path, params) -> None:
    async with runtime_api.client() as client:
        response = await client.get(f"/api/{path}", headers={"X-Equestrian-Id": str(TENANT_ID)}, params=params)
    assert response.status_code == 400
    runtime_api.assert_no_calls()
    assert runtime_api.events == ["open", "begin", "rollback", "close"]
