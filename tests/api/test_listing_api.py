"""AT-SP-03/05: pages metrics contract and unchanged queries."""

import pytest
from dependency_injector import providers

from containers import services_container
from tests.api.conftest import ITEM_ID, TENANT_ID, RuntimeApi
from tests.unit.test_page_service import make_listing_service


def empty_metrics_table():
    service, repository, client = make_listing_service()
    return service._metrics_table_builder.build(
        period=service._metrics_period_resolver.resolve(), metric="visits", page_ids=[]
    ).model_dump(mode="json")


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
                "page_hash": "private",
            }
        ],
        "total": 7,
    }
    if path == "pages":
        service.list_filtered.return_value["metrics_table"] = empty_metrics_table()
    params = (
        {} if ordering is None else {"ordering": ordering + field, "search": "ЛОШАДЬ", "limit": "5", "offset": "2"}
    )
    async with runtime_api.client() as client:
        response = await client.get(f"/api/{path}", headers={"X-Equestrian-Id": str(TENANT_ID)}, params=params)
    assert response.status_code == 200, response.text
    assert set(response.json()) == ({"items", "total", "metrics_table"} if path == "pages" else {"items", "total"})
    assert response.json()["total"] == 7
    assert set(response.json()["items"][0]) == {"id", field, "created_at"}
    extra = {"mode": "day", "metric": "visits", "date_from": None, "date_to": None} if path == "pages" else {}
    service.list_filtered.assert_awaited_once_with(
        equestrian_id=TENANT_ID,
        search=None if ordering is None else "ЛОШАДЬ",
        ordering=None if ordering is None else ordering + field,
        limit=20 if ordering is None else 5,
        offset=0 if ordering is None else 2,
        **extra,
    )
    assert runtime_api.events == ["open", "begin", "commit", "close"]


@pytest.mark.parametrize(
    "params",
    [
        {"ordering": "created_at"},
        {"offset": "abc"},
        {"limit": "abc"},
        {"ordering": "id"},
    ],
)
async def test_queries_invalid_parameters_unchanged(runtime_api, params):
    async with runtime_api.client() as client:
        response = await client.get("/api/search-queries", headers={"X-Equestrian-Id": str(TENANT_ID)}, params=params)
    assert response.status_code == 400
    runtime_api.assert_no_calls()


@pytest.mark.parametrize(
    "params",
    [
        {"mode": "quarter"},
        {"metric": "private"},
        {"date_from": "2026-10-20"},
        {"date_to": "2026-10-20"},
        {"date_from": "2026-10-22", "date_to": "2026-10-20"},
        {"date_from": "broken", "date_to": "broken"},
        {"ordering": "metric:2026-13-40"},
        {"ordering": "id"},
        {"limit": "abc"},
        {"offset": "abc"},
        {"limit": 0},
        {"limit": 101},
        {"offset": -1},
    ],
)
async def test_at_sp_03_pages_validate_before_io(runtime_api, params):
    service, repository, peer = make_listing_service()
    with services_container.page_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get("/api/pages", headers={"X-Equestrian-Id": str(TENANT_ID)}, params=params)
    assert response.status_code == 400, response.text
    assert peer.mock_calls == [] and repository.mock_calls == []
    assert runtime_api.events == ["open", "begin", "rollback", "close"]


async def test_at_sp_05_real_service_serialization(runtime_api):
    from core.entities import Page

    service, repository, peer = make_listing_service()
    repository.get_filtered.return_value = [Page(id=ITEM_ID, equestrian_id=TENANT_ID, page="/a")]
    repository.count_filtered.return_value = 1
    with services_container.page_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get("/api/pages", headers={"X-Equestrian-Id": str(TENANT_ID)})
    assert response.status_code == 200, response.text
    data = response.json()
    assert set(data) == {"items", "total", "metrics_table"}
    assert set(data["items"][0]) == {"id", "page", "created_at"}
    assert data["metrics_table"]["period"]["defaulted"] is True
    assert len(data["metrics_table"]["columns"]) == 7
    assert data["metrics_table"]["metric"]["key"] == "visits"
    peer.table.assert_awaited_once()
