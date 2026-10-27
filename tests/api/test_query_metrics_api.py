"""UT-SQ-01/03/05/06: real service, HTTP peer adapter and transaction boundary."""

import httpx
import pytest
from dependency_injector import providers

from clients.ya_metrica import YaMetricaClient
from containers import services_container
from core.entities import SearchQuery
from tests.api.conftest import ITEM_ID, TENANT_ID
from tests.unit.test_search_query_service import make_listing_service

CREDENTIALS = [{}, {"Authorization": "Bearer ignored", "Cookie": "session=ignored"}]


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize("selector", [None, "", "   ", "not-a-uuid"])
async def test_ut_sq_06_selector_before_database_peer(runtime_api, credentials, selector):
    service, repository, peer = make_listing_service()
    headers = dict(credentials)
    if selector is not None:
        headers["X-Equestrian-Id"] = selector
    with services_container.search_query_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get("/api/search-queries?mode=broken", headers=headers)
    assert response.status_code == 401
    assert runtime_api.events == [] and repository.mock_calls == [] and peer.mock_calls == []


@pytest.mark.parametrize("credentials", CREDENTIALS)
async def test_ut_sq_06_credentials_ignored_and_current_page_values(runtime_api, credentials):
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.params["equestrian_id"] == str(TENANT_ID)
        assert request.url.params.get_list("search_query_ids") == [str(ITEM_ID)]
        assert "cookie" not in request.headers and "authorization" not in request.headers
        return httpx.Response(
            200,
            json={
                "metric": "visits",
                "values": [
                    {"search_query_id": str(ITEM_ID), "bucket": "2026-10-26", "value": 0},
                    {"search_query_id": str(TENANT_ID), "bucket": "2026-10-26", "value": 99},
                ],
            },
        )

    peer = YaMetricaClient(base_url="http://private-peer", timeout=4, transport=httpx.MockTransport(handler))
    service, repository, _ = make_listing_service(client=peer)
    repository.get_filtered.return_value = [SearchQuery(id=ITEM_ID, equestrian_id=TENANT_ID, query="Лошадь")]
    repository.count_filtered.return_value = 7
    with services_container.search_query_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get(
                "/api/search-queries", headers={"X-Equestrian-Id": str(TENANT_ID), **credentials}
            )
    assert response.status_code == 200, response.text
    data = response.json()
    assert set(data) == {"items", "total", "metrics_table"}
    assert data["total"] == 7 and data["items"][0]["id"] == str(ITEM_ID)
    assert data["metrics_table"]["columns"][-1]["values"] == {str(ITEM_ID): {"text": "0", "value": 0, "hint": None}}
    assert len(requests) == 1
    assert runtime_api.events == ["open", "begin", "commit", "close"]


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
        {"ordering": "page"},
        {"limit": "abc"},
        {"offset": "abc"},
    ],
)
async def test_ut_sq_01_invalid_before_io(runtime_api, params):
    service, repository, peer = make_listing_service()
    with services_container.search_query_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get(
                "/api/search-queries", headers={"X-Equestrian-Id": str(TENANT_ID)}, params=params
            )
    assert response.status_code == 400
    assert repository.mock_calls == [] and peer.mock_calls == []
    assert runtime_api.events == ["open", "begin", "rollback", "close"]


@pytest.mark.parametrize("operation", ["table", "ranking"])
@pytest.mark.parametrize("failure", ["status", "timeout", "transport", "json", "schema", "metric", "direction"])
async def test_ut_sq_03_real_peer_failure_safe_502_rollback(runtime_api, caplog, operation, failure):
    if operation == "table" and failure == "direction":
        pytest.skip("table has no direction field")
    requests = []

    def handler(request):
        requests.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("PRIVATE_SECRET", request=request)
        if failure == "transport":
            raise httpx.ConnectError("PRIVATE_SECRET", request=request)
        if failure == "status":
            return httpx.Response(503, text="PRIVATE_SECRET")
        if failure == "json":
            return httpx.Response(200, text="PRIVATE_SECRET")
        if failure == "schema":
            return httpx.Response(200, json={"private": "PRIVATE_SECRET"})
        payload = {"metric": "users" if failure == "metric" else "visits"}
        if operation == "table":
            payload["values"] = []
        else:
            payload.update(direction="desc" if failure == "direction" else "asc", search_query_ids=[])
        return httpx.Response(200, json=payload)

    peer = YaMetricaClient(base_url="http://PRIVATE_SECRET", timeout=4, transport=httpx.MockTransport(handler))
    service, repository, _ = make_listing_service(client=peer)
    repository.get_filtered.return_value = [SearchQuery(id=ITEM_ID, equestrian_id=TENANT_ID, query="Лошадь")]
    repository.count_filtered.return_value = 1
    params = {"ordering": "metric:2026-10-26"} if operation == "ranking" else {}
    with services_container.search_query_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get(
                "/api/search-queries", headers={"X-Equestrian-Id": str(TENANT_ID)}, params=params
            )
    assert response.status_code == 502, response.text
    assert response.json() == {"detail": "Сервис статистики недоступен"}
    assert "PRIVATE_SECRET" not in response.text + caplog.text
    assert runtime_api.events == ["open", "begin", "rollback", "close"]
    assert len(requests) == 1
    if operation == "ranking":
        assert repository.mock_calls == []


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize("field", ["date_from", "date_to"])
@pytest.mark.parametrize(
    "invalid_date",
    ["0", "86400", "2026-10-20T00:00:00Z", "2026-10-20 00:00:00", "20261020", "2026-02-30"],
)
async def test_sq_fix_1_wire_dates_rejected_before_data(runtime_api, credentials, field, invalid_date):
    service, repository, peer = make_listing_service()
    params = {"date_from": "2026-10-20", "date_to": "2026-10-26", field: invalid_date}
    with services_container.search_query_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get(
                "/api/search-queries",
                headers={"X-Equestrian-Id": str(TENANT_ID), **credentials},
                params=params,
            )
    assert response.status_code == 400, response.text
    assert repository.mock_calls == [] and peer.mock_calls == []
    assert runtime_api.events == ["open", "begin", "rollback", "close"]


@pytest.mark.parametrize("credentials", CREDENTIALS)
async def test_sq_fix_1_valid_paired_dates(runtime_api, credentials):
    service, repository, peer = make_listing_service()
    repository.get_filtered.return_value = [SearchQuery(id=ITEM_ID, equestrian_id=TENANT_ID, query="Лошадь")]
    repository.count_filtered.return_value = 1
    with services_container.search_query_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get(
                "/api/search-queries",
                headers={"X-Equestrian-Id": str(TENANT_ID), **credentials},
                params={"date_from": "2026-10-20", "date_to": "2026-10-26"},
            )
    assert response.status_code == 200, response.text
    period = response.json()["metrics_table"]["period"]
    assert period["requested_date_from"] == period["date_from"] == "2026-10-20"
    assert period["requested_date_to"] == period["date_to"] == "2026-10-26"
    repository.get_filtered.assert_awaited_once()
    repository.count_filtered.assert_awaited_once()
    peer.query_table.assert_awaited_once()
    assert peer.query_table.call_args.kwargs["equestrian_id"] == TENANT_ID
    assert runtime_api.events == ["open", "begin", "commit", "close"]


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize("selector", [None, "", "not-a-uuid"])
async def test_sq_fix_1_bad_dates_do_not_override_selector_rejection(runtime_api, credentials, selector):
    service, repository, peer = make_listing_service()
    headers = dict(credentials)
    if selector is not None:
        headers["X-Equestrian-Id"] = selector
    with services_container.search_query_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get(
                "/api/search-queries", headers=headers, params={"date_from": "0", "date_to": "86400"}
            )
    assert response.status_code == 401
    assert runtime_api.events == [] and repository.mock_calls == [] and peer.mock_calls == []
