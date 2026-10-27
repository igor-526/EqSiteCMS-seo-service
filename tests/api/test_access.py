import pytest

from core.exceptions import UnauthorizedError
from tests.api.test_listing_api import empty_metrics_table

from .conftest import API_ROUTES, ITEM_ID, TENANT_ID, RuntimeApi


def request_path(path: str) -> str:
    return path.replace("{item_id}", str(ITEM_ID))


def request_body(path: str) -> dict:
    return {"items": ["horse"]} if path.endswith("/resolve") else {"value": "top"}


@pytest.mark.parametrize(("method", "path"), API_ROUTES)
@pytest.mark.parametrize("credentials", [{}, {"Authorization": "Bearer ignored", "Cookie": "session=ignored"}])
async def test_credentials_do_not_affect_access(runtime_api: RuntimeApi, method: str, path: str, credentials: dict):
    runtime_api.services["page_service"].list_filtered.return_value["metrics_table"] = empty_metrics_table()
    headers = {"X-Equestrian-Id": str(TENANT_ID), **credentials}
    async with runtime_api.client() as client:
        response = await client.request(method, request_path(path), headers=headers, json=request_body(path))
    expected = 204 if method == "DELETE" else 201 if method == "POST" and not path.endswith("/resolve") else 200
    assert response.status_code == expected, response.text
    calls = [call for service in runtime_api.services.values() for call in service.mock_calls]
    assert len(calls) == 1
    assert calls[0].kwargs["equestrian_id"] == TENANT_ID
    assert runtime_api.events == ["open", "begin", "commit", "close"]
    if method == "DELETE":
        assert response.content == b""


@pytest.mark.parametrize(("method", "path"), API_ROUTES)
@pytest.mark.parametrize("selector", [None, "", "   ", "not-a-uuid"])
@pytest.mark.parametrize("body", [b'{"items": []}', b'{"broken":'])
async def test_tenant_rejected_before_body_and_database(
    runtime_api: RuntimeApi,
    method: str,
    path: str,
    selector: str | None,
    body: bytes,
):
    headers = {"Content-Type": "application/json"}
    if selector is not None:
        headers["X-Equestrian-Id"] = selector
    async with runtime_api.client() as client:
        response = await client.request(method, request_path(path), headers=headers, content=body)
    assert response.status_code == 401
    assert response.json() == {"detail": UnauthorizedError.message}
    runtime_api.assert_no_calls()
    assert runtime_api.events == []


@pytest.mark.parametrize("path", ["/api/pages/resolve", "/api/search-queries/resolve"])
async def test_body_cannot_override_tenant(runtime_api: RuntimeApi, path: str):
    async with runtime_api.client() as client:
        response = await client.post(
            path,
            headers={"X-Equestrian-Id": str(TENANT_ID)},
            json={"items": ["horse"], "equestrian_id": str(ITEM_ID)},
        )
    assert response.status_code == 400
    runtime_api.assert_no_calls()
    assert runtime_api.events == ["open", "begin", "rollback", "close"]


def test_route_inventory():
    assert len(API_ROUTES) == 12
    assert len(set(API_ROUTES)) == 12


@pytest.mark.parametrize("selector", [None, "not-a-uuid"])
async def test_at_sp_02_no_peer_or_database(runtime_api, selector):
    from dependency_injector import providers

    from containers import services_container
    from tests.unit.test_page_service import make_listing_service

    service, repository, peer = make_listing_service()
    headers = {} if selector is None else {"X-Equestrian-Id": selector}
    with services_container.page_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.get("/api/pages?mode=week", headers=headers)
    assert response.status_code == 401
    assert runtime_api.events == [] and repository.mock_calls == [] and peer.mock_calls == []


async def test_at_sp_01_credentials_do_not_change_metrics(runtime_api):
    from dependency_injector import providers

    from containers import services_container
    from core.entities import Page
    from tests.unit.test_page_service import make_listing_service

    service, repository, peer = make_listing_service()
    repository.get_filtered.return_value = [Page(id=ITEM_ID, equestrian_id=TENANT_ID, page="/a")]
    repository.count_filtered.return_value = 1
    with services_container.page_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            anonymous = await client.get("/api/pages", headers={"X-Equestrian-Id": str(TENANT_ID)})
            authenticated = await client.get(
                "/api/pages",
                headers={
                    "X-Equestrian-Id": str(TENANT_ID),
                    "Authorization": "Bearer ignored",
                    "Cookie": "session=ignored",
                },
            )
    assert anonymous.status_code == authenticated.status_code == 200
    assert anonymous.json() == authenticated.json()
    assert "metrics_table" in anonymous.json()
    assert peer.table.await_count == 2
