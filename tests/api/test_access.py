import pytest

from core.exceptions import UnauthorizedError

from .conftest import API_ROUTES, ITEM_ID, TENANT_ID, RuntimeApi


def request_path(path: str) -> str:
    return path.replace("{item_id}", str(ITEM_ID))


def request_body(path: str) -> dict:
    return {"items": ["horse"]} if path.endswith("/resolve") else {"value": "top"}


@pytest.mark.parametrize(("method", "path"), API_ROUTES)
@pytest.mark.parametrize("credentials", [{}, {"Authorization": "Bearer ignored", "Cookie": "session=ignored"}])
async def test_credentials_do_not_affect_access(runtime_api: RuntimeApi, method: str, path: str, credentials: dict):
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
