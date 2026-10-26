"""UT-SEO-19: both blacklist HTTP contracts and tenant-scoped failures."""

from unittest.mock import AsyncMock

import pytest

from core.exceptions import AlreadyExistsError, NotFoundError
from core.services import BlacklistedHashService, BlacklistedQueryParamService
from tests.api.conftest import ITEM, ITEM_ID, TENANT_ID, RuntimeApi


@pytest.fixture(
    params=[
        ("blacklisted-query-params", "blacklisted_query_param_service"),
        ("blacklisted-hashes", "blacklisted_hash_service"),
    ]
)
def blacklist_case(request):
    return request.param


async def test_crud_status_payload_and_arguments(runtime_api: RuntimeApi, blacklist_case) -> None:
    resource, name = blacklist_case
    path = f"/api/page-normalization/{resource}"
    service = runtime_api.services[name]
    async with runtime_api.client() as client:
        client.headers["X-Equestrian-Id"] = str(TENANT_ID)
        created = await client.post(path, json={"value": "top"})
        listed = await client.get(path)
        updated = await client.patch(f"{path}/{ITEM_ID}", json={"value": "top"})
        deleted = await client.delete(f"{path}/{ITEM_ID}")
    assert created.status_code == 201 and created.json() == ITEM
    assert listed.status_code == 200 and listed.json() == {"items": [ITEM], "total": 1}
    assert updated.status_code == 200 and updated.json() == ITEM
    assert deleted.status_code == 204 and deleted.content == b""
    service.list_all.assert_awaited_once_with(equestrian_id=TENANT_ID)
    service.delete.assert_awaited_once_with(equestrian_id=TENANT_ID, item_id=ITEM_ID)
    for method in (service.create, service.update):
        assert method.await_args.kwargs["equestrian_id"] == TENANT_ID
        assert method.await_args.kwargs["data"].value == "top"
    assert service.update.await_args.kwargs["item_id"] == ITEM_ID
    assert runtime_api.events == ["open", "begin", "commit", "close"] * 4


@pytest.mark.parametrize("method", ["PATCH", "DELETE"])
async def test_invalid_item_id(runtime_api: RuntimeApi, blacklist_case, method) -> None:
    resource, _ = blacklist_case
    async with runtime_api.client() as client:
        response = await client.request(
            method,
            f"/api/page-normalization/{resource}/not-uuid",
            headers={"X-Equestrian-Id": str(TENANT_ID)},
            json={"value": "top"} if method == "PATCH" else None,
        )
    assert response.status_code == 400
    runtime_api.assert_no_calls()


@pytest.mark.parametrize(
    "method,error",
    [("POST", AlreadyExistsError), ("PATCH", AlreadyExistsError), ("PATCH", NotFoundError), ("DELETE", NotFoundError)],
)
async def test_domain_errors(runtime_api: RuntimeApi, blacklist_case, method, error) -> None:
    resource, name = blacklist_case
    operation = {"POST": "create", "PATCH": "update", "DELETE": "delete"}[method]
    getattr(runtime_api.services[name], operation).side_effect = error()
    path = f"/api/page-normalization/{resource}" + ("" if method == "POST" else f"/{ITEM_ID}")
    async with runtime_api.client() as client:
        response = await client.request(
            method,
            path,
            headers={"X-Equestrian-Id": str(TENANT_ID)},
            json={"value": "top"} if method != "DELETE" else None,
        )
    assert response.status_code == error.status_code
    assert response.json() == {"detail": error.message}
    assert runtime_api.events == ["open", "begin", "rollback", "close"]


@pytest.mark.parametrize("value", ["", "a b", "#", "a" * 256])
async def test_invalid_value_with_real_service(runtime_api: RuntimeApi, blacklist_case, value) -> None:
    resource, name = blacklist_case
    repository = AsyncMock()
    service = (
        BlacklistedHashService(repository)
        if resource == "blacklisted-hashes"
        else BlacklistedQueryParamService(repository)
    )
    runtime_api.services[name].create.side_effect = service.create
    async with runtime_api.client() as client:
        response = await client.post(
            f"/api/page-normalization/{resource}", headers={"X-Equestrian-Id": str(TENANT_ID)}, json={"value": value}
        )
    assert response.status_code == 400
    assert repository.mock_calls == []


@pytest.mark.parametrize("method", ["PATCH", "DELETE"])
async def test_foreign_or_missing_is_404_before_mutation(runtime_api: RuntimeApi, blacklist_case, method) -> None:
    resource, name = blacklist_case
    repository = AsyncMock()
    repository.get_by_id.return_value = None
    service = (
        BlacklistedHashService(repository)
        if resource == "blacklisted-hashes"
        else BlacklistedQueryParamService(repository)
    )
    operation = "update" if method == "PATCH" else "delete"
    getattr(runtime_api.services[name], operation).side_effect = getattr(service, operation)
    async with runtime_api.client() as client:
        response = await client.request(
            method,
            f"/api/page-normalization/{resource}/{ITEM_ID}",
            headers={"X-Equestrian-Id": str(TENANT_ID)},
            json={"value": "top"} if method == "PATCH" else None,
        )
    assert response.status_code == 404
    repository.get_by_id.assert_awaited_once_with(ITEM_ID, equestrian_id=TENANT_ID)
    repository.update.assert_not_awaited()
    repository.delete.assert_not_awaited()
