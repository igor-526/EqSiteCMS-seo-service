"""UT-PS-12: internal peer write, tenant precedence and safe errors."""

from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from dependency_injector import providers

from containers import services_container
from core.exceptions import (
    ParseProviderUnknownError,
    ParseQueueUnavailableError,
    ParseTargetMissingError,
    ParseTargetsUnavailableError,
    UnauthorizedError,
)
from core.services.parsing import ParseDispatchService
from core.services.parsing.registry import ParseProviderRegistry
from tests.api.conftest import ITEM_ID, TENANT_ID, RuntimeApi
from tests.api.test_errors import LEAK

PATH = "/api/parse-jobs"
CREDENTIALS = [{}, {"Cookie": "access_token=ignored", "Authorization": "Bearer ignored"}]


@pytest.mark.parametrize("credentials", CREDENTIALS)
async def test_accepted_schema_and_tenant(runtime_api: RuntimeApi, credentials: dict) -> None:
    async with runtime_api.client() as client:
        first = await client.post(
            PATH + f"?equestrian_id={ITEM_ID}",
            headers={"X-Equestrian-Id": str(TENANT_ID), **credentials},
            json={"provider": "ya_metrica"},
        )
        second = await client.post(
            PATH, headers={"X-Equestrian-Id": str(TENANT_ID), **credentials}, json={"provider": "ya_metrica"}
        )
    assert first.status_code == second.status_code == 202
    data = first.json()
    assert set(data) == {"job_id", "provider", "status"}
    assert UUID(data["job_id"]).version == 4
    assert data["provider"] == "ya_metrica" and data["status"] == "accepted"
    assert data["job_id"] != second.json()["job_id"]
    service = runtime_api.services["parse_dispatch_service"]
    assert service.dispatch_manual.await_count == 2
    service.dispatch_manual.assert_awaited_with(provider="ya_metrica", equestrian_id=TENANT_ID)


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize("selector", [None, "", "   ", "not-a-uuid"])
@pytest.mark.parametrize("body", [b'{"provider":"ya_metrica"}', b'{"broken":', b"not json"])
async def test_selector_precedes_body_and_dependencies(runtime_api, credentials, selector, body):
    headers = {"Content-Type": "application/json", **credentials}
    if selector is not None:
        headers["X-Equestrian-Id"] = selector
    async with runtime_api.client() as client:
        response = await client.post(PATH, headers=headers, content=body)
    assert response.status_code == 401
    assert response.json() == {"detail": UnauthorizedError.message}
    assert runtime_api.events == []
    runtime_api.assert_no_calls()


@pytest.mark.parametrize(
    "body",
    [
        b'{"broken":',
        b"null",
        b"[]",
        b"{}",
        b'{"provider":null}',
        b'{"provider":123}',
        b'{"provider":[]}',
        b'{"provider":""}',
        b'{"provider":"a"}',
        b'{"provider":"Ya_metrica"}',
        b'{"provider":"ya-metrica"}',
        b'{"provider":"' + b"a" * 33 + b'"}',
        b'{"provider":"ya_metrica","equestrian_id":"' + str(ITEM_ID).encode() + b'"}',
        ('{"provider":"ya_metrica","' + LEAK + '":"' + LEAK + '"}').encode(),
        ('{"provider":"' + LEAK + '"}').encode(),
    ],
)
async def test_invalid_body_safe_400(runtime_api: RuntimeApi, body: bytes) -> None:
    async with runtime_api.client() as client:
        response = await client.post(
            PATH, headers={"X-Equestrian-Id": str(TENANT_ID), "Content-Type": "application/json"}, content=body
        )
    assert response.status_code == 400
    assert set(response.json()) == {"detail"}
    assert LEAK not in response.text and str(ITEM_ID) not in response.text
    for detail in response.json()["detail"]:
        assert detail["msg"] == "Некорректные параметры запроса"
        assert set(detail) == {"type", "loc", "msg"}
    runtime_api.assert_no_calls()
    assert runtime_api.events == ["open", "begin", "rollback", "close"]


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize(
    "error,status",
    [
        (ParseProviderUnknownError, 400),
        (ParseTargetMissingError, 400),
        (ParseTargetsUnavailableError, 502),
        (ParseQueueUnavailableError, 503),
    ],
)
async def test_constant_domain_errors(runtime_api, caplog, credentials, error, status):
    runtime_api.services["parse_dispatch_service"].dispatch_manual.side_effect = error(LEAK)
    async with runtime_api.client() as client:
        response = await client.post(
            PATH, headers={"X-Equestrian-Id": str(TENANT_ID), **credentials}, json={"provider": "ya_metrica"}
        )
    assert response.status_code == status
    assert response.json() == {"detail": error.message}
    assert LEAK not in response.text and LEAK not in caplog.text
    assert runtime_api.events == ["open", "begin", "rollback", "close"]


@pytest.mark.parametrize("enabled", [True, False])
async def test_registry_unknown_or_disabled(runtime_api, enabled):
    registry = ParseProviderRegistry(enabled_codes=["ya_metrica"] if enabled else [])
    provider = Mock(code="ya_metrica", subject="commands.parse.ya_metrica.run")
    registry.register(provider)
    publisher, repository = AsyncMock(), AsyncMock()
    service = ParseDispatchService(
        publisher=publisher, repository=repository, registry=registry, page_size=100, max_attempts=3
    )
    with services_container.parse_dispatch_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.post(
                PATH,
                headers={"X-Equestrian-Id": str(TENANT_ID)},
                json={"provider": "unknown" if enabled else "ya_metrica"},
            )
    assert response.status_code == 400
    assert response.json() == {"detail": ParseProviderUnknownError.message}
    assert publisher.mock_calls == repository.mock_calls == []
