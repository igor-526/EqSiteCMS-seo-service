"""UT-PS-12: internal peer write, tenant precedence and safe errors."""

from unittest.mock import AsyncMock, Mock
from uuid import UUID

import httpx
import pytest
from dependency_injector import providers

from clients.yandex_service import YandexServiceClient
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
from providers.ya_metrica import YaMetricaParseProvider
from tests.api.conftest import ITEM_ID, TENANT_ID, RuntimeApi
from tests.api.test_errors import LEAK
from tests.yandex_contract import connected_counter_payload

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


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize("outcome", ["ready", "cleanup_pending", "missing", "invalid", "foreign", "broker_down"])
async def test_real_peer_binding_parse_admission(runtime_api, credentials, outcome):
    """Real HTTP adapter -> provider -> dispatch; only peer I/O and persistence are fake.

    Pending cleanup must not reject a current binding at SEO preflight: M owns the
    authoritative maintenance gate after command publication (202 is not finished).
    """
    payload = connected_counter_payload(tenant=TENANT_ID, present=outcome != "missing")
    if outcome == "ready":
        payload["cleanup_completed_seq"] = payload["cleanup_requested_seq"]
    elif outcome == "invalid":
        payload["cleanup_requested_seq"] = True
    elif outcome == "foreign":
        payload["counter"]["equestrian_id"] = str(ITEM_ID)
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.path == f"/api/connections/metrica/{TENANT_ID}"
        assert not any(h in request.headers for h in ("cookie", "authorization", "x-equestrian-id"))
        return httpx.Response(200, json=payload)

    peer = YandexServiceClient(base_url="http://private-peer", transport=httpx.MockTransport(handler))
    registry = ParseProviderRegistry(enabled_codes=["ya_metrica"])
    registry.register(YaMetricaParseProvider(client=peer))
    publisher, repository = AsyncMock(), AsyncMock()
    if outcome == "broker_down":
        publisher.publish.side_effect = ParseQueueUnavailableError()
    service = ParseDispatchService(
        publisher=publisher, repository=repository, registry=registry, page_size=100, max_attempts=3
    )
    with services_container.parse_dispatch_service.override(providers.Object(service)):
        async with runtime_api.client() as client:
            response = await client.post(
                PATH,
                headers={"X-Equestrian-Id": str(TENANT_ID), **credentials},
                json={"provider": "ya_metrica"},
            )
    assert len(requests) == 1
    if outcome in ("ready", "cleanup_pending"):
        assert response.status_code == 202
        assert set(response.json()) == {"job_id", "provider", "status"}
        assert response.json()["status"] == "accepted"
        command = publisher.publish.await_args.kwargs["command"]
        assert command.equestrian_id == TENANT_ID
        assert str(command.job_id) == response.json()["job_id"]
        publisher.publish.assert_awaited_once()
        repository.record_manual.assert_awaited_once()
        assert runtime_api.events == ["open", "begin", "commit", "close"]
    else:
        error = {
            "missing": ParseTargetMissingError,
            "invalid": ParseTargetsUnavailableError,
            "foreign": ParseTargetsUnavailableError,
            "broker_down": ParseQueueUnavailableError,
        }[outcome]
        assert response.status_code == error.status_code
        assert response.json() == {"detail": error.message}
        repository.record_manual.assert_not_awaited()
        if outcome != "broker_down":
            publisher.publish.assert_not_awaited()
