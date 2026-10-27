import asyncio
import logging
import traceback
from uuid import uuid4

import httpx
import pytest

from clients.yandex_service import YandexServiceClient
from core.exceptions import ParseTargetsUnavailableError
from core.protocols.clients.yandex_service import YandexServiceClientProtocol

TENANT = uuid4()
ITEM = {"equestrian_id": str(TENANT), "counter_id": "123"}


async def test_contract_without_credentials_or_cookie(caplog, monkeypatch, tmp_path) -> None:
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("HTTP_PROXY", "http://secret-proxy:1")
    monkeypatch.setenv("HTTPS_PROXY", "http://secret-proxy:1")
    netrc = tmp_path / "netrc"
    netrc.write_text("machine secret-peer login secret-user password secret-password\n")
    monkeypatch.setenv("NETRC", str(netrc))
    requests = []
    real_client = httpx.AsyncClient
    client_options = []

    def create_client(**kwargs):
        client_options.append(kwargs)
        return real_client(**kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", create_client)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        for header in ("authorization", "cookie", "x-service-key", "x-equestrian-id"):
            assert header not in request.headers
        assert request.extensions["timeout"] == dict.fromkeys(["connect", "read", "write", "pool"], 1.5)
        if request.url.path.endswith(str(TENANT)):
            assert not request.url.query
            return httpx.Response(200, json={"counter": {**ITEM, "yandex_account": {"secret": "secret-body"}}})
        assert dict(request.url.params) == {"limit": "100", "offset": "0"}
        return httpx.Response(200, json={"total": 1, "items": [ITEM]}, headers={"Set-Cookie": "secret-cookie=1"})

    client: YandexServiceClientProtocol = YandexServiceClient(
        base_url="http://secret-peer", timeout=1.5, transport=httpx.MockTransport(handler)
    )
    assert (await client.list_metrica_bindings(offset=0, limit=100)).items[0].equestrian_id == TENANT
    binding = await client.get_metrica_binding(equestrian_id=TENANT)
    assert binding is not None and binding.model_dump() == {"equestrian_id": TENANT, "counter_id": "123"}
    assert len(requests) == 2
    assert len(client_options) == 2
    assert all(option["trust_env"] is False and option["follow_redirects"] is False for option in client_options)
    assert all(not any(key in option for key in ("headers", "cookies", "auth")) for option in client_options)
    assert "secret" not in caplog.text
    logging.getLogger("httpx").info("unrelated request")
    assert "unrelated request" in caplog.text


@pytest.mark.parametrize(
    "failure",
    [
        "timeout",
        "transport",
        "json",
        "schema",
        "200-empty",
        "bool",
        "negative",
        "extra",
        "uuid",
        "counter",
        "duplicate",
        "empty-page",
        "over-limit",
        "302",
        "204",
        "400",
        "401",
        "404",
        "500",
        "503",
    ],
)
@pytest.mark.parametrize("method", ["list", "target"])
async def test_failures_are_safe(failure: str, method: str, caplog) -> None:
    caplog.set_level(logging.DEBUG)

    def handler(request: httpx.Request) -> httpx.Response:
        if failure == "timeout":
            raise httpx.ReadTimeout("secret-body secret-peer", request=request)
        if failure == "transport":
            raise httpx.ConnectError("secret-body secret-peer", request=request)
        if failure.isdigit():
            return httpx.Response(int(failure), text="secret-body", headers={"Location": "http://secret-redirect"})
        if failure == "json":
            return httpx.Response(200, text="secret-body")
        payload = {
            "schema": {},
            "200-empty": None,
            "bool": {"total": True, "items": []},
            "negative": {"total": -1, "items": []},
            "extra": {"total": 0, "items": [], "secret": "secret-body"},
            "uuid": {"total": 1, "items": [{**ITEM, "equestrian_id": "secret-body"}]},
            "counter": {"total": 1, "items": [{**ITEM, "counter_id": 123}]},
            "duplicate": {"total": 2, "items": [ITEM, ITEM]},
            "empty-page": {"total": 1, "items": []},
            "over-limit": {"total": 3, "items": [ITEM, {**ITEM, "equestrian_id": str(uuid4())}]},
        }[failure]
        return httpx.Response(200, json=payload)

    client = YandexServiceClient(base_url="http://secret-peer", transport=httpx.MockTransport(handler))
    with pytest.raises(ParseTargetsUnavailableError) as caught:
        if method == "list":
            await client.list_metrica_bindings(offset=0, limit=1)
        else:
            await client.get_metrica_binding(equestrian_id=TENANT)
    assert str(caught.value) == ParseTargetsUnavailableError.message
    assert caught.value.status_code == 502
    assert "secret" not in "".join(traceback.format_exception(caught.value))
    assert "secret" not in caplog.text


async def test_absent_binding_and_out_of_range_page() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"counter": None} if request.url.path.endswith(str(TENANT)) else {"total": 0, "items": []}
        )

    client = YandexServiceClient(base_url="http://peer", transport=httpx.MockTransport(handler))
    assert await client.get_metrica_binding(equestrian_id=TENANT) is None
    assert (await client.list_metrica_bindings(offset=100, limit=100)).items == []


async def test_wrong_binding_is_not_accepted() -> None:
    client = YandexServiceClient(
        base_url="http://peer",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"counter": {**ITEM, "equestrian_id": str(uuid4())}})
        ),
    )
    with pytest.raises(ParseTargetsUnavailableError):
        await client.get_metrica_binding(equestrian_id=TENANT)


async def test_url_credentials_rejected_before_transport() -> None:
    def handler(request):
        pytest.fail("No request may be sent")

    client = YandexServiceClient(base_url="http://secret:password@peer", transport=httpx.MockTransport(handler))
    with pytest.raises(ParseTargetsUnavailableError):
        await client.list_metrica_bindings(offset=0, limit=100)


async def test_log_filter_is_task_local_and_resets_on_cancel(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        logging.getLogger("httpcore.http11").debug("secret-response")
        entered.set()
        await release.wait()
        return httpx.Response(200, json={"total": 0, "items": []})

    client = YandexServiceClient(base_url="http://secret-peer", transport=httpx.MockTransport(handler))

    async def request():
        try:
            await client.list_metrica_bindings(offset=0, limit=100)
        finally:
            logging.getLogger("httpx").info("reset after cancellation")

    task = asyncio.create_task(request())
    await entered.wait()
    logging.getLogger("httpx").info("unrelated concurrent request")
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert "secret" not in caplog.text
    assert "unrelated concurrent request" in caplog.text
    assert "reset after cancellation" in caplog.text
