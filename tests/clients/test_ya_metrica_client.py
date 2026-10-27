import logging
import traceback
from datetime import date
from typing import Any
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from clients.ya_metrica import YaMetricaClient
from core.exceptions import MetricaUnavailableError
from core.protocols.clients.ya_metrica import YaMetricaClientProtocol
from core.services.metrics_period import PeriodColumn
from settings import Settings

TENANT, PAGE, OTHER = uuid4(), uuid4(), uuid4()
DAY = date(2026, 10, 19)
COLUMNS = [
    PeriodColumn("2026-10-19", DAY, date(2026, 10, 25)),
    PeriodColumn("2026-10-26", date(2026, 10, 26), date(2026, 10, 26)),
]


async def test_table_and_ranking_contract(caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("HTTP_PROXY", "http://unreachable-secret-proxy:1")
    monkeypatch.setenv("HTTPS_PROXY", "http://unreachable-secret-proxy:1")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert "cookie" not in request.headers and "authorization" not in request.headers
        assert request.extensions["timeout"] == dict.fromkeys(["connect", "read", "write", "pool"], 2.5)
        assert request.url.params["equestrian_id"] == str(TENANT)
        assert request.url.params["metric"] == "visits"
        if request.url.path.endswith("table"):
            assert request.url.params.get_list("bucket") == ["2026-10-19..2026-10-25", "2026-10-26..2026-10-26"]
            assert request.url.params.get_list("page_ids") == [str(PAGE), str(OTHER)]
            return httpx.Response(
                200,
                json={"metric": "visits", "values": [{"page_id": str(PAGE), "bucket": str(DAY), "value": 12}]},
                headers={"Set-Cookie": "secret-cookie=private"},
            )
        assert request.url.params["date_from"] == "2026-10-19"
        assert request.url.params["date_to"] == "2026-10-25"
        assert request.url.params["direction"] == "desc"
        return httpx.Response(200, json={"metric": "visits", "direction": "desc", "page_ids": [str(PAGE)]})

    client: YaMetricaClientProtocol = YaMetricaClient(
        base_url="http://secret-peer", timeout=2.5, transport=httpx.MockTransport(handler)
    )
    table = await client.table(equestrian_id=TENANT, metric="visits", columns=COLUMNS, page_ids=[PAGE, OTHER])
    assert table.values[0].page_id == PAGE and table.values[0].value == 12
    ranking = await client.ranking(
        equestrian_id=TENANT, metric="visits", date_from=DAY, date_to=date(2026, 10, 25), direction="desc"
    )
    assert ranking.page_ids == [PAGE] and len(requests) == 2
    assert "secret-peer" not in caplog.text and "secret-cookie" not in caplog.text
    # The task-local filter resets after each request.
    logging.getLogger("httpx").info("unrelated request")
    assert "unrelated request" in caplog.text


@pytest.mark.parametrize("method", ["table", "ranking"])
@pytest.mark.parametrize(
    "failure",
    [
        "400",
        "401",
        "404",
        "500",
        "503",
        "302",
        "204",
        "timeout",
        "transport",
        "json",
        "schema",
        "metric",
        "direction",
        "nan",
        "null",
        "bool",
        "string",
    ],
)
async def test_safe_failures(method: str, failure: str, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    def handler(request: httpx.Request) -> httpx.Response:
        if failure == "timeout":
            raise httpx.ReadTimeout("secret-peer secret-body", request=request)
        if failure == "transport":
            raise httpx.ConnectError("secret-peer secret-body", request=request)
        if failure.isdigit():
            return httpx.Response(int(failure), text="secret-body", headers={"Location": "http://secret-peer/other"})
        if failure == "json":
            return httpx.Response(200, text="secret-body")
        if failure == "schema":
            return httpx.Response(200, json={"unexpected": "secret-body"})
        payload: dict[str, Any] = {"metric": "visits", "values": [], "direction": "asc", "page_ids": []}
        if failure == "metric":
            payload["metric"] = "users"
        elif failure == "direction":
            payload["direction"] = "invalid"
        else:
            value = {"nan": float("nan"), "null": None, "bool": True, "string": "12"}[failure]
            if method == "table":
                # NaN must be decoded, but is forbidden by the response schema.
                if failure == "nan":
                    return httpx.Response(
                        200,
                        content=(
                            f'{{"metric":"visits","values":[{{"page_id":"{PAGE}","bucket":"{DAY}","value":NaN}}]}}'
                        ),
                    )
                payload["values"] = [{"page_id": str(PAGE), "bucket": str(DAY), "value": value}]
            else:
                payload["page_ids"] = [value]
        # A direction field is irrelevant to table; make this case invalid too.
        if method == "table" and failure == "direction":
            payload["values"] = "secret-body"
        return httpx.Response(200, json=payload)

    client = YaMetricaClient(base_url="http://secret-peer", transport=httpx.MockTransport(handler))
    with pytest.raises(MetricaUnavailableError) as caught:
        if method == "table":
            await client.table(equestrian_id=TENANT, metric="visits", columns=COLUMNS, page_ids=[PAGE])
        else:
            await client.ranking(equestrian_id=TENANT, metric="visits", date_from=DAY, date_to=DAY, direction="desc")
    assert caught.value.status_code == 502
    rendered = "".join(traceback.format_exception(caught.value)) + caplog.text
    assert "secret-peer" not in rendered and "secret-body" not in rendered
    assert str(caught.value) == "Сервис статистики недоступен"


def test_settings_defaults_and_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("YA_METRICA_SERVICE_URL", "YA_METRICA_TIMEOUT_SECONDS"):
        monkeypatch.delenv(key, raising=False)
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.ya_metrica_service_url == "http://eqsitecms-ya-metrica-service:8000"
    assert settings.ya_metrica_timeout_seconds == 4
    monkeypatch.setenv("YA_METRICA_SERVICE_URL", "http://another-peer:8000")
    monkeypatch.setenv("YA_METRICA_TIMEOUT_SECONDS", "1.25")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.ya_metrica_service_url == "http://another-peer:8000"
    assert settings.ya_metrica_timeout_seconds == 1.25
    for value in ("0", "-1", "inf", "nan"):
        monkeypatch.setenv("YA_METRICA_TIMEOUT_SECONDS", value)
        with pytest.raises(ValidationError):
            Settings(_env_file=None)  # type: ignore[call-arg]


async def test_url_credentials_are_rejected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("No request may be sent with URL credentials")

    client = YaMetricaClient(base_url="http://user:secret@peer", transport=httpx.MockTransport(handler))
    with pytest.raises(MetricaUnavailableError):
        await client.table(equestrian_id=TENANT, metric="visits", columns=COLUMNS, page_ids=[PAGE])


async def test_environment_disabled_and_no_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    original = httpx.AsyncClient
    options: list[dict[str, Any]] = []

    def factory(**kwargs: Any) -> httpx.AsyncClient:
        options.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)
    client = YaMetricaClient(
        base_url="http://peer",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"metric": "visits", "values": []})),
    )
    await client.table(equestrian_id=TENANT, metric="visits", columns=COLUMNS, page_ids=[PAGE])
    assert options[0]["trust_env"] is False and options[0]["follow_redirects"] is False
    assert options[0]["timeout"] == 4
    assert "auth" not in options[0] and "cookies" not in options[0]
