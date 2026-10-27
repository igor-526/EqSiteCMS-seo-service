"""UT-SQ-02/03: query-only wire, safe errors and credentials-free requests."""

import logging
import traceback
from datetime import date
from typing import Any
from uuid import uuid4

import httpx
import pytest

from clients.ya_metrica import YaMetricaClient
from core.exceptions import MetricaUnavailableError
from core.protocols.clients.ya_metrica import YaMetricaClientProtocol
from core.services.metrics_period import PeriodColumn

TENANT, QUERY, OTHER = uuid4(), uuid4(), uuid4()
DAY = date(2026, 10, 19)
COLUMNS = [
    PeriodColumn("2026-10-19", DAY, date(2026, 10, 25)),
    PeriodColumn("2026-10-26", date(2026, 10, 26), date(2026, 10, 26)),
]


@pytest.mark.parametrize(
    "metric",
    [
        "visits",
        "users",
        "pageviews",
        "bounces",
        "bounce_rate",
        "page_depth",
        "avg_visit_duration_seconds",
        "visit_duration_seconds",
    ],
)
@pytest.mark.parametrize("direction", ["asc", "desc"])
async def test_table_and_ranking_contract(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch, metric: str, direction: Any
) -> None:
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("HTTP_PROXY", "http://unreachable-secret-proxy:1")
    monkeypatch.setenv("HTTPS_PROXY", "http://unreachable-secret-proxy:1")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path in {"/api/metrica/search-query-stats/table", "/api/metrica/search-query-stats/ranking"}
        assert "page_ids" not in request.url.params
        assert "cookie" not in request.headers and "authorization" not in request.headers
        assert request.extensions["timeout"] == dict.fromkeys(["connect", "read", "write", "pool"], 2.5)
        assert request.url.params["equestrian_id"] == str(TENANT)
        assert request.url.params["metric"] == metric
        if request.url.path.endswith("table"):
            assert request.url.params.get_list("bucket") == ["2026-10-19..2026-10-25", "2026-10-26..2026-10-26"]
            assert request.url.params.get_list("search_query_ids") == [str(QUERY), str(OTHER)]
            return httpx.Response(
                200,
                json={"metric": metric, "values": [{"search_query_id": str(QUERY), "bucket": str(DAY), "value": 12}]},
                headers={"Set-Cookie": "secret-cookie=private"},
            )
        assert request.url.params["date_from"] == "2026-10-19"
        assert request.url.params["date_to"] == "2026-10-25"
        assert request.url.params["direction"] == direction
        return httpx.Response(200, json={"metric": metric, "direction": direction, "search_query_ids": [str(QUERY)]})

    client: YaMetricaClientProtocol = YaMetricaClient(
        base_url="http://secret-peer", timeout=2.5, transport=httpx.MockTransport(handler)
    )
    assert requests == []
    table = await client.query_table(
        equestrian_id=TENANT, metric=metric, columns=COLUMNS, search_query_ids=[QUERY, OTHER]
    )
    assert table.values[0].search_query_id == QUERY and table.values[0].value == 12
    ranking = await client.query_ranking(
        equestrian_id=TENANT, metric=metric, date_from=DAY, date_to=date(2026, 10, 25), direction=direction
    )
    assert ranking.search_query_ids == [QUERY] and len(requests) == 2
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
        payload: dict[str, Any] = {"metric": "visits", "values": [], "direction": "asc", "search_query_ids": []}
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
                            '{"metric":"visits","values":[{'
                            f'"search_query_id":"{QUERY}","bucket":"{DAY}","value":NaN'
                            "}]}"
                        ),
                    )
                payload["values"] = [{"search_query_id": str(QUERY), "bucket": str(DAY), "value": value}]
            else:
                payload["search_query_ids"] = [value]
        # A direction field is irrelevant to table; make this case invalid too.
        if method == "table" and failure == "direction":
            payload["values"] = "secret-body"
        return httpx.Response(200, json=payload)

    client = YaMetricaClient(base_url="http://secret-peer", transport=httpx.MockTransport(handler))
    with pytest.raises(MetricaUnavailableError) as caught:
        if method == "table":
            await client.query_table(equestrian_id=TENANT, metric="visits", columns=COLUMNS, search_query_ids=[QUERY])
        else:
            await client.query_ranking(
                equestrian_id=TENANT, metric="visits", date_from=DAY, date_to=DAY, direction="desc"
            )
    assert caught.value.status_code == 502
    rendered = "".join(traceback.format_exception(caught.value)) + caplog.text
    assert "secret-peer" not in rendered and "secret-body" not in rendered
    assert str(caught.value) == "Сервис статистики недоступен"


async def test_url_credentials_are_rejected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("No request may be sent with URL credentials")

    client = YaMetricaClient(base_url="http://user:secret@peer", transport=httpx.MockTransport(handler))
    with pytest.raises(MetricaUnavailableError):
        await client.query_table(equestrian_id=TENANT, metric="visits", columns=COLUMNS, search_query_ids=[QUERY])


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
    await client.query_table(equestrian_id=TENANT, metric="visits", columns=COLUMNS, search_query_ids=[QUERY])
    assert options[0]["trust_env"] is False and options[0]["follow_redirects"] is False
    assert options[0]["timeout"] == 4
    assert "auth" not in options[0] and "cookies" not in options[0]


async def test_valid_but_mismatched_direction_is_rejected() -> None:
    client = YaMetricaClient(
        base_url="http://peer",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"metric": "visits", "direction": "asc", "search_query_ids": []})
        ),
    )
    with pytest.raises(MetricaUnavailableError):
        await client.query_ranking(equestrian_id=TENANT, metric="visits", date_from=DAY, date_to=DAY, direction="desc")


@pytest.mark.parametrize("method", ["table", "ranking"])
async def test_page_wire_cannot_masquerade_as_queries(method: str) -> None:
    payload = {
        "metric": "visits",
        "direction": "asc",
        "page_ids": [str(QUERY)],
        "values": [{"page_id": str(QUERY), "bucket": str(DAY), "value": 1}],
    }
    client = YaMetricaClient(
        base_url="http://peer", transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    )
    with pytest.raises(MetricaUnavailableError):
        if method == "table":
            await client.query_table(equestrian_id=TENANT, metric="visits", columns=COLUMNS, search_query_ids=[QUERY])
        else:
            await client.query_ranking(
                equestrian_id=TENANT, metric="visits", date_from=DAY, date_to=DAY, direction="asc"
            )
