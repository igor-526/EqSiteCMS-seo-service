"""UT-SEO-20: safe error envelopes and request transaction lifecycle."""

import logging

import pytest
from starlette.exceptions import HTTPException

from core.exceptions import AlreadyExistsError, ClientError, NotFoundError, RepositoryError, UnauthorizedError
from tests.api.conftest import TENANT_ID, RuntimeApi

LEAK = "PRIVATE_INPUT SELECT * FROM secret Traceback postgresql://user:password@db/private"


@pytest.mark.parametrize(
    "error,status",
    [
        (ClientError(LEAK), 400),
        (UnauthorizedError(LEAK), 401),
        (NotFoundError(LEAK), 404),
        (AlreadyExistsError(LEAK), 409),
        (RepositoryError(LEAK), 500),
        (RuntimeError(LEAK), 500),
        (HTTPException(409, detail=LEAK, headers={"X-Leak": LEAK}), 409),
    ],
)
async def test_safe_exception_and_rollback(runtime_api: RuntimeApi, caplog, error, status) -> None:
    runtime_api.services["search_query_service"].resolve.side_effect = error
    with caplog.at_level(logging.ERROR):
        async with runtime_api.client() as client:
            response = await client.post(
                "/api/search-queries/resolve", headers={"X-Equestrian-Id": str(TENANT_ID)}, json={"items": [LEAK]}
            )
    assert response.status_code == status
    assert set(response.json()) == {"detail"}
    assert LEAK not in response.text and LEAK not in caplog.text
    for marker in ("PRIVATE_INPUT", "SELECT", "Traceback", "password", "postgresql://"):
        assert marker not in response.text and marker not in caplog.text
    assert "X-Leak" not in response.headers
    assert runtime_api.events == ["open", "begin", "rollback", "close"]


@pytest.mark.parametrize("kind", ["json", "extra", "type", "query", "selector"])
async def test_validation_never_echoes_input(runtime_api: RuntimeApi, kind) -> None:
    headers = {"X-Equestrian-Id": str(TENANT_ID)}
    async with runtime_api.client() as client:
        if kind == "json":
            response = await client.post("/api/pages/resolve", headers=headers, content='{"' + LEAK)
        elif kind == "query":
            response = await client.get("/api/pages", headers=headers, params={"offset": LEAK})
        elif kind == "selector":
            response = await client.post("/api/pages/resolve", headers={"X-Equestrian-Id": LEAK}, content=LEAK)
        else:
            data = {"items": ["/a"], LEAK: LEAK} if kind == "extra" else {"items": [{LEAK: LEAK}]}
            response = await client.post("/api/pages/resolve", headers=headers, json=data)
    assert response.status_code == (401 if kind == "selector" else 400)
    assert set(response.json()) == {"detail"}
    assert LEAK not in response.text
    if kind != "selector":
        for detail in response.json()["detail"]:
            assert set(detail) == {"type", "loc", "msg"}
            assert detail["msg"] == "Некорректные параметры запроса"
    runtime_api.assert_no_calls()
    assert runtime_api.events == ([] if kind == "selector" else ["open", "begin", "rollback", "close"])


async def test_response_validation_failure_rolls_back(runtime_api: RuntimeApi, caplog) -> None:
    runtime_api.services["page_service"].resolve.return_value = {"items": [{"id": LEAK}]}
    async with runtime_api.client() as client:
        response = await client.post(
            "/api/pages/resolve", headers={"X-Equestrian-Id": str(TENANT_ID)}, json={"items": ["/a"]}
        )
    assert response.status_code == 500
    assert response.json() == {"detail": "Внутренняя ошибка сервиса"}
    assert LEAK not in caplog.text
    assert runtime_api.events == ["open", "begin", "rollback", "close"]
