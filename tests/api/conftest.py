from collections.abc import AsyncIterator, Iterator
from contextlib import ExitStack, asynccontextmanager
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import httpx
import pytest
from dependency_injector import providers

from api import blacklisted_hashes, blacklisted_query_params, pages, parse_jobs, search_queries
from containers import repositories_container, services_container
from core.services.parsing import AcceptedParseJob
from depends import utils as session_dependencies
from main import app

TENANT_ID = UUID("22222222-2222-4222-8222-222222222222")
ITEM_ID = UUID("11111111-1111-4111-8111-111111111111")
ITEM = {"id": str(ITEM_ID), "value": "top", "created_at": "2026-01-01T00:00:00Z"}
API_ROUTES = [
    (method.upper(), path)
    for path, operations in app.openapi()["paths"].items()
    if path.startswith("/api/")
    for method in operations
]


class RequestSession:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def __aenter__(self):
        self.events.append("open")
        return self

    async def __aexit__(self, *args: object) -> None:
        self.events.append("close")

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[None]:
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")


class RuntimeApi:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.services: dict[str, Mock] = {}
        for name in (
            "parse_dispatch_service",
            "search_query_service",
            "page_service",
            "blacklisted_query_param_service",
            "blacklisted_hash_service",
        ):
            service = Mock()
            service.dispatch_manual = AsyncMock(
                side_effect=lambda **kwargs: AcceptedParseJob(job_id=uuid4(), provider=kwargs["provider"])
            )
            service.resolve = AsyncMock(return_value={"items": []})
            service.list_filtered = AsyncMock(return_value={"items": [], "total": 0})
            service.list_all = AsyncMock(return_value={"items": [ITEM], "total": 1})
            service.create = AsyncMock(return_value=ITEM)
            service.update = AsyncMock(return_value=ITEM)
            service.delete = AsyncMock(return_value=None)
            self.services[name] = service

    def session_factory(self) -> RequestSession:
        return RequestSession(self.events)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")

    def assert_no_calls(self) -> None:
        assert all(service.mock_calls == [] for service in self.services.values())


@pytest.fixture
def runtime_api(monkeypatch: pytest.MonkeyPatch) -> Iterator[RuntimeApi]:
    services_container.wire(modules=[search_queries, pages, blacklisted_query_params, blacklisted_hashes, parse_jobs])
    # Async mode is sticky in dependency-injector, even after resource shutdown.
    # Restore provider modes so API calls cannot pollute later synchronous DI tests.
    provider_modes: list[tuple[providers.Provider[Any], bool, bool]] = [
        (provider, provider.is_async_mode_enabled(), provider.is_async_mode_disabled())
        for container in (repositories_container, services_container)
        for provider in cast(Iterator[providers.Provider[Any]], container.traverse())
    ]
    harness = RuntimeApi()
    monkeypatch.setattr(session_dependencies, "SessionFactory", harness.session_factory)
    with ExitStack() as stack:
        for name, service in harness.services.items():
            stack.enter_context(getattr(services_container, name).override(providers.Object(service)))
        from tests.api.test_listing_api import empty_metrics_table

        for name in ("page_service", "search_query_service"):
            harness.services[name].list_filtered.return_value["metrics_table"] = empty_metrics_table()
        yield harness
    assert not repositories_container.session.initialized
    for provider, enabled, disabled in provider_modes:
        if enabled:
            provider.enable_async_mode()
        elif disabled:
            provider.disable_async_mode()
        else:
            provider.reset_async_mode()
