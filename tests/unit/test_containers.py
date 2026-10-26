import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from dependency_injector import providers
from fastapi import Request, Response
from fastapi.routing import APIRoute
from sqlalchemy.ext.asyncio import AsyncSession

from containers import application
from containers.repositories import RepositoriesContainer
from containers.services import ServicesContainer
from containers.settings import SettingsContainer
from core.exceptions import ClientError
from core.services import (
    BlacklistedHashService,
    BlacklistedQueryParamService,
    PageNormalizer,
    PageService,
    SearchQueryNormalizer,
    SearchQueryService,
)
from depends import utils
from repositories import (
    BlacklistedHashRepository,
    BlacklistedQueryParamRepository,
    PageRepository,
    SearchQueryRepository,
)
from utils import transactional_route


def test_settings_singleton_and_real_provider_graph() -> None:
    settings = SettingsContainer()
    repositories = RepositoriesContainer()
    services = ServicesContainer(repositories_container=repositories)
    assert isinstance(settings.app_settings, providers.Singleton)
    assert settings.app_settings() is settings.app_settings()
    assert isinstance(repositories.session, providers.ContextLocalResource)
    session = AsyncMock(spec=AsyncSession)
    with repositories.session.override(providers.Object(session)):
        for name, cls in (
            ("search_query_repository", SearchQueryRepository),
            ("page_repository", PageRepository),
            ("blacklisted_query_param_repository", BlacklistedQueryParamRepository),
            ("blacklisted_hash_repository", BlacklistedHashRepository),
        ):
            provider = getattr(repositories, name)
            repository = provider()
            assert isinstance(provider, providers.Factory)
            assert isinstance(repository, cls)
            assert repository.session is session
            assert provider() is not repository
        for name, service_cls, dependencies in (
            ("search_query_service", SearchQueryService, ["search_query_repository"]),
            (
                "page_service",
                PageService,
                ["page_repository", "blacklisted_query_param_repository", "blacklisted_hash_repository"],
            ),
            ("blacklisted_query_param_service", BlacklistedQueryParamService, ["blacklisted_query_param_repository"]),
            ("blacklisted_hash_service", BlacklistedHashService, ["blacklisted_hash_repository"]),
        ):
            provider = getattr(services, name)
            service = provider()
            assert isinstance(provider, providers.Factory)
            assert isinstance(service, service_cls)
            assert provider() is not service
            for dependency in dependencies:
                repository = getattr(service, f"_{dependency}")
                assert isinstance(repository, getattr(repositories, dependency).provides)
                assert repository.session is session
        assert isinstance(services.search_query_service()._search_query_normalizer, SearchQueryNormalizer)
        assert isinstance(services.page_service()._page_normalizer, PageNormalizer)
        for name in ("search_query_normalizer", "page_normalizer"):
            provider = getattr(services, name)
            assert isinstance(provider, providers.Factory)
            assert provider() is not provider()
    assert not repositories.session.overridden
    assert not repositories.session.initialized


def test_application_graph_is_lazy_and_connected() -> None:
    assert not application.repositories_container.session.initialized
    session = AsyncMock(spec=AsyncSession)
    with application.repositories_container.session.override(providers.Object(session)):
        service = cast(Any, application.services_container.page_service())
        assert service._page_repository.session is session
        assert service._blacklisted_query_param_repository.session is session
        assert service._blacklisted_hash_repository.session is session
    assert not application.repositories_container.session.overridden


def install_session_factory(monkeypatch: pytest.MonkeyPatch) -> list[AsyncMock]:
    sessions: list[AsyncMock] = []

    def factory() -> AsyncMock:
        session = AsyncMock(spec=AsyncSession)
        session.__aenter__.return_value = session
        sessions.append(session)
        return session

    monkeypatch.setattr(utils, "SessionFactory", Mock(side_effect=factory))
    return sessions


async def test_context_local_resource_isolates_concurrent_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions = install_session_factory(monkeypatch)
    repositories = RepositoriesContainer()
    services = ServicesContainer(repositories_container=repositories)
    barrier = asyncio.Barrier(2)

    async def request() -> AsyncSession:
        session = await repositories.session.async_()
        try:
            await barrier.wait()
            service = cast(Any, await services.page_service.async_())
            assert service._page_repository.session is session
            assert service._blacklisted_query_param_repository.session is session
            assert service._blacklisted_hash_repository.session is session
            assert await repositories.session.async_() is session
            return session
        finally:
            await cast(Any, repositories.session).shutdown()
            assert not repositories.session.initialized

    first, second = await asyncio.gather(request(), request())
    assert first is not second and len(sessions) == 2
    assert not repositories.session.initialized
    # A later request receives a new session, rather than a closed resource.
    third = await repositories.session.async_()
    assert third is not first and third is not second
    await cast(Any, repositories.session).shutdown()
    assert len(sessions) == 3
    for session in sessions:
        session.__aenter__.assert_awaited_once()
        session.__aexit__.assert_awaited_once_with(None, None, None)
        session.commit.assert_not_awaited()
        session.rollback.assert_not_awaited()


async def test_route_initializes_and_releases_each_concurrent_request(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions = install_session_factory(monkeypatch)
    repositories = RepositoriesContainer()
    services = ServicesContainer(repositories_container=repositories)
    monkeypatch.setattr(transactional_route, "repositories_container", repositories)
    barrier = asyncio.Barrier(2)

    async def original_handler(request: Request) -> Response:
        service = cast(Any, await services.page_service.async_())
        session = service._page_repository.session
        assert service._blacklisted_hash_repository.session is session
        await barrier.wait()
        assert await repositories.session.async_() is session
        return Response(status_code=204)

    monkeypatch.setattr(APIRoute, "get_route_handler", lambda self: original_handler)
    handler = transactional_route.TransactionalRoute("/test", endpoint=original_handler).get_route_handler()
    assert not sessions and not repositories.session.initialized

    async def request() -> None:
        response = await handler(Request({"type": "http", "path": "/test", "headers": []}))
        assert response.status_code == 204
        assert not repositories.session.initialized

    await asyncio.gather(request(), request())
    assert len(sessions) == 2 and sessions[0] is not sessions[1]
    for session in sessions:
        session.begin.assert_called_once_with()
        session.begin.return_value.__aexit__.assert_awaited_once_with(None, None, None)
        session.__aexit__.assert_awaited_once_with(None, None, None)


@pytest.mark.parametrize("outcome", ["success", "domain_error", "cancel", "commit_error", "begin_error"])
async def test_transaction_boundary_and_cleanup(monkeypatch: pytest.MonkeyPatch, outcome: str) -> None:
    sessions = install_session_factory(monkeypatch)
    repositories = RepositoriesContainer()
    monkeypatch.setattr(transactional_route, "repositories_container", repositories)
    transaction = AsyncMock()
    error: BaseException | None = None
    if outcome == "domain_error":
        error = ClientError()
    elif outcome == "cancel":
        error = asyncio.CancelledError()
    elif outcome in ("commit_error", "begin_error"):
        error = RuntimeError("transaction failed")
        target = transaction.__aexit__ if outcome == "commit_error" else transaction.__aenter__
        target.side_effect = error
    response = Response(content="ok")

    async def original_handler(request: Request) -> Response:
        assert request.url.path == "/test"
        assert repositories.session.initialized
        session = await repositories.session.async_()
        assert (await repositories.search_query_repository.async_()).session is session
        transaction.__aenter__.assert_awaited_once()
        if outcome in ("domain_error", "cancel"):
            assert error is not None
            raise error
        return response

    monkeypatch.setattr(APIRoute, "get_route_handler", lambda self: original_handler)
    route = transactional_route.TransactionalRoute("/test", endpoint=original_handler)
    handler = route.get_route_handler()
    # Initialize in this context so the route uses this mock transaction.
    session = cast(AsyncMock, await repositories.session.async_())
    session.begin.return_value = transaction
    request = Request({"type": "http", "path": "/test", "headers": []})
    if error is None:
        assert await handler(request) is response
    else:
        with pytest.raises(type(error)) as caught:
            await handler(request)
        assert caught.value is error
    session.begin.assert_called_once_with()
    if outcome in ("success", "commit_error"):
        transaction.__aexit__.assert_awaited_once_with(None, None, None)
    elif outcome == "begin_error":
        transaction.__aexit__.assert_not_awaited()
    else:
        args = transaction.__aexit__.await_args.args
        assert args[0] is type(error) and args[1] is error
    assert len(sessions) == 1
    sessions[0].__aexit__.assert_awaited_once_with(None, None, None)
    assert not repositories.session.initialized
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()
