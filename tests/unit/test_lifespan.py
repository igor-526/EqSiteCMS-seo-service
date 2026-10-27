"""SEO-5: non-blocking shared NATS startup, retry and shutdown."""

import asyncio
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from dependency_injector import providers

import main
from containers.application import clients_container


async def test_lifespan_health_during_pending_connect_and_graceful_stop(monkeypatch):
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def connect():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    client = Mock(connect=AsyncMock(side_effect=connect), setup_streams=AsyncMock(), close=AsyncMock())
    close_database = AsyncMock()
    monkeypatch.setattr(main, "close_database", close_database)
    with clients_container.nats_client.override(providers.Object(client)):
        async with main.lifespan(main.app):
            await asyncio.wait_for(started.wait(), timeout=1)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url="http://test") as http:
                response = await http.get("/health")
            assert response.status_code == 200 and response.json() == {"status": "ok"}
            close_database.assert_not_awaited()
        assert cancelled.is_set()
    client.setup_streams.assert_not_awaited()
    client.close.assert_awaited_once()
    close_database.assert_awaited_once()
    assert not any(task.get_name() == "parse-queue-startup" for task in asyncio.all_tasks())


@pytest.mark.parametrize("failed_step", ["connect", "setup_streams"])
async def test_startup_retries_with_bounded_backoff_without_leaks(monkeypatch, caplog, failed_step):
    client = Mock(connect=AsyncMock(), setup_streams=AsyncMock(), close=AsyncMock())
    getattr(client, failed_step).side_effect = [RuntimeError("nats://secret:password@private")] * 7 + [None]
    sleep = AsyncMock()
    # Patch main's asyncio binding, not the event loop used by pytest.
    monkeypatch.setattr(main, "asyncio", Mock(sleep=sleep))
    await main.setup_nats(client)
    assert [call.args[0] for call in sleep.await_args_list] == [1, 2, 4, 8, 16, 30, 30]
    assert client.connect.await_count == 8
    assert client.setup_streams.await_count == (1 if failed_step == "connect" else 8)
    assert "password" not in caplog.text and "nats://" not in caplog.text


async def test_ready_startup_uses_shared_publisher_client_and_closes(monkeypatch):
    ready = asyncio.Event()
    client = Mock(connect=AsyncMock(), setup_streams=AsyncMock(side_effect=ready.set), close=AsyncMock())
    close_database = AsyncMock()
    monkeypatch.setattr(main, "close_database", close_database)
    with clients_container.nats_client.override(providers.Object(client)):
        # Factory publisher must consume exactly the instance initialized by lifespan.
        publisher = clients_container.parse_command_publisher()
        assert publisher._client is client
        async with main.lifespan(main.app):
            await asyncio.wait_for(ready.wait(), timeout=1)
        client.connect.assert_awaited_once()
        client.setup_streams.assert_awaited_once()
        client.close.assert_awaited_once()
    close_database.assert_awaited_once()


async def test_database_cleanup_even_if_nats_close_fails(monkeypatch):
    client = Mock(connect=AsyncMock(), setup_streams=AsyncMock(), close=AsyncMock(side_effect=RuntimeError))
    close_database = AsyncMock()
    monkeypatch.setattr(main, "close_database", close_database)
    with clients_container.nats_client.override(providers.Object(client)):
        with pytest.raises(RuntimeError):
            async with main.lifespan(main.app):
                pass
    close_database.assert_awaited_once()
