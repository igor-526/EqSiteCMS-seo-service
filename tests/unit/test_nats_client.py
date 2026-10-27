import logging
from unittest.mock import AsyncMock, MagicMock, patch

import nats.errors
import pytest
from nats.js.api import RetentionPolicy, StorageType

from clients.nats.client import NatsJetstreamClient
from clients.nats.lifecycle import NatsConnectionErrorPolicy
from settings import NatsSettings


@pytest.fixture(autouse=True)
def disable_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(NatsSettings.model_config, "env_file", None)


def connected_client() -> tuple[NatsJetstreamClient, MagicMock, MagicMock]:
    client = NatsJetstreamClient(NatsSettings())
    connection = MagicMock(is_connected=True, is_closed=False)
    connection.drain = AsyncMock()
    connection.close = AsyncMock()
    jetstream = MagicMock()
    jetstream.add_stream = AsyncMock()
    jetstream.add_consumer = AsyncMock()
    jetstream.publish = AsyncMock()
    connection.jetstream.return_value = jetstream
    client._connection = connection
    client._jetstream = jetstream
    return client, connection, jetstream


async def test_topology_ut_ps_08() -> None:
    client, _, jetstream = connected_client()
    await client.setup()
    await client.setup()
    assert jetstream.add_stream.await_count == 2
    first, second = jetstream.add_stream.await_args_list
    config = first.kwargs["config"]
    assert config == second.kwargs["config"]
    assert config.name == "PARSE_COMMANDS"
    assert config.subjects == ["commands.parse.>"]
    assert config.storage == StorageType.FILE
    assert config.retention == RetentionPolicy.LIMITS
    assert config.max_age == 7 * 86400
    jetstream.add_consumer.assert_not_called()


async def test_stream_config_override_ut_ps_08() -> None:
    client, _, jetstream = connected_client()
    client._settings = NatsSettings.model_validate(
        {"NATS_STREAM_PARSE_COMMANDS": "TEST", "NATS_PARSE_COMMANDS_MAX_AGE_SECONDS": 12}
    )
    await client.setup_streams()
    assert jetstream.add_stream.await_args.kwargs["config"].max_age == 12
    assert jetstream.add_stream.await_args.kwargs["config"].name == "TEST"


@pytest.mark.parametrize("error", [TimeoutError(), nats.errors.ConnectionReconnectingError()])
async def test_close_drain_failure_ut_ps_08(error: Exception) -> None:
    client, connection, _ = connected_client()
    connection.drain.side_effect = error
    await client.close()
    connection.close.assert_awaited_once()
    assert not client.is_connected
    assert client._jetstream is None
    await client.close()


async def test_connect_publish_and_close_ut_ps_08() -> None:
    client, connection, jetstream = connected_client()
    client._connection = None
    client._jetstream = None
    connection.connect = AsyncMock()
    with patch("clients.nats.client.NATS", return_value=connection):
        await client.connect()
        await client.connect()
    connection.connect.assert_awaited_once()
    options = connection.connect.await_args.kwargs
    assert options["servers"] == ["nats://eqsitecms-nats:4222"]
    assert options["error_cb"] == client._error_policy.on_error
    ack = await client.publish(subject="commands.parse.demo.run", payload=b"{}", headers={"Nats-Msg-Id": "id"})
    assert ack is jetstream.publish.return_value
    jetstream.publish.assert_awaited_once_with(
        subject="commands.parse.demo.run", payload=b"{}", headers={"Nats-Msg-Id": "id"}, timeout=5.0
    )
    await client.close()
    connection.drain.assert_awaited_once()


async def test_not_connected_and_failed_initial_connect_ut_ps_08() -> None:
    client = NatsJetstreamClient(NatsSettings())
    with pytest.raises(RuntimeError, match="not connected"):
        await client.setup_streams()
    connection = MagicMock(is_connected=False, is_closed=False)
    connection.connect = AsyncMock(side_effect=OSError())
    connection.drain = AsyncMock(side_effect=nats.errors.ConnectionReconnectingError())
    connection.close = AsyncMock()
    with patch("clients.nats.client.NATS", return_value=connection), pytest.raises(OSError):
        await client.connect()
    connection.close.assert_awaited_once()
    assert client._connection is None


async def test_error_policy_escalates_once_ut_ps_08(caplog: pytest.LogCaptureFixture) -> None:
    policy = NatsConnectionErrorPolicy(service_name="seo-service", report_after_attempts=2)
    with caplog.at_level(logging.WARNING):
        for _ in range(5):
            await policy.on_error(OSError("offline"))
        assert policy.escalated
        assert sum(record.levelno == logging.ERROR for record in caplog.records) == 1
        await policy.on_reconnected()
        assert policy.consecutive_failures == 0
        for _ in range(3):
            await policy.on_error(OSError("offline"))
        assert sum(record.levelno == logging.ERROR for record in caplog.records) == 2
