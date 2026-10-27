import asyncio
import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nats.js.api import PubAck

from clients.nats.publisher import ParseCommandPublisher
from core.exceptions.parsing import ParseQueueUnavailableError
from core.schemas.messaging import ParseCommand, msg_id


def command() -> ParseCommand:
    return ParseCommand(
        occurred_at=datetime.now(UTC),
        job_id=uuid4(),
        provider="ya_metrica",
        equestrian_id=uuid4(),
        trigger="manual",
    )


@pytest.mark.parametrize("duplicate", [False, True])
async def test_publish_ut_ps_07(duplicate: bool, caplog: pytest.LogCaptureFixture) -> None:
    client = AsyncMock()
    client.publish.return_value = PubAck(stream="PARSE_COMMANDS", seq=1, duplicate=duplicate)
    payload = command()
    with caplog.at_level(logging.WARNING):
        result = await ParseCommandPublisher(client=client).publish(command=payload)
    assert result.job_id == payload.job_id
    assert result.message_id == msg_id(payload.provider, payload.job_id)
    assert result.duplicate is duplicate
    client.publish.assert_awaited_once_with(
        subject="commands.parse.ya_metrica.run",
        payload=payload.model_dump_json(exclude_none=True).encode(),
        headers={"Nats-Msg-Id": str(result.message_id)},
    )
    assert ("deduplicated" in caplog.text) is duplicate
    if duplicate:
        assert str(payload.job_id) in caplog.text


@pytest.mark.parametrize("error", [TimeoutError("secret"), RuntimeError("secret"), OSError("secret")])
async def test_failure_ut_ps_07(error: Exception, caplog: pytest.LogCaptureFixture) -> None:
    client = AsyncMock()
    client.publish.side_effect = error
    with pytest.raises(ParseQueueUnavailableError) as caught:
        await ParseCommandPublisher(client=client).publish(command=command())
    assert caught.value.status_code == 503
    assert "secret" not in str(caught.value)
    assert "secret" not in caplog.text


async def test_cancellation_is_not_queue_error_ut_ps_07() -> None:
    client = AsyncMock()
    client.publish.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await ParseCommandPublisher(client=client).publish(command=command())
