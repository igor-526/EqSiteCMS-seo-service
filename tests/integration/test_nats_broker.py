"""IT-NATS-S1: real JetStream; missing TEST_NATS_URL/broker is a failure."""

import asyncio
import os
from uuid import uuid4

import pytest
from nats.js.api import RetentionPolicy, StorageType
from nats.js.errors import NotFoundError

from clients.nats import NatsJetstreamClient, ParseCommandPublisher
from core.schemas.messaging import ParseCommand
from settings import NatsSettings

pytestmark = pytest.mark.infrastructure


async def test_stream_creation_idempotent_setup_and_duplicate_ack():
    url = os.environ.get("TEST_NATS_URL")
    assert url, "IT-NATS-S1 requires TEST_NATS_URL and a real JetStream broker (no skip)"
    token = uuid4().hex
    stream = f"TEST_SEO_{token}"
    # Isolated namespace avoids overlapping the application's commands.parse.>.
    prefix = f"qa.seo.{token}"
    settings = NatsSettings(
        _env_file=None,
        nats_servers_raw=url,
        nats_stream_parse_commands=stream,
        nats_subjects_parse_commands_raw=f"{prefix}.>",
    )
    client = NatsJetstreamClient(settings)
    peer = NatsJetstreamClient(settings)
    created = False
    try:
        await asyncio.wait_for(client.connect(), timeout=10)
        with pytest.raises(NotFoundError):
            await client.jetstream.stream_info(stream)
        # Mark ownership before setup so partially successful setup is cleaned too.
        created = True
        await asyncio.wait_for(client.setup_streams(), timeout=5)
        info = await client.jetstream.stream_info(stream)
        assert info.config.subjects == [f"{prefix}.>"]
        assert info.config.storage == StorageType.FILE
        assert info.config.retention == RetentionPolicy.LIMITS
        assert info.config.max_age == settings.nats_parse_commands_max_age_seconds
        first = await client.publish(subject=f"{prefix}.probe", payload=b"probe", headers={"Nats-Msg-Id": token})
        assert not first.duplicate
        await asyncio.wait_for(peer.connect(), timeout=10)
        await asyncio.wait_for(asyncio.gather(client.setup_streams(), peer.setup_streams()), timeout=5)
        assert (await client.jetstream.stream_info(stream)).state.messages == 1
        duplicate = await peer.publish(subject=f"{prefix}.probe", payload=b"probe", headers={"Nats-Msg-Id": token})
        assert duplicate.duplicate and duplicate.seq == first.seq

        class IsolatedTransport:
            """Only the subject namespace changes; production publisher/envelope stay real."""

            async def publish(self, *, subject, payload, headers=None):
                assert subject == "commands.parse.demo.run"
                return await client.publish(subject=f"{prefix}.demo.run", payload=payload, headers=headers)

        from datetime import UTC, datetime

        command = ParseCommand(
            occurred_at=datetime.now(UTC), job_id=uuid4(), provider="demo", equestrian_id=uuid4(), trigger="schedule"
        )
        publisher = ParseCommandPublisher(client=IsolatedTransport())
        published = await publisher.publish(command=command)
        repeated = await publisher.publish(command=command)
        assert not published.duplicate and repeated.duplicate
        assert published.message_id == repeated.message_id
        info = await client.jetstream.stream_info(stream)
        assert info.state.messages == 2
        message = await client.jetstream.get_msg(stream, seq=2)
        assert ParseCommand.model_validate_json(message.data) == command
    finally:
        try:
            if created and client.is_connected:
                await asyncio.wait_for(client.jetstream.delete_stream(stream), timeout=5)
        finally:
            try:
                await peer.close()
            finally:
                await client.close()
