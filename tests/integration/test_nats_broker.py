"""NATS-01: exact runner-owned stream/durable; real broker, no standalone skip.

Run each exact node ID through scripts/qa/infra/run.py --kind nats with a new
manifest/permit. No TEST_NATS_URL fallback; no root resource cleanup in tests.
Namespace adaptation only: production topology validation and publisher stay real.
"""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, DiscardPolicy
from nats.js.errors import NotFoundError

from clients.nats import ParseCommandPublisher
from clients.nats.client import MaintenanceTopologyError, NatsJetstreamClient, _maintenance_stream
from core.schemas.messaging import ParseCommand
from settings import NatsSettings

pytestmark = pytest.mark.infrastructure


@asynccontextmanager
async def broker(qa_infra_context):
    context = qa_infra_context
    resource = context["resource"]
    client = NatsJetstreamClient(NatsSettings(_env_file=None, nats_servers_raw=context["url"]))
    try:
        await asyncio.wait_for(client.connect(), 10)
        info = await client.jetstream.stream_info(resource["stream"])
        expected = _maintenance_stream(resource["stream"], [resource["subject"] + ".>"])
        # Preserve runner ownership token; only this exact owned stream is configured.
        expected.description = info.config.description
        await client.jetstream.update_stream(config=expected)
        try:
            await client.jetstream.delete_consumer(resource["stream"], resource["durable"])
        except NotFoundError:
            pass
        yield client, expected
    finally:
        await client.close()


def consumer_config(resource):
    return ConsumerConfig(
        durable_name=resource["durable"],
        filter_subject=resource["subject"] + ".result",
        deliver_policy=DeliverPolicy.ALL,
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=30,
        max_deliver=-1,
    )


async def validate_existing(client, expected, resource):
    await asyncio.wait_for(
        client._setup_maintenance_topology(
            expected,
            [],
            [(resource["stream"], resource["subject"] + ".result", resource["durable"])],
        ),
        10,
    )


async def test_stream_creation_idempotent_setup_and_duplicate_ack(qa_infra_context):
    """Existing compatible config, production header replay, durable NAK/redelivery/ACK."""
    resource = qa_infra_context["resource"]
    async with broker(qa_infra_context) as (client, expected):
        await client.jetstream.add_consumer(resource["stream"], config=consumer_config(resource))
        before = (await client.jetstream.stream_info(resource["stream"])).config
        durable_before = (await client.jetstream.consumer_info(resource["stream"], resource["durable"])).config
        await validate_existing(client, expected, resource)
        await validate_existing(client, expected, resource)
        assert (await client.jetstream.stream_info(resource["stream"])).config == before
        assert (await client.jetstream.consumer_info(resource["stream"], resource["durable"])).config == durable_before

        class IsolatedTransport:
            async def publish(self, *, subject, payload, headers=None):
                assert subject == "commands.parse.demo.run"
                assert headers and headers["Nats-Msg-Id"]
                return await client.publish(subject=resource["subject"] + ".result", payload=payload, headers=headers)

        command = ParseCommand(
            occurred_at=datetime.now(UTC),
            job_id=uuid4(),
            provider="demo",
            equestrian_id=uuid4(),
            trigger="schedule",
        )
        publisher = ParseCommandPublisher(client=IsolatedTransport())
        first = await publisher.publish(command=command)
        replay = await publisher.publish(command=command)
        assert not first.duplicate and replay.duplicate and replay.message_id == first.message_id
        subscription = await client.jetstream.pull_subscribe_bind(
            durable=resource["durable"],
            stream=resource["stream"],
        )
        try:
            message = (await subscription.fetch(1, timeout=3))[0]
            assert ParseCommand.model_validate_json(message.data) == command
            assert message.headers["Nats-Msg-Id"] == str(first.message_id)
            await message.nak(delay=0)
            redelivered = (await subscription.fetch(1, timeout=3))[0]
            assert redelivered.metadata.num_delivered == 2
            assert redelivered.data == message.data and redelivered.headers == message.headers
            await redelivered.ack_sync(timeout=3)
            assert (await client.jetstream.consumer_info(resource["stream"], resource["durable"])).num_ack_pending == 0
        finally:
            await subscription.unsubscribe()


@pytest.mark.parametrize("field", ["max_age", "discard"])
async def test_incompatible_existing_stream_not_repaired(qa_infra_context, field):
    resource = qa_infra_context["resource"]
    async with broker(qa_infra_context) as (client, expected):
        incompatible = deepcopy(expected)
        if field == "max_age":
            incompatible.max_age = 300
        else:
            incompatible.discard = DiscardPolicy.OLD
        await client.jetstream.update_stream(config=incompatible)
        before = (await client.jetstream.stream_info(resource["stream"])).config
        with pytest.raises(MaintenanceTopologyError, match=field):
            await validate_existing(client, expected, resource)
        assert (await client.jetstream.stream_info(resource["stream"])).config == before
        with pytest.raises(NotFoundError):
            await client.jetstream.consumer_info(resource["stream"], resource["durable"])


@pytest.mark.parametrize("field", ["filter_subject", "ack_wait", "max_deliver"])
async def test_incompatible_existing_durable_not_repaired(qa_infra_context, field):
    resource = qa_infra_context["resource"]
    async with broker(qa_infra_context) as (client, expected):
        config = consumer_config(resource)
        setattr(
            config, field, {"filter_subject": resource["subject"] + ".other", "ack_wait": 60, "max_deliver": 5}[field]
        )
        await client.jetstream.add_consumer(resource["stream"], config=config)
        before = (await client.jetstream.consumer_info(resource["stream"], resource["durable"])).config
        stream_before = (await client.jetstream.stream_info(resource["stream"])).config
        with pytest.raises(MaintenanceTopologyError, match=field):
            await validate_existing(client, expected, resource)
        assert (await client.jetstream.consumer_info(resource["stream"], resource["durable"])).config == before
        assert (await client.jetstream.stream_info(resource["stream"])).config == stream_before


async def test_fixture_failure_closes_client_without_root_cleanup(qa_infra_context):
    with pytest.raises(RuntimeError, match="synthetic fixture failure"):
        async with broker(qa_infra_context) as (client, _expected):
            raise RuntimeError("synthetic fixture failure")
    assert not client.is_connected
    async with broker(qa_infra_context) as (verification, _expected):
        assert (await verification.jetstream.stream_info(qa_infra_context["resource"]["stream"])).config.name
