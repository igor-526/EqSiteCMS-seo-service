"""Integration tests for NATS client (requires infrastructure)."""

import uuid

import pytest

from infrastructure.nats.client import NatsJetstreamClient
from infrastructure.nats.publisher import TaskPublisher
from settings import NatsSettings


@pytest.mark.infrastructure
async def test_nats_client_connection():
    """Test NATS client can connect and disconnect."""
    settings = NatsSettings()
    client = NatsJetstreamClient(settings)

    await client.connect()
    assert client.is_connected

    await client.close()
    assert not client.is_connected


@pytest.mark.infrastructure
async def test_task_publisher_publish():
    """Test TaskPublisher can publish a task."""
    settings = NatsSettings()
    client = NatsJetstreamClient(settings)
    publisher = TaskPublisher(client, settings)

    await client.connect()

    try:
        task_id = str(uuid.uuid4())
        message_id, duplicate = await publisher.publish_task(
            parser_type="yandex_metrics",
            task_id=task_id,
            site_id=1,
            params={"test": "smoke"},
            trace_id="test-trace-123",
        )

        assert message_id == task_id
        assert duplicate is False
    finally:
        await client.close()


@pytest.mark.infrastructure
async def test_task_publisher_duplicate_detection():
    """Test that duplicate tasks are detected."""
    settings = NatsSettings()
    client = NatsJetstreamClient(settings)
    publisher = TaskPublisher(client, settings)

    await client.connect()

    try:
        task_id = str(uuid.uuid4())

        # Первая публикация
        message_id_1, duplicate_1 = await publisher.publish_task(
            parser_type="yandex_metrics",
            task_id=task_id,
            site_id=1,
            params={"test": "duplicate"},
            trace_id="test-trace-dup",
        )

        assert message_id_1 == task_id
        assert duplicate_1 is False

        # Повторная публикация того же task_id (в пределах duplicate window)
        message_id_2, duplicate_2 = await publisher.publish_task(
            parser_type="yandex_metrics",
            task_id=task_id,  # тот же task_id
            site_id=1,
            params={"test": "duplicate"},
            trace_id="test-trace-dup",
        )

        assert message_id_2 == task_id
        assert duplicate_2 is True  # Должен быть обнаружен дубликат
    finally:
        await client.close()
