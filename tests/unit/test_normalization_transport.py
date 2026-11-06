"""UT-N-04 producer replay and bounded relay; no broker acceptance claimed."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    ConsumerInfo,
    DeliverPolicy,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
    StreamInfo,
    StreamState,
)
from nats.js.errors import BadRequestError, NotFoundError

from clients.nats.client import NatsJetstreamClient
from clients.nats.publisher import MaintenancePublisher
from core.entities.normalization import MaintenanceMessage, OutboxClaim, StorageLease
from core.services.normalization_outbox_relay import NormalizationOutboxRelay
from settings import NatsSettings

NOW = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def disable_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(NatsSettings.model_config, "env_file", None)


def message(kind: str = "seal", /, **updates: object) -> MaintenanceMessage:
    tenant, operation, result = uuid4(), uuid4(), uuid4()
    subject = "events.seo.pages.merge.finalized" if kind == "finalized" else "commands.seo.pages.merge"
    suffix = f"finalized:{result}" if kind == "finalized" else "0" if kind == "mappings" else "seal"
    identity = uuid5(NAMESPACE_URL, f"eqcms:maintenance:v1:{subject}:{operation}:{suffix}")
    data: dict[str, object] = dict(
        schema_version=1,
        message_id=str(identity),
        operation_id=str(operation),
        equestrian_id=str(tenant),
        occurred_at="2026-01-01T00:00:00Z",
        normalization_revision=1,
    )
    if kind == "finalized":
        data["result_message_id"] = str(result)
    else:
        data["kind"] = kind
        if kind == "mappings":
            data.update(part_index=0, mappings=[dict(source_page_id=str(uuid4()), target_page_id=str(uuid4()))])
        else:
            data["part_count"] = 0
    data.update(updates)
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    # Deliberately not canonical whitespace: publisher must preserve stored bytes.
    payload = json.dumps(data, indent=2)
    return MaintenanceMessage(
        tenant,
        operation,
        identity,
        kind,
        payload,
        sha256(canonical.encode()).hexdigest(),
        0 if kind == "mappings" else None,
    )


@pytest.mark.parametrize("kind", ["mappings", "seal", "finalized"])
@pytest.mark.parametrize("duplicate", [False, True])
async def test_saved_wire_replay(kind: str, duplicate: bool, caplog: pytest.LogCaptureFixture) -> None:
    client = MagicMock(publish=AsyncMock(return_value=MagicMock(duplicate=duplicate)))
    publisher = MaintenancePublisher(client=client)
    saved = message(kind)
    for _ in range(2):
        assert await publisher.publish(message=saved) is duplicate
    calls = client.publish.await_args_list
    assert calls[0] == calls[1]
    assert calls[0].kwargs["payload"] == saved.payload_json.encode()
    assert calls[0].kwargs["headers"] == {"Nats-Msg-Id": str(saved.message_id)}
    assert calls[0].kwargs["subject"] == (
        "events.seo.pages.merge.finalized" if kind == "finalized" else "commands.seo.pages.merge"
    )
    assert ("deduplicated" in caplog.text) is duplicate


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": 2},
        {"schema_version": True},
        {"token": "DO_NOT_LOG"},
        {"normalization_revision": 0},
        {"normalization_revision": True},
        {"occurred_at": "2026-01-01T00:00:00"},
        {"occurred_at": "2026-01-01T03:00:00+03:00"},
        {"part_count": -1},
    ],
)
async def test_invalid_wire_never_published(changes: dict[str, object]) -> None:
    client = MagicMock(publish=AsyncMock())
    with pytest.raises(ValueError):
        await MaintenancePublisher(client=client).publish(message=message(**changes))
    client.publish.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("equestrian_id", uuid4()),
        ("operation_id", uuid4()),
        ("message_id", uuid4()),
        ("payload_hash", "0" * 64),
        ("kind", "unknown"),
        ("part_index", 1),
    ],
)
async def test_storage_identity_mismatch(field: str, value: Any) -> None:
    client = MagicMock(publish=AsyncMock())
    saved = replace(message(), **{field: value})
    with pytest.raises(ValueError):
        await MaintenancePublisher(client=client).publish(message=saved)
    client.publish.assert_not_called()


async def test_bad_mapping_and_body_bounds() -> None:
    client = MagicMock(publish=AsyncMock())
    publisher = MaintenancePublisher(client=client)
    source = str(uuid4())
    for mappings in [
        [],
        [dict(source_page_id=source, target_page_id=source)],
        [dict(source_page_id=source, target_page_id=str(uuid4()))] * 2,
    ]:
        with pytest.raises(ValueError):
            await publisher.publish(message=message("mappings", mappings=mappings))
    with pytest.raises(ValueError, match="too large"):
        await publisher.publish(message=replace(message(), payload_json=" " * 262145))
    client.publish.assert_not_called()


def canonical_topology_mock() -> MagicMock:
    """Model owner creation and foreign lookup with real SDK info/config objects."""
    subjects = {
        "METRICA_MAINTENANCE_RESULTS": ["events.metrica.cleanup.completed", "events.metrica.pages.merge.completed"],
        "SEO_MAINTENANCE": ["commands.seo.pages.merge", "events.seo.pages.merge.finalized"],
        "YANDEX_LIFECYCLE": ["events.yandex.metrica.cleanup.requested"],
    }
    owner = "SEO_MAINTENANCE"
    streams = {
        name: StreamConfig(
            name=name,
            subjects=items,
            storage=StorageType.FILE,
            retention=RetentionPolicy.LIMITS,
            max_age=0,
            max_msgs=-1,
            max_bytes=-1,
            discard=DiscardPolicy.NEW,
            duplicate_window=120,
        )
        for name, items in subjects.items()
        if name != owner
    }
    consumers: dict[tuple[str, str], ConsumerConfig] = {}

    def stream_result(config: StreamConfig) -> StreamInfo:
        return StreamInfo(config=deepcopy(config), state=StreamState(0, 0, 0, 0, 0))

    async def stream_info(name: str) -> StreamInfo:
        if name not in streams:
            raise NotFoundError()
        return stream_result(streams[name])

    async def add_stream(*, config: StreamConfig) -> StreamInfo:
        assert config.name is not None
        streams[config.name] = deepcopy(config)
        return stream_result(config)

    def consumer_result(stream: str, durable: str, config: ConsumerConfig) -> ConsumerInfo:
        return ConsumerInfo(
            name=durable, stream_name=stream, config=deepcopy(config), created=datetime(2026, 1, 1, tzinfo=UTC)
        )

    async def consumer_info(stream: str, durable: str) -> ConsumerInfo:
        if (stream, durable) not in consumers:
            raise NotFoundError()
        return consumer_result(stream, durable, consumers[stream, durable])

    async def add_consumer(*, stream: str, config: ConsumerConfig) -> ConsumerInfo:
        assert config.durable_name is not None
        consumers[stream, config.durable_name] = deepcopy(config)
        return consumer_result(stream, config.durable_name, config)

    return MagicMock(
        stream_info=AsyncMock(side_effect=stream_info),
        add_stream=AsyncMock(side_effect=add_stream),
        consumer_info=AsyncMock(side_effect=consumer_info),
        add_consumer=AsyncMock(side_effect=add_consumer),
    )


def topology_client(**values: object) -> tuple[NatsJetstreamClient, MagicMock]:
    client = NatsJetstreamClient(NatsSettings.model_validate(values))
    client._connection = MagicMock(is_connected=True)
    js = canonical_topology_mock()
    client._jetstream = js
    return client, js


async def test_explicit_maintenance_topology_and_default_isolation() -> None:
    client, js = topology_client()
    await client.setup()
    assert js.add_stream.await_count == 1
    js.add_consumer.assert_not_called()
    js.reset_mock()
    await client.setup_maintenance()
    config = js.add_stream.await_args.kwargs["config"]
    assert config.name == "SEO_MAINTENANCE"
    assert config.subjects == ["commands.seo.pages.merge", "events.seo.pages.merge.finalized"]
    assert config.storage == StorageType.FILE and config.retention == RetentionPolicy.LIMITS
    assert (config.max_age, config.max_msgs, config.max_bytes, config.discard, config.duplicate_window) == (
        0,
        -1,
        -1,
        DiscardPolicy.NEW,
        120,
    )
    assert [c[0] for c in js.mock_calls[:3]] == ["stream_info", "add_stream", "stream_info"]
    js.update_stream.assert_not_called()
    configs = [call.kwargs["config"] for call in js.add_consumer.await_args_list]
    assert [c.durable_name for c in configs] == ["seo-metrica-pages-merge-completed", "seo-metrica-cleanup-completed"]
    assert [c.filter_subject for c in configs] == [
        "events.metrica.pages.merge.completed",
        "events.metrica.cleanup.completed",
    ]
    for call, c in zip(js.add_consumer.await_args_list, configs, strict=True):
        assert call.kwargs["stream"] == "METRICA_MAINTENANCE_RESULTS"
        assert c.ack_policy == AckPolicy.EXPLICIT and c.deliver_policy == DeliverPolicy.ALL
        assert c.ack_wait == 30 and c.max_deliver == -1


async def test_foreign_lookup_bounded_retry_and_incompatible_fail_fast() -> None:
    client, js = topology_client(NATS_SETUP_MAX_ATTEMPTS=3, NATS_SETUP_BACKOFF_SECONDS=2)
    lookup = js.stream_info.side_effect
    failures = 0

    async def delayed_foreign(name: str) -> StreamInfo:
        nonlocal failures
        if name == "METRICA_MAINTENANCE_RESULTS" and failures < 2:
            failures += 1
            raise NotFoundError()
        return await lookup(name)

    js.stream_info.side_effect = delayed_foreign
    with patch("clients.nats.client.asyncio.sleep", new_callable=AsyncMock) as sleep:
        await client.setup_maintenance()
        assert [call.args[0] for call in sleep.await_args_list] == [2, 4]
    assert js.add_consumer.await_count == 2
    assert [c[0] for c in js.mock_calls[:7]] == [
        "stream_info",
        "add_stream",
        "stream_info",
        "stream_info",
        "stream_info",
        "stream_info",
        "stream_info",
    ]
    js.reset_mock()

    async def incompatible_foreign(name: str) -> StreamInfo:
        info = await lookup(name)
        if name == "METRICA_MAINTENANCE_RESULTS":
            info.config.max_age = 60
        return info

    js.stream_info.side_effect = incompatible_foreign
    with patch("clients.nats.client.asyncio.sleep", new_callable=AsyncMock) as sleep:
        with pytest.raises(BadRequestError, match="max_age"):
            await client.setup_maintenance()
        sleep.assert_not_awaited()
    assert js.stream_info.await_count == 2
    js.consumer_info.assert_not_awaited()
    js.add_consumer.assert_not_awaited()
    js.add_stream.assert_not_awaited()
    js.update_stream.assert_not_called()
    js.reset_mock()

    async def absent_foreign(name: str) -> StreamInfo:
        if name == "METRICA_MAINTENANCE_RESULTS":
            raise NotFoundError()
        return await lookup(name)

    js.stream_info.side_effect = absent_foreign
    with patch("clients.nats.client.asyncio.sleep", new_callable=AsyncMock) as sleep:
        with pytest.raises(NotFoundError):
            await client.setup_maintenance()
        assert [call.args[0] for call in sleep.await_args_list] == [2, 4]
    assert sum(c.args == ("METRICA_MAINTENANCE_RESULTS",) for c in js.stream_info.await_args_list) == 3
    js.consumer_info.assert_not_awaited()
    js.add_consumer.assert_not_awaited()
    js.add_stream.assert_not_awaited()
    js.update_stream.assert_not_called()


class Harness:
    def __init__(self, *, attempts: int = 1) -> None:
        self.in_transaction = False
        self.events: list[str] = []
        self.saved = message()
        self.claim = OutboxClaim(self.saved, StorageLease(uuid4(), NOW + timedelta(seconds=30), attempts))
        self.repository = MagicMock(
            claim_outbox=AsyncMock(return_value=[self.claim]), finish_outbox=AsyncMock(return_value=True)
        )
        self.publisher = MagicMock(publish=AsyncMock(side_effect=self.publish))
        self.now = NOW

    @asynccontextmanager
    async def transaction(self):
        assert not self.in_transaction
        self.in_transaction = True
        self.events.append("begin")
        try:
            yield self.repository
            self.events.append("commit")
        except BaseException:
            self.events.append("rollback")
            raise
        finally:
            self.in_transaction = False

    async def publish(self, *, message: MaintenanceMessage) -> bool:
        assert not self.in_transaction
        assert self.events[-1] == "commit"
        assert message is self.saved
        self.events.append("publish")
        return False

    def relay(self) -> NormalizationOutboxRelay:
        return NormalizationOutboxRelay(
            transaction=self.transaction, publisher=self.publisher, clock=lambda: self.now, batch_size=7
        )


async def test_relay_transaction_order_and_replay_after_lost_mark() -> None:
    h = Harness()
    h.repository.finish_outbox.side_effect = OSError("DO_NOT_LOG")
    with pytest.raises(OSError):
        await h.relay().run_batch(equestrian_id=h.saved.equestrian_id)
    assert h.events == ["begin", "commit", "publish", "begin", "rollback"]
    h.repository.finish_outbox.side_effect = None
    h.events.clear()
    assert await h.relay().run_batch(equestrian_id=h.saved.equestrian_id) == 1
    assert h.events == ["begin", "commit", "publish", "begin", "commit"]
    assert h.publisher.publish.await_args_list[0] == h.publisher.publish.await_args_list[1]
    assert h.repository.claim_outbox.await_args.kwargs["limit"] == 7


@pytest.mark.parametrize("attempt,delay", [(1, 1), (2, 2), (5, 16), (7, 60), (10000, 60)])
async def test_retry_no_cap_and_safe_logging(attempt: int, delay: int, caplog: pytest.LogCaptureFixture) -> None:
    h = Harness(attempts=attempt)
    h.publisher.publish.side_effect = OSError("DO_NOT_LOG")
    with caplog.at_level(logging.WARNING):
        assert await h.relay().run_batch(equestrian_id=h.saved.equestrian_id) == 0
    args = h.repository.finish_outbox.await_args.kwargs
    assert not args["published"] and args["retry_after"] == timedelta(seconds=delay)
    assert "DO_NOT_LOG" not in caplog.text
    assert caplog.records[-1].levelno == (logging.ERROR if attempt >= 5 else logging.WARNING)


async def test_cancellation_and_claim_failure_retain_pending() -> None:
    h = Harness()
    h.publisher.publish.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await h.relay().run_batch(equestrian_id=h.saved.equestrian_id)
    h.repository.finish_outbox.assert_not_called()
    h.publisher.publish.reset_mock()
    h.repository.claim_outbox.side_effect = OSError()
    with pytest.raises(OSError):
        await h.relay().run_batch(equestrian_id=h.saved.equestrian_id)
    h.publisher.publish.assert_not_called()


async def test_expired_claim_and_stale_finish_do_not_count() -> None:
    h = Harness()
    h.now = h.claim.lease.until
    assert await h.relay().run_batch(equestrian_id=h.saved.equestrian_id) == 0
    h.publisher.publish.assert_not_called()
    h.repository.finish_outbox.assert_not_called()
    h.now = NOW
    h.repository.finish_outbox.return_value = False
    assert await h.relay().run_batch(equestrian_id=h.saved.equestrian_id) == 0
    assert h.repository.finish_outbox.await_args.kwargs["published"] is True


@pytest.mark.parametrize("size", [0, 501])
def test_relay_bounds(size: int) -> None:
    h = Harness()
    with pytest.raises(ValueError):
        NormalizationOutboxRelay(transaction=h.transaction, publisher=h.publisher, batch_size=size)


def test_di_publisher_factory_without_runtime_start() -> None:
    from containers.application import clients_container

    assert isinstance(clients_container.maintenance_publisher(), MaintenancePublisher)
