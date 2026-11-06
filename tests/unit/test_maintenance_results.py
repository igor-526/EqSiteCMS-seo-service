"""Transport commit/ACK ordering, recovery and safe diagnostics (not broker acceptance)."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from nats.errors import TimeoutError as NatsTimeoutError

from clients.nats.maintenance_results import (
    CLEANUP_SUBJECT,
    MERGE_SUBJECT,
    InvalidMaintenanceResult,
    MaintenanceResultConsumer,
    MaintenanceResultHandler,
    decode_result,
)
from settings import NatsSettings


@pytest.fixture(autouse=True)
def disable_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(NatsSettings.model_config, "env_file", None)


def payload(subject=MERGE_SUBJECT, **updates):
    data = dict(
        schema_version=1,
        operation_id=str(uuid4()),
        equestrian_id=str(uuid4()),
        occurred_at="2026-01-01T00:00:00Z",
        status="completed",
    )
    data.update(dict(cleanup_seq=1) if subject == CLEANUP_SUBJECT else dict(normalization_revision=1, part_count=0))
    data.update(updates)
    data.setdefault(
        "message_id",
        str(uuid5(NAMESPACE_URL, f"eqcms:maintenance:v1:{subject}:{data['operation_id']}:{data['status']}")),
    )
    return json.dumps(data, indent=2).encode()


def harness(*, subject=MERGE_SUBJECT, error=None, commit_error=None, duplicate=False, attempt=1):
    events = []
    repo = AsyncMock()

    async def record(**kwargs):
        events.append("record")
        if error is not None:
            raise error
        return not duplicate

    repo.record_result.side_effect = record

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield repo
            if commit_error is not None:
                raise commit_error
            events.append("commit")
        except BaseException:
            events.append("rollback")
            raise

    handler = MaintenanceResultHandler(transaction=transaction)
    client = MagicMock()
    settings = NatsSettings()
    consumer = MaintenanceResultConsumer(client=client, settings=settings, handler=handler, subject=subject)
    msg = MagicMock(subject=subject, data=payload(subject))
    msg.headers = {"Nats-Msg-Id": json.loads(msg.data)["message_id"]}
    msg.metadata = SimpleNamespace(num_delivered=attempt)
    msg.ack = AsyncMock(side_effect=lambda: events.append("ack"))
    msg.term = AsyncMock(side_effect=lambda: events.append("term"))
    msg.nak = AsyncMock(side_effect=lambda **kw: events.append("nak"))
    return consumer, msg, events, repo, client


@pytest.mark.parametrize("subject", [MERGE_SUBJECT, CLEANUP_SUBJECT])
@pytest.mark.parametrize("duplicate", [False, True])
async def test_commit_before_ack_including_duplicate(subject, duplicate):
    consumer, msg, events, repo, _ = harness(subject=subject, duplicate=duplicate)
    await consumer.process(msg)
    assert events == ["begin", "record", "commit", "ack"]
    saved = repo.record_result.call_args.kwargs["message"]
    assert saved.payload_json.encode() == msg.data
    canonical = json.dumps(json.loads(msg.data), sort_keys=True, separators=(",", ":")).encode()
    assert saved.payload_hash == sha256(canonical).hexdigest()
    assert saved.kind == ("cleanup_completed" if subject == CLEANUP_SUBJECT else "merge_completed")
    msg.nak.assert_not_awaited()
    msg.term.assert_not_awaited()


@pytest.mark.parametrize("error", [ValueError("secret foreign tenant"), InvalidMaintenanceResult("secret")])
async def test_permanent_correlation_failure_rolls_back_then_term(error, caplog):
    consumer, msg, events, _, _ = harness(error=error)
    with caplog.at_level(logging.WARNING):
        await consumer.process(msg)
    assert events == ["begin", "record", "rollback", "term"]
    assert "secret" not in caplog.text
    msg.ack.assert_not_awaited()


@pytest.mark.parametrize("attempt,delay", [(1, 1), (2, 2), (5, 16), (7, 60), (100, 60)])
async def test_temporary_failure_naks_with_bounded_backoff(attempt, delay, caplog):
    consumer, msg, events, _, _ = harness(error=RuntimeError("secret database URI"), attempt=attempt)
    with caplog.at_level(logging.WARNING):
        await consumer.process(msg)
    assert events == ["begin", "record", "rollback", "nak"]
    msg.nak.assert_awaited_once_with(delay=delay)
    assert "secret" not in caplog.text
    assert any(record.levelno == (logging.ERROR if attempt >= 5 else logging.WARNING) for record in caplog.records)


@pytest.mark.parametrize("error", [RuntimeError("commit"), ValueError("commit")])
async def test_commit_failure_is_never_ack_or_permanent_reject(error):
    consumer, msg, events, _, _ = harness(commit_error=error)
    await consumer.process(msg)
    assert events == ["begin", "record", "rollback", "nak"]


async def test_cancellation_rolls_back_without_ack_nak_or_term():
    consumer, msg, events, _, _ = harness(error=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await consumer.process(msg)
    assert events == ["begin", "record", "rollback"]


@pytest.mark.parametrize(
    "body",
    [b"not json secret", b"\xff", b"[]", b"{}", b"x" * 262145, b'{"schema_version":true}'],
)
async def test_invalid_body_never_opens_transaction(body, caplog):
    consumer, msg, events, _, _ = harness()
    msg.data = body
    await consumer.process(msg)
    assert events == ["term"]
    assert "secret" not in caplog.text


@pytest.mark.parametrize(
    "updates",
    [dict(token="secret"), dict(schema_version=2), dict(part_count=-1), dict(normalization_revision=True)],
)
async def test_invalid_schema_rejected_before_transaction(updates):
    consumer, msg, events, _, _ = harness()
    msg.data = payload(**updates)
    await consumer.process(msg)
    assert events == ["term"]


async def test_wrong_subject_is_not_dispatched():
    consumer, msg, events, _, _ = harness()
    msg.subject = CLEANUP_SUBJECT
    await consumer.process(msg)
    assert events == ["term"]
    with pytest.raises(InvalidMaintenanceResult, match="^invalid maintenance result$"):
        decode_result(subject="other", payload=msg.data, headers=msg.headers)


async def test_ack_transport_failure_propagates_for_supervisor():
    consumer, msg, events, _, _ = harness()
    msg.ack.side_effect = RuntimeError("ack lost")
    with pytest.raises(RuntimeError, match="ack lost"):
        await consumer.process(msg)
    assert events == ["begin", "record", "commit"]
    msg.nak.assert_not_awaited()
    msg.term.assert_not_awaited()


async def test_restart_binds_existing_durable_and_does_not_create_streams():
    consumer, msg, _, _, client = harness()
    sub = AsyncMock()
    sub.fetch.side_effect = [NatsTimeoutError(), [msg], asyncio.CancelledError()]
    client.jetstream.pull_subscribe_bind = AsyncMock(return_value=sub)
    with pytest.raises(asyncio.CancelledError):
        await consumer.run()
    client.jetstream.pull_subscribe_bind.assert_awaited_once_with(
        durable="seo-metrica-pages-merge-completed", stream="METRICA_MAINTENANCE_RESULTS"
    )
    sub.unsubscribe.assert_awaited_once()
    msg.ack.assert_awaited_once()
    client.setup_maintenance.assert_not_called()
    client.jetstream.add_stream.assert_not_called()


async def test_fetch_failure_propagates_and_unsubscribes():
    consumer, _, _, _, client = harness()
    sub = AsyncMock()
    sub.fetch.side_effect = RuntimeError("fetch")
    client.jetstream.pull_subscribe_bind = AsyncMock(return_value=sub)
    with pytest.raises(RuntimeError, match="fetch"):
        await consumer.run()
    sub.unsubscribe.assert_awaited_once()


RESULT_VARIANTS = [
    (CLEANUP_SUBJECT, "completed"),
    (MERGE_SUBJECT, "completed"),
    (MERGE_SUBJECT, "superseded_by_delete"),
]


def result_payload(subject, status):
    updates = dict(status=status)
    if status == "superseded_by_delete":
        updates.update(cleanup_operation_id=str(uuid4()), cleanup_seq=1, part_count=None)
    return payload(subject, **updates)


@pytest.mark.parametrize("subject,status", RESULT_VARIANTS)
async def test_correlated_result_identity_is_accepted(subject, status):
    consumer, msg, events, repo, _ = harness(subject=subject)
    msg.data = result_payload(subject, status)
    msg.headers = {"Nats-Msg-Id": json.loads(msg.data)["message_id"]}
    await consumer.process(msg)
    assert events == ["begin", "record", "commit", "ack"]
    assert repo.record_result.call_args.kwargs["message"].message_id == uuid5(
        NAMESPACE_URL,
        f"eqcms:maintenance:v1:{subject}:{json.loads(msg.data)['operation_id']}:{status}",
    )
    msg.term.assert_not_awaited()
    msg.nak.assert_not_awaited()


@pytest.mark.parametrize("subject,status", RESULT_VARIANTS)
@pytest.mark.parametrize(
    "invalid",
    [
        "missing",
        "empty",
        "malformed_header",
        "mismatch",
        "wrong_uuid5",
        "wrong_subject",
        "wrong_suffix",
        "corrupt_body",
    ],
)
async def test_invalid_identity_terms_before_storage_and_does_not_poison_valid_result(
    subject, status, invalid, caplog
):
    consumer, msg, events, repo, _ = harness(subject=subject)
    valid = result_payload(subject, status)
    body = json.loads(valid)
    identity = body["message_id"]
    headers = {"Nats-Msg-Id": identity}
    if invalid == "missing":
        headers = None
    elif invalid == "empty":
        headers = {}
    elif invalid == "malformed_header":
        headers["Nats-Msg-Id"] = "secret invalid identity"
    elif invalid == "mismatch":
        headers["Nats-Msg-Id"] = str(uuid4())
    else:
        if invalid == "wrong_uuid5":
            body["message_id"] = str(uuid4())
        elif invalid == "wrong_subject":
            other = MERGE_SUBJECT if subject == CLEANUP_SUBJECT else CLEANUP_SUBJECT
            body["message_id"] = str(
                uuid5(NAMESPACE_URL, f"eqcms:maintenance:v1:{other}:{body['operation_id']}:{status}")
            )
        elif invalid == "wrong_suffix":
            suffix = "superseded_by_delete" if status == "completed" else "completed"
            body["message_id"] = str(
                uuid5(NAMESPACE_URL, f"eqcms:maintenance:v1:{subject}:{body['operation_id']}:{suffix}")
            )
        else:
            body["message_id"] = "secret corrupt identity"
        headers["Nats-Msg-Id"] = body["message_id"]
    msg.data = json.dumps(body).encode()
    msg.headers = headers
    with caplog.at_level(logging.WARNING):
        await consumer.process(msg)
    assert events == ["term"]  # No transaction, record, commit or ACK.
    repo.record_result.assert_not_awaited()
    msg.term.assert_awaited_once()
    msg.ack.assert_not_awaited()
    msg.nak.assert_not_awaited()
    assert "secret" not in caplog.text

    msg.data = valid
    msg.headers = {"Nats-Msg-Id": identity}
    await consumer.process(msg)
    assert events == ["term", "begin", "record", "commit", "ack"]
    repo.record_result.assert_awaited_once()
    msg.ack.assert_awaited_once()
    msg.nak.assert_not_awaited()


@pytest.mark.parametrize("subject,status", RESULT_VARIANTS)
@pytest.mark.parametrize("occurred_at", [1767225600, 1767225600.0, "1767225600", "2026-01-01 00:00:00Z", None])
async def test_non_rfc3339_timestamp_terms_before_storage(subject, status, occurred_at):
    consumer, msg, events, repo, _ = harness(subject=subject)
    body = json.loads(result_payload(subject, status))
    body["occurred_at"] = occurred_at
    msg.data = json.dumps(body).encode()
    msg.headers = {"Nats-Msg-Id": body["message_id"]}  # Valid identity isolates timestamp validation.
    await consumer.process(msg)
    assert events == ["term"]
    repo.record_result.assert_not_awaited()
    msg.ack.assert_not_awaited()
    msg.nak.assert_not_awaited()
