"""SEO acceptance registry (test-only, no runtime connections on import).

WIRE-02: metrica-seo-nats via scripts/qa/contracts/run.mjs; producer and consumer
must use their own .venv/bin/python + uv.lock. No broker is claimed by that test.
PG-02 / INF-01 exact node IDs below run only through scripts/qa/infra/run.py
with explicit QA_TEST_SUPPORT_ROOT and an unexpired exact permit. qa_infra_context
is mandatory even for negative fixture tests. Missing prerequisites never skip.
Root schema/stream lifecycle belongs exclusively to that runner.
"""

import asyncio
import base64
import json
import os
from contextlib import asynccontextmanager
from importlib import import_module
from types import SimpleNamespace
from uuid import uuid4

import pytest

pytestmark = pytest.mark.infrastructure


async def _produce_result(case: str) -> dict:
    # Producer-only modules must not resolve against SEO's identically named core.
    MaintenanceResultPublisher = import_module("clients.nats.maintenance_result_publisher").MaintenanceResultPublisher
    MaintenanceResultClaim = import_module("core.entities.metrica_maintenance").MaintenanceResultClaim
    schemas = import_module("core.schemas.messaging.maintenance")
    CLEANUP_COMPLETED = schemas.CLEANUP_COMPLETED
    CleanupCompleted = schemas.CleanupCompleted
    maintenance_message_id = schemas.maintenance_message_id
    NatsSettings = import_module("settings").NatsSettings

    operation = uuid4()
    identity = maintenance_message_id(CLEANUP_COMPLETED, operation, "completed")
    body = CleanupCompleted(
        schema_version=1,
        message_id=identity,
        operation_id=operation,
        equestrian_id=uuid4(),
        occurred_at="2026-01-01T00:00:00Z",
        cleanup_seq=1,
        status="completed",
    )
    settings = NatsSettings(_env_file=None)
    captured: dict = {}

    class Transport:
        async def publish(self, subject, payload, *, headers, timeout):  # noqa: ASYNC109 - production transport API
            assert subject == CLEANUP_COMPLETED and timeout == 5
            captured.update(protocol="nats", dataBase64=base64.b64encode(payload).decode(), headers=headers)
            return SimpleNamespace(stream=settings.nats_stream_maintenance_results, seq=1, duplicate=False)

    claim = MaintenanceResultClaim(
        message_id=identity,
        subject=CLEANUP_COMPLETED,
        payload=body.model_dump(mode="json"),
        owner=uuid4(),
        revision=1,
        attempts=1,
    )
    await MaintenanceResultPublisher(client=SimpleNamespace(jetstream=Transport()), settings=settings).publish_result(
        claim
    )
    if case == "missing-header":
        captured["headers"] = {}
    elif case == "wrong-header":
        captured["headers"]["Nats-Msg-Id"] = str(uuid4())
    elif case != "valid":
        payload = json.loads(base64.b64decode(captured["dataBase64"]))
        if case == "identity-mismatch":
            payload["message_id"] = str(uuid4())
            captured["headers"]["Nats-Msg-Id"] = payload["message_id"]
        else:
            payload["occurred_at"] = {
                "timestamp-invalid": "not-a-timestamp",
                "timestamp-naive": "2026-01-01T00:00:00",
                "timestamp-numeric": 1767225600,
            }[case]
        captured["dataBase64"] = base64.b64encode(json.dumps(payload).encode()).decode()
    return captured


async def _consume_result(wire: dict) -> dict:
    from clients.nats.maintenance_results import CLEANUP_SUBJECT, MaintenanceResultConsumer, MaintenanceResultHandler
    from settings import NatsSettings

    effects = []
    opened = []
    disposition = []

    class Storage:
        async def record_result(self, *, message, now):
            effects.append(message)
            return True

    @asynccontextmanager
    async def transaction():
        opened.append(True)
        yield Storage()

    class Message:
        subject = CLEANUP_SUBJECT
        data = base64.b64decode(wire["dataBase64"], validate=True)
        headers = wire["headers"]
        metadata = SimpleNamespace(num_delivered=1)

        async def ack(self):
            disposition.append("ack")

        async def term(self):
            disposition.append("term")

        async def nak(self, *, delay):
            disposition.append("nak")

    handler = MaintenanceResultHandler(transaction=transaction)  # type: ignore[arg-type]
    consumer = MaintenanceResultConsumer(
        client=None,  # type: ignore[arg-type]
        settings=NatsSettings(_env_file=None),  # type: ignore[call-arg]
        handler=handler,
        subject=CLEANUP_SUBJECT,
    )
    await consumer.process(Message())  # type: ignore[arg-type]  # transport spy
    assert len(disposition) == 1
    if disposition == ["term"]:
        assert not opened and not effects  # rejection precedes opening ANY storage transaction
    return {"accepted": disposition == ["ack"], "storageEffects": len(effects), "disposition": disposition[0]}


def wire_nats_adapter(request: dict) -> dict:
    """Real production publisher -> handler/consumer; only transport/storage are spies."""
    if request["case"] not in {
        "valid",
        "missing-header",
        "wrong-header",
        "identity-mismatch",
        "timestamp-invalid",
        "timestamp-naive",
        "timestamp-numeric",
    }:
        raise ValueError("unknown NATS case")
    result = {"schemaVersion": 1, "scenarioId": request["scenarioId"], "status": "passed"}
    if request["phase"] == "producer":
        result["wire"] = asyncio.run(_produce_result(request["case"]))
    elif request["phase"] == "consumer":
        result["observed"] = asyncio.run(_consume_result(request["wire"]))
    else:
        raise ValueError("unknown phase")
    return result


async def test_pg_search_path_and_failure_rollback(empty_pg_engine, qa_infra_context):
    """PG-02: exact schema only; a failed fixture body rolls back its child table.

    This does NOT claim the public-shadow scenario: creating a public table is
    outside the schema-only permit. That requires a separately provisioned fixture.
    """
    from sqlalchemy import text

    schema = qa_infra_context["resource"]["schema"]
    with pytest.raises(RuntimeError, match="synthetic fixture failure"):
        async with empty_pg_engine.begin() as connection:
            assert await connection.scalar(text("SHOW search_path")) == schema
            assert await connection.scalar(text("SELECT current_schema()")) == schema
            await connection.execute(text("CREATE TABLE fixture_failure_probe (id integer)"))
            raise RuntimeError("synthetic fixture failure")
    # Independent connection after rollback, not a mock/identity-map observation.
    async with empty_pg_engine.connect() as connection:
        assert await connection.scalar(text("SELECT to_regclass('fixture_failure_probe')")) is None
        assert await connection.scalar(text("SELECT current_schema()")) == schema


@pytest.mark.parametrize("invalid", ["unsafe-target", "missing-permit", "not-owned"])
def test_fixture_context_fails_closed(qa_infra_context, monkeypatch, invalid):
    """INF-01: fixture bootstrap rejects altered inputs before any SQL connection."""
    from copy import deepcopy

    infra = import_module("infra")
    GuardError = import_module("infra.contracts").GuardError

    manifest = deepcopy(qa_infra_context["manifest"])
    original_load = infra.load

    def load(path):
        if path == infra.os.environ["QA_INFRA_MANIFEST"]:
            value = deepcopy(manifest)
            if invalid == "unsafe-target":
                value["target"]["database"] = "product"
            return value
        if invalid == "missing-permit":
            raise GuardError("missing-permit")
        return original_load(path)

    monkeypatch.setattr(infra, "load", load)
    if invalid == "not-owned":
        monkeypatch.setenv("QA_INFRA_ACTIVE", "not-this-run")
    with pytest.raises(GuardError):
        infra.load_context("pg")


async def test_public_shadow_missing_local_table_fails_closed(empty_pg_engine, qa_infra_context):
    """PG-02 prerequisite: operator-provisioned public.qa_seo_public_shadow(id int).

    The schema-only permit cannot authorize CREATE/DROP in public. The operator
    must provision that synthetic table in the dedicated test DB, grant SELECT,
    and leave it for QG. This test only reads it; no public cleanup is attempted.
    Run this exact node ID separately if that prerequisite is not yet provisioned.
    """
    from pathlib import Path

    from sqlalchemy import text
    from sqlalchemy.exc import ProgrammingError

    async with empty_pg_engine.connect() as connection:
        exists = await connection.scalar(text("SELECT to_regclass('public.qa_seo_public_shadow') IS NOT NULL"))
        # Safe prerequisite fact only; no URL, SQL values or exceptions in evidence.
        evidence = Path(os.environ["QA_INFRA_RESULT"]).parent / "public-shadow-prerequisite.json"
        evidence.write_text(json.dumps({"available": bool(exists), "scope": "dedicated-test-db-read-only"}))
        assert exists, "Operator-provisioned public shadow table required; schema permit forbids creating it"
        await connection.execute(text("SELECT id FROM public.qa_seo_public_shadow LIMIT 0"))
        assert await connection.scalar(text("SHOW search_path")) == qa_infra_context["resource"]["schema"]
        assert await connection.scalar(text("SELECT to_regclass('qa_seo_public_shadow')")) is None
        with pytest.raises(ProgrammingError) as rejected:
            await connection.execute(text("SELECT id FROM qa_seo_public_shadow"))
        assert getattr(rejected.value.orig, "sqlstate", None) == "42P01"
    # Root cleanup remains runner-owned even when the SELECT failed.
