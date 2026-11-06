"""LIVE-C01: pair the real producer serializer with SEO's strict consumer."""

import asyncio
import importlib.util
import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

# The registry imports this module in two different service interpreters.
# Never import SEO's core/clients into the producer interpreter.
if os.environ.get("EQCMS_WIRE_PHASE") != "producer":
    from clients.yandex_service import YandexServiceClient
    from core.exceptions import ParseTargetsUnavailableError
    from core.schemas.clients.yandex_service import ConnectedMetricaCounter
    from tests.yandex_contract import connected_counter_payload


def _counter_payload(*, tenant, present):
    path = Path(__file__).resolve().parents[1] / "yandex_contract.py"
    spec = importlib.util.spec_from_file_location("seo_wire_fixture", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.connected_counter_payload(tenant=tenant, present=present)


LIFECYCLE_FIELDS = ("binding_generation", "cleanup_requested_seq", "cleanup_completed_seq")


def serialize_producer(payload: dict) -> dict:
    producer_repo = Path(__file__).resolve().parents[3] / "yandex-service"
    producer_src = producer_repo / "src"
    python = producer_repo / ".venv/bin/python"
    if not producer_src.is_dir() or not python.is_file() or not (producer_repo / "uv.lock").is_file():
        if os.environ.get("EQCMS_MONOREPO") == "1":
            pytest.fail("Strict paired contract requires Yandex checkout and lockfile environment")
        pytest.skip("Standalone: paired Yandex checkout and lockfile environment unavailable")
    # Separate interpreter prevents collision of both services' `core` packages.
    result = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            "import sys; sys.path.insert(0, sys.argv[1]); "
            "from core.schemas.api.yandex_metrica_counters import YandexMetricaConnectedCounterOutDto as D; "
            "print(D.model_validate_json(sys.stdin.read()).model_dump_json())",
            str(producer_src),
        ],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=True,
        timeout=15,
        cwd=producer_repo,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize("present", [True, False])
async def test_real_producer_serialization_is_consumable(present: bool) -> None:
    tenant = uuid4()
    payload = connected_counter_payload(tenant=tenant, present=present)
    wire = await asyncio.to_thread(serialize_producer, payload)
    assert wire == payload
    snapshot = ConnectedMetricaCounter.model_validate(wire)
    assert [getattr(snapshot, field) for field in LIFECYCLE_FIELDS] == [3, 2, 1]
    client = YandexServiceClient(
        base_url="http://peer", transport=httpx.MockTransport(lambda _: httpx.Response(200, json=wire))
    )
    binding = await client.get_metrica_binding(equestrian_id=tenant)
    if present:
        assert binding is not None
        assert binding.model_dump() == {"equestrian_id": tenant, "counter_id": "123"}
        assert snapshot.counter is not None
        assert "yandex_account" not in snapshot.counter.model_dump()
    else:
        assert binding is None


@pytest.mark.parametrize("present", [True, False])
@pytest.mark.parametrize("field", LIFECYCLE_FIELDS)
@pytest.mark.parametrize("bad", [-1, True, 1.5, "3", None, "missing"])
async def test_malformed_lifecycle_fails_closed(present: bool, field: str, bad: object) -> None:
    tenant = uuid4()
    payload = connected_counter_payload(tenant=tenant, present=present)
    if bad == "missing":
        del payload[field]
    else:
        payload[field] = bad
    with pytest.raises(ValidationError):
        ConnectedMetricaCounter.model_validate(payload)
    client = YandexServiceClient(
        base_url="http://peer", transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    )
    with pytest.raises(ParseTargetsUnavailableError):
        await client.get_metrica_binding(equestrian_id=tenant)


@pytest.mark.parametrize("present", [True, False])
async def test_unknown_top_level_field_still_rejected(present: bool) -> None:
    payload = connected_counter_payload(tenant=uuid4(), present=present)
    payload["unexpected"] = "not allowed"
    with pytest.raises(ValidationError, match="extra_forbidden"):
        ConnectedMetricaCounter.model_validate(payload)


def test_initial_null_binding_lifecycle() -> None:
    payload = connected_counter_payload(tenant=uuid4(), present=False)
    payload.update(dict.fromkeys(LIFECYCLE_FIELDS, 0))
    assert ConnectedMetricaCounter.model_validate(payload).counter is None


def wire_http_adapter(request: dict) -> dict:
    """WIRE-01: production Yandex DTO JSON -> production SEO HTTP parser.

    Registry: yandex-seo-http. Runner supplies separate lockfile interpreters.
    Cases: present/absent and lifecycle-missing/negative/bool/float/string/null.
    """
    case = request["case"]
    invalid = {
        "lifecycle-missing": "missing",
        "lifecycle-negative": -1,
        "lifecycle-bool": True,
        "lifecycle-float": 1.5,
        "lifecycle-string": "3",
        "lifecycle-null": None,
    }
    if case not in {"present", "absent", *invalid}:
        raise ValueError("unknown HTTP case")
    result = {"schemaVersion": 1, "scenarioId": request["scenarioId"], "status": "passed"}
    if request["phase"] == "producer":
        # Producer-only DTO is loaded in Yandex's interpreter, not SEO's type graph.
        YandexMetricaConnectedCounterOutDto = importlib.import_module(
            "core.schemas.api.yandex_metrica_counters"
        ).YandexMetricaConnectedCounterOutDto

        payload = _counter_payload(tenant=uuid4(), present=case != "absent")
        body = YandexMetricaConnectedCounterOutDto.model_validate(payload).model_dump_json()
        if case in invalid:
            payload = json.loads(body)
            if case == "lifecycle-missing":
                del payload["binding_generation"]
            else:
                payload["binding_generation"] = invalid[case]
            body = json.dumps(payload)
        result["wire"] = {"protocol": "http", "body": body}
    elif request["phase"] == "consumer":
        from uuid import UUID

        payload = json.loads(request["wire"]["body"])
        tenant = UUID(payload["counter"]["equestrian_id"]) if payload["counter"] else uuid4()

        async def consume():
            client = YandexServiceClient(
                base_url="http://synthetic.invalid",
                transport=httpx.MockTransport(lambda _: httpx.Response(200, content=request["wire"]["body"])),
            )
            try:
                binding = await client.get_metrica_binding(equestrian_id=tenant)
            except ParseTargetsUnavailableError:
                return False
            assert (binding is None) == (payload["counter"] is None)
            return True

        accepted = asyncio.run(consume())
        result["observed"] = {
            "accepted": accepted,
            "storageEffects": 0,
            "disposition": "accepted" if accepted else "rejected",
        }
    else:
        raise ValueError("unknown phase")
    return result


def wire_nats_adapter(request: dict) -> dict:
    """Registry-compatible entrypoint; implementation owned by integration slice."""
    path = Path(__file__).resolve().parents[1] / "integration/test_acceptance_boundaries.py"
    spec = importlib.util.spec_from_file_location("seo_nats_boundary", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.wire_nats_adapter(request)
