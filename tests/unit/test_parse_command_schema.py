from datetime import UTC, date, datetime
from uuid import NAMESPACE_DNS, uuid4, uuid5

import pytest
from pydantic import ValidationError

from core.schemas.messaging import NAMESPACE_PARSE_COMMANDS, ParseCommand, ParseWindow, msg_id, schedule_job_id


def command() -> ParseCommand:
    return ParseCommand(
        occurred_at=datetime.now(UTC),
        job_id=uuid4(),
        provider="ya_metrica",
        equestrian_id=uuid4(),
        trigger="manual",
    )


def test_ids_ut_ps_06() -> None:
    assert NAMESPACE_PARSE_COMMANDS == uuid5(NAMESPACE_DNS, "parse-commands.eqcms")
    tenant, day = uuid4(), date(2026, 10, 28)
    job = schedule_job_id("ya_metrica", tenant, day)
    assert job == uuid5(NAMESPACE_PARSE_COMMANDS, f"schedule:ya_metrica:{tenant}:2026-10-28")
    assert job == schedule_job_id("ya_metrica", tenant, day)
    jobs = {
        job,
        schedule_job_id("demo", tenant, day),
        schedule_job_id("ya_metrica", uuid4(), day),
        schedule_job_id("ya_metrica", tenant, date(2026, 10, 29)),
        uuid4(),
        uuid4(),
    }
    assert len(jobs) == 6
    assert msg_id("ya_metrica", job) == uuid5(NAMESPACE_PARSE_COMMANDS, f"ya_metrica:{job}")
    assert msg_id("demo", job) != msg_id("ya_metrica", job)


def test_envelope_roundtrip_ut_ps_06() -> None:
    value = command()
    assert ParseCommand.model_validate_json(value.model_dump_json(exclude_none=True)) == value
    assert value.model_dump(exclude_none=True)["window"] == {"mode": "auto"}
    assert value.params == {}
    explicit = ParseWindow(mode="explicit", date_from=date(2026, 1, 1), date_to=date(2026, 1, 2))
    assert explicit.date_from == date(2026, 1, 1)
    # Provider params belong to consumers, not a provider-specific publisher.
    assert command().model_copy(update={"provider": "demo", "params": {"option": True}}).params == {"option": True}


@pytest.mark.parametrize(
    "window",
    [
        {"mode": "unknown"},
        {"mode": "auto", "date_from": "2026-01-01"},
        {"mode": "auto", "date_from": None},
        {"mode": "explicit"},
        {"mode": "explicit", "date_from": "2026-01-02", "date_to": "2026-01-01"},
        {"mode": "auto", "other": 1},
    ],
)
def test_invalid_window_ut_ps_06(window: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ParseWindow.model_validate(window)


@pytest.mark.parametrize(
    "patch",
    [
        {"schema_version": 2},
        {"provider": "Bad-Code"},
        {"trigger": "other"},
        {"occurred_at": "2026-01-01T00:00:00"},
        {"extra": "forbidden"},
        {"params": []},
    ],
)
def test_invalid_envelope_ut_ps_06(patch: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ParseCommand.model_validate({**command().model_dump(exclude_none=True), **patch})
