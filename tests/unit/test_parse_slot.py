from datetime import UTC, date, datetime, time, timedelta, timezone
from uuid import uuid4

import pytest

from core.services.parsing import EnvTimezoneProvider, SlotCalculator


@pytest.mark.parametrize(
    ("offset", "now", "expected"),
    [
        (3, "2026-10-28T23:59:00", None),
        (3, "2026-10-29T00:00:00", date(2026, 10, 29)),
        (3, "2026-10-29T20:59:59", date(2026, 10, 29)),
        (3, "2026-10-29T21:00:00", None),
        (-12, "2026-10-29T11:59:59", date(2026, 10, 28)),
        (-12, "2026-10-29T12:00:00", None),
        (-12, "2026-10-29T15:00:00", date(2026, 10, 29)),
        (14, "2026-10-28T10:00:00", None),
        (14, "2026-10-28T13:00:00", date(2026, 10, 29)),
    ],
)
def test_due_slot_uses_only_current_local_date(offset: int, now: str, expected: date | None) -> None:
    calculator = SlotCalculator(timezone_provider=EnvTimezoneProvider(utc_offset_hours=offset), schedule_time=time(3))
    moment = datetime.fromisoformat(now).replace(tzinfo=UTC)
    assert calculator.due_slot(utc_now=moment, equestrian_id=uuid4()) == expected
    assert calculator.due_slot(utc_now=moment.astimezone(timezone(timedelta(hours=5)))) == expected
    assert calculator.local_now(utc_now=moment).utcoffset() == timedelta(hours=offset)


@pytest.mark.parametrize("offset", [-13, 15, True, 1.5])
def test_invalid_offsets(offset: int) -> None:
    with pytest.raises(ValueError):
        EnvTimezoneProvider(utc_offset_hours=offset)


def test_naive_clock_and_aware_schedule_rejected() -> None:
    provider = EnvTimezoneProvider(utc_offset_hours=0)
    with pytest.raises(ValueError):
        SlotCalculator(timezone_provider=provider, schedule_time=time(3, tzinfo=UTC))
    with pytest.raises(ValueError):
        SlotCalculator(timezone_provider=provider, schedule_time=time(3)).due_slot(utc_now=datetime(2026, 10, 29))


@pytest.mark.parametrize("offset", [-12, 3, 14])
def test_slot_expiration_uses_local_midnight_not_utc(offset: int) -> None:
    calculator = SlotCalculator(timezone_provider=EnvTimezoneProvider(utc_offset_hours=offset), schedule_time=time(3))
    before = datetime(2026, 10, 29, 23, 59, 59, tzinfo=UTC) - timedelta(hours=offset)
    assert calculator.is_current_slot(local_date=date(2026, 10, 29), utc_now=before)
    assert not calculator.is_current_slot(local_date=date(2026, 10, 29), utc_now=before + timedelta(seconds=1))
    assert not calculator.is_current_slot(local_date=date(2026, 10, 30), utc_now=before)
