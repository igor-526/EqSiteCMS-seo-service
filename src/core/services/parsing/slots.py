from datetime import UTC, date, datetime, time, timedelta, timezone
from typing import Protocol
from uuid import UUID


class TimezoneProvider(Protocol):
    def utc_offset_hours(self, equestrian_id: UUID | None = None) -> int: ...


class EnvTimezoneProvider:
    """One configured offset for all tenants, injected from SEO_UTC_OFFSET_HOURS."""

    def __init__(self, *, utc_offset_hours: int) -> None:
        if type(utc_offset_hours) is not int or not -12 <= utc_offset_hours <= 14:
            raise ValueError("Invalid UTC offset")
        self._offset = utc_offset_hours

    def utc_offset_hours(self, equestrian_id: UUID | None = None) -> int:
        return self._offset


class SlotCalculator:
    def __init__(self, *, timezone_provider: TimezoneProvider, schedule_time: time) -> None:
        if schedule_time.tzinfo is not None:
            raise ValueError("Schedule time must be local")
        self._timezone_provider = timezone_provider
        self._schedule_time = schedule_time

    def local_now(self, *, utc_now: datetime, equestrian_id: UUID | None = None) -> datetime:
        if utc_now.tzinfo is None or utc_now.utcoffset() is None:
            raise ValueError("Current time must be timezone-aware")
        offset = self._timezone_provider.utc_offset_hours(equestrian_id)
        if type(offset) is not int or not -12 <= offset <= 14:
            raise ValueError("Invalid UTC offset")
        return utc_now.astimezone(UTC).astimezone(timezone(timedelta(hours=offset)))

    def due_slot(self, *, utc_now: datetime, equestrian_id: UUID | None = None) -> date | None:
        local = self.local_now(utc_now=utc_now, equestrian_id=equestrian_id)
        return local.date() if local.time() >= self._schedule_time else None

    def is_current_slot(self, *, local_date: date, utc_now: datetime, equestrian_id: UUID | None = None) -> bool:
        return self.local_now(utc_now=utc_now, equestrian_id=equestrian_id).date() == local_date
