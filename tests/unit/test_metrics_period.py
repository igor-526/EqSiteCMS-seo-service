from datetime import UTC, date, datetime, timedelta

import pytest

from core.exceptions import ClientError
from core.services.metrics_period import MetricsPeriodResolver
from core.services.metrics_table.registry import MetricRegistry

TODAY = date(2026, 10, 26)


def resolver(today: date = TODAY, offset: int = 3) -> MetricsPeriodResolver:
    return MetricsPeriodResolver(
        utc_offset_hours=offset, clock=lambda: datetime.combine(today, datetime.min.time(), tzinfo=UTC)
    )


@pytest.mark.parametrize(
    ("mode", "start", "count"),
    [("day", "2026-10-20", 7), ("week", "2026-10-05", 4), ("month", "2026-05-01", 6), ("year", "2025-01-01", 2)],
)
def test_defaults_ut_sp_01(mode: str, start: str, count: int) -> None:
    result = resolver().resolve(mode=mode)
    assert result.period.date_from.isoformat() == start
    assert result.period.date_to == TODAY
    assert result.period.defaulted
    assert not result.period.adjusted and not result.period.clamped
    assert result.period.requested_date_from is None and result.period.requested_date_to is None
    assert len(result.columns) == count
    assert MetricRegistry().get().key == "visits"


@pytest.mark.parametrize(("offset", "today"), [(3, date(2026, 10, 27)), (-12, TODAY)])
def test_timezone_ut_sp_01(offset: int, today: date) -> None:
    result = MetricsPeriodResolver(
        utc_offset_hours=offset, clock=lambda: datetime(2026, 10, 26, 22, 30, tzinfo=UTC)
    ).resolve()
    assert result.period.today == today
    assert result.period.date_from == today - timedelta(days=6)


@pytest.mark.parametrize(
    ("mode", "start", "end", "expected_start", "expected_end"),
    [
        ("week", "2026-10-08", "2026-10-20", "2026-10-05", "2026-10-25"),
        ("month", "2026-08-10", "2026-09-02", "2026-08-01", "2026-09-30"),
        ("year", "2024-03-15", "2025-06-01", "2024-01-01", "2025-12-31"),
    ],
)
def test_alignment_ut_sp_02(mode: str, start: str, end: str, expected_start: str, expected_end: str) -> None:
    period = resolver().resolve(mode=mode, date_from=date.fromisoformat(start), date_to=date.fromisoformat(end)).period
    assert (period.date_from.isoformat(), period.date_to.isoformat()) == (expected_start, expected_end)
    assert period.adjusted and not period.clamped


@pytest.mark.parametrize(
    ("mode", "start", "end", "expected_start"),
    [
        ("day", "2026-10-27", "2026-10-30", "2026-10-26"),
        ("day", "2026-10-20", "2026-11-05", "2026-10-20"),
        ("month", "2026-08-10", "2026-10-26", "2026-08-01"),
        ("week", "2026-10-27", "2026-11-01", "2026-10-26"),
        ("year", "2026-12-01", "2026-12-31", "2026-01-01"),
    ],
)
def test_today_ut_sp_03(mode: str, start: str, end: str, expected_start: str) -> None:
    result = resolver().resolve(mode=mode, date_from=date.fromisoformat(start), date_to=date.fromisoformat(end))
    assert result.period.date_from.isoformat() == expected_start
    assert result.period.date_to == TODAY
    assert result.period.adjusted and not result.period.clamped
    assert result.columns[-1].date_to == TODAY


def test_default_current_week_ut_sp_03() -> None:
    result = resolver(date(2026, 10, 28)).resolve(mode="week")
    assert result.columns[-1].date_from == TODAY
    assert result.columns[-1].date_to == date(2026, 10, 28)


@pytest.mark.parametrize(
    ("mode", "start", "limit_end", "next_end", "count"),
    [
        ("day", "2024-02-01", "2024-03-02", "2024-03-03", 31),
        ("week", "2024-01-01", "2024-04-21", "2024-04-28", 16),
        ("month", "2024-03-01", "2025-02-28", "2025-03-31", 12),
        ("year", "2020-01-01", "2024-12-31", "2025-12-31", 5),
    ],
)
def test_limit_ut_sp_04(mode: str, start: str, limit_end: str, next_end: str, count: int) -> None:
    for end, clamped in [(limit_end, False), (next_end, True)]:
        result = resolver().resolve(mode=mode, date_from=date.fromisoformat(start), date_to=date.fromisoformat(end))
        assert result.period.date_to.isoformat() == limit_end
        assert result.period.clamped is clamped
        assert not result.period.adjusted  # Pure limit clipping is not calendar adjustment.
        assert result.period.requested_date_from == date.fromisoformat(start)
        assert result.period.requested_date_to == date.fromisoformat(end)
        assert len(result.columns) == count


def test_leap_month_ut_sp_04() -> None:
    result = resolver().resolve(mode="month", date_from=date(2024, 2, 29), date_to=date(2024, 2, 29))
    assert result.columns[0].date_from == date(2024, 2, 1)
    assert result.columns[0].date_to == date(2024, 2, 29)


def test_limit_before_today_ut_sp_04() -> None:
    result = resolver().resolve(date_from=date(2026, 10, 1), date_to=date(2027, 1, 1))
    assert result.period.date_to == TODAY
    assert result.period.clamped and result.period.adjusted
    result = resolver().resolve(date_from=date(2026, 9, 1), date_to=TODAY)
    assert result.period.date_to == date(2026, 10, 1)
    assert result.period.clamped and not result.period.adjusted


@pytest.mark.parametrize("case", ["from", "to", "reversed", "mode", "metric"])
def test_validation_ut_sp_05(case: str) -> None:
    with pytest.raises(ClientError) as error:
        if case == "from":
            resolver().resolve(date_from=TODAY)
        elif case == "to":
            resolver().resolve(date_to=TODAY)
        elif case == "reversed":
            resolver().resolve(date_from=TODAY, date_to=date(2026, 10, 1))
        elif case == "mode":
            resolver().resolve(mode="quarter")
        else:
            MetricRegistry().get("unknown")
    assert error.value.status_code == 400


@pytest.mark.parametrize("mode", ["day", "week", "month", "year"])
def test_columns_ut_sp_05(mode: str) -> None:
    result = resolver().resolve(mode=mode, date_from=date(2020, 1, 1), date_to=TODAY)
    assert 1 <= len(result.columns) <= 31
    assert result.columns[0].date_from == result.period.date_from
    assert result.columns[-1].date_to == result.period.date_to
    for index, column in enumerate(result.columns):
        assert column.key == column.date_from.isoformat()
        assert column.date_from <= column.date_to <= TODAY
        if index:
            assert column.date_from == result.columns[index - 1].date_to + timedelta(days=1)


def test_unchanged_period_ut_sp_05() -> None:
    period = resolver().resolve(date_from=date(2026, 10, 10), date_to=date(2026, 10, 20)).period
    assert not period.defaulted and not period.adjusted and not period.clamped
