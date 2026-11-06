"""Bounded dispatch and transaction order; not infrastructure acceptance."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from core.services.normalization_dispatcher import NormalizationDispatcher
from repositories.normalization_repository import NormalizationRepository

NOW = datetime(2026, 1, 1, tzinfo=UTC)
TENANTS = [UUID(int=i) for i in range(1, 5)]


class Harness:
    def __init__(self):
        self.events = []
        self.active = None
        self.rows = TENANTS
        self.fail_stage: str | None = None
        self.error: BaseException = RuntimeError("do not log secrets")
        self.repository = AsyncMock()
        self.repository.list_work_tenants.side_effect = self.discover
        self.scan_batch = AsyncMock()
        self.scan_batch.run_batch.side_effect = lambda **kw: self.batch("scan", kw)
        self.delete_batch = AsyncMock()
        self.delete_batch.run_batch.side_effect = lambda **kw: self.batch("finalization", kw)
        self.relay = AsyncMock()
        self.relay.run_batch.side_effect = self.publish

    async def discover(self, *, now, after, limit):
        assert self.active == "discovery"
        assert now == NOW
        return [tenant for tenant in self.rows if after is None or tenant > after][:limit]

    def batch(self, stage, kwargs):
        assert self.active == stage
        self.events.append((stage, kwargs["equestrian_id"]))
        if self.fail_stage == stage:
            raise self.error

    async def publish(self, *, equestrian_id):
        assert self.active is None  # all discovery/scan/delete contexts have exited
        self.events.append(("relay", equestrian_id))
        if self.fail_stage == "relay":
            raise self.error
        return 1

    @asynccontextmanager
    async def context(self, stage, value):
        assert self.active is None
        self.active = stage
        self.events.append(("begin", stage))
        try:
            yield value
            if self.fail_stage == stage + "-commit":
                raise self.error
            self.events.append(("commit", stage))
        except BaseException:
            self.events.append(("rollback", stage))
            raise
        finally:
            self.active = None

    def build(self, size=2):
        return NormalizationDispatcher(
            transaction=lambda: self.context("discovery", self.repository),
            scan=lambda: self.context("scan", self.scan_batch),
            finalization=lambda: self.context("finalization", self.delete_batch),
            relay=self.relay,
            tenant_page_size=size,
            clock=lambda: NOW,
        )


async def test_bounded_keyset_pages_wrap_and_no_network_inside_transaction():
    h = Harness()
    dispatcher = h.build()
    assert await dispatcher.tick() == 0
    assert h.scan_batch.run_batch.await_count == h.delete_batch.run_batch.await_count == 2
    assert h.relay.run_batch.await_count == 2
    assert await dispatcher.tick() == 0
    assert h.relay.run_batch.await_count == 4
    assert await dispatcher.tick() == 0  # empty wrap tick, bounded discovery only
    assert h.relay.run_batch.await_count == 4
    assert await dispatcher.tick() == 0
    cursors = [call.kwargs["after"] for call in h.repository.list_work_tenants.await_args_list]
    assert cursors == [None, TENANTS[1], TENANTS[3], None]
    assert h.events[:8] == [
        ("begin", "discovery"),
        ("commit", "discovery"),
        ("begin", "scan"),
        ("scan", TENANTS[0]),
        ("commit", "scan"),
        ("begin", "finalization"),
        ("finalization", TENANTS[0]),
        ("commit", "finalization"),
    ]


async def test_restart_resets_discovery_only_and_lower_new_tenants_are_not_starved():
    h = Harness()
    dispatcher = h.build()
    await dispatcher.tick()
    h.rows = [UUID(int=0), *TENANTS]
    await dispatcher.tick()
    await dispatcher.tick()  # end of page, wrap
    await dispatcher.tick()
    assert h.scan_batch.run_batch.await_args_list[-2].kwargs["equestrian_id"] == UUID(int=0)
    restarted = h.build()
    await restarted.tick()
    assert h.repository.list_work_tenants.await_args.kwargs["after"] is None


@pytest.mark.parametrize("stage", ["scan", "finalization", "relay", "scan-commit", "finalization-commit"])
async def test_failure_rolls_back_one_stage_continues_others_and_is_reported(stage, caplog):
    h = Harness()
    h.fail_stage = stage
    assert await h.build().tick() == 2
    assert h.relay.run_batch.await_count == 2
    assert h.scan_batch.run_batch.await_count == h.delete_batch.run_batch.await_count == 2
    assert h.active is None
    if stage != "relay":
        assert ("rollback", stage.removesuffix("-commit")) in h.events
    assert "do not log secrets" not in caplog.text
    assert "Normalization batch failed" in caplog.text


@pytest.mark.parametrize("stage", ["scan", "finalization", "relay", "discovery-commit"])
async def test_cancellation_propagates_without_health_success_or_later_work(stage):
    h = Harness()
    h.fail_stage = stage
    h.error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await h.build().tick()
    assert h.active is None
    assert h.relay.run_batch.await_count <= 1
    if stage in ("scan", "discovery-commit"):
        h.delete_batch.run_batch.assert_not_awaited()


async def test_discovery_commit_error_propagates_without_advancing_cursor():
    h = Harness()
    dispatcher = h.build()
    h.fail_stage = "discovery-commit"
    with pytest.raises(RuntimeError):
        await dispatcher.tick()
    h.scan_batch.run_batch.assert_not_awaited()
    h.fail_stage = None
    await dispatcher.tick()
    assert h.repository.list_work_tenants.await_args.kwargs["after"] is None


async def test_short_page_wraps_without_unbounded_rescan():
    h = Harness()
    h.rows = TENANTS[:1]
    dispatcher = h.build()
    await dispatcher.tick()
    await dispatcher.tick()
    assert h.repository.list_work_tenants.await_count == 2
    assert all(call.kwargs["after"] is None for call in h.repository.list_work_tenants.await_args_list)


@pytest.mark.parametrize("size", [0, -1, 501])
def test_dispatch_bounds(size):
    with pytest.raises(ValueError):
        Harness().build(size=size)


async def test_discovery_sql_is_distinct_keyset_bounded_and_includes_due_outbox():
    session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    result.scalars.return_value = iter(TENANTS[:2])
    session.execute.return_value = result
    rows = await NormalizationRepository(session).list_work_tenants(now=NOW, after=TENANTS[0], limit=2)
    assert rows == TENANTS[:2]
    statement = session.execute.await_args.args[0].compile(dialect=postgresql.dialect())
    sql = str(statement)
    assert sql.count("SELECT DISTINCT") == 2
    assert sql.count("LIMIT") == 3 and "OFFSET" not in sql and "FOR UPDATE" not in sql
    assert "UNION" in sql
    assert "normalization_revisions.status !=" in sql
    assert "normalization_outbox.published_at IS NULL" in sql
    assert "normalization_outbox.next_attempt_at <=" in sql
    assert "normalization_outbox.lease_until <=" in sql
    assert "normalization_revisions.equestrian_id >" in sql
    assert "normalization_outbox.equestrian_id >" in sql
    assert list(statement.params.values()).count(2) == 3
    session.commit.assert_not_awaited()


@pytest.mark.parametrize("kwargs", [{"limit": 0}, {"limit": 501}, {"now": datetime(2026, 1, 1)}])
async def test_discovery_invalid_bounds_rejected_before_sql(kwargs):
    session = AsyncMock(spec=AsyncSession)
    with pytest.raises(ValueError):
        await NormalizationRepository(session).list_work_tenants(**{"now": NOW, **kwargs})
    session.execute.assert_not_awaited()
