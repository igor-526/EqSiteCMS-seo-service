import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime
from typing import Protocol

from core.services.parsing import SlotCalculator
from core.services.parsing.registry import ParseProviderRegistry

logger = logging.getLogger(__name__)


class ScheduledDispatchProtocol(Protocol):
    async def dispatch_scheduled_slot(self, *, provider: str, local_date: date) -> bool: ...


def utc_now() -> datetime:
    return datetime.now(UTC)


class SchedulerRunner:
    def __init__(
        self,
        *,
        registry: ParseProviderRegistry,
        slots: SlotCalculator,
        dispatch: ScheduledDispatchProtocol,
        enabled: bool,
        tick_seconds: float,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._registry = registry
        self._slots = slots
        self._dispatch = dispatch
        self._enabled = enabled
        self._tick_seconds = tick_seconds
        self._clock = clock
        self._sleep = sleep
        self._completed: set[tuple[str, date]] = set()

    async def tick(self) -> None:
        if not self._enabled:
            return
        now = self._clock()
        local_date = self._slots.local_now(utc_now=now).date()
        # Keep memory bounded without retaining yesterday's catch-up slots.
        self._completed = {key for key in self._completed if key[1] == local_date}
        slot = self._slots.due_slot(utc_now=now)
        if slot is None:
            return
        for provider in self._registry.enabled():
            # A slow provider can cross local midnight; never carry its slot to
            # another provider. The next tick calculates the new day's due slot.
            if not self._slots.is_current_slot(local_date=slot, utc_now=self._clock()):
                return
            key = (provider.code, slot)
            if key in self._completed:
                continue
            try:
                completed = await self._dispatch.dispatch_scheduled_slot(provider=provider.code, local_date=slot)
            except Exception:
                # No exception text/traceback: peer errors may contain credentials.
                logger.warning("Scheduled provider dispatch failed; retry next tick: provider=%s", provider.code)
                continue
            # A claim skip can be a live pending lease, not a completed slot.
            if completed is True:
                self._completed.add(key)

    async def run(self) -> None:
        while True:
            try:
                await self.tick()
            except Exception:
                logger.warning("Scheduler tick failed; retry next tick")
            await self._sleep(self._tick_seconds)
