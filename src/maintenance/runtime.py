"""Supervised maintenance only; never started by HTTP or parse scheduler."""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from pathlib import Path
from time import time
from typing import Protocol

from nats.js.errors import BadRequestError

from settings import NormalizationSettings

logger = logging.getLogger(__name__)


class RuntimeClient(Protocol):
    @property
    def is_connected(self) -> bool: ...
    async def connect(self) -> None: ...
    async def setup_maintenance(self) -> None: ...
    async def close(self) -> None: ...


class RuntimeConsumer(Protocol):
    @property
    def is_ready(self) -> bool: ...
    async def run(self) -> None: ...


class RuntimeDispatcher(Protocol):
    async def tick(self) -> int: ...


class MaintenanceRuntime:
    def __init__(
        self,
        *,
        client: RuntimeClient,
        consumers: Sequence[RuntimeConsumer],
        dispatcher: RuntimeDispatcher,
        settings: NormalizationSettings,
    ) -> None:
        self.client = client
        self.consumers = tuple(consumers)
        self.dispatcher = dispatcher
        self.settings = settings
        self._tick_ok = False
        self._tasks: list[asyncio.Task[None]] = []

    @property
    def is_ready(self) -> bool:
        return all(
            (
                self._tick_ok,
                self.client.is_connected,
                len(self.consumers) == 2,
                all(consumer.is_ready for consumer in self.consumers),
                len(self._tasks) == 3,
                all(not task.done() for task in self._tasks),
            )
        )

    async def _dispatch(self) -> None:
        delay = self.settings.normalization_tick_seconds
        while True:
            self._tick_ok = False
            failures = await self.dispatcher.tick()
            self._tick_ok = failures == 0
            delay = self.settings.normalization_tick_seconds if not failures else min(max(delay * 2, 1), 60)
            await asyncio.sleep(delay)

    async def _epoch(self) -> None:
        await self.client.connect()
        await self.client.setup_maintenance()
        self._tasks = [asyncio.create_task(consumer.run()) for consumer in self.consumers]
        self._tasks.append(asyncio.create_task(self._dispatch()))
        try:
            while True:
                for task in self._tasks:
                    if task.done():
                        await task
                        raise RuntimeError("maintenance worker stopped")
                # File heartbeat is local process readiness, not HTTP health.
                await asyncio.to_thread(
                    Path(self.settings.normalization_readiness_file).write_text, str(time()) if self.is_ready else "0"
                )
                await asyncio.sleep(1)
        finally:
            self._tick_ok = False
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []

    async def run(self) -> None:
        path = Path(self.settings.normalization_readiness_file)
        await asyncio.to_thread(path.write_text, "0")
        delay = 1.0
        attempts = 0
        try:
            while True:
                try:
                    await self._epoch()
                except BadRequestError:
                    # Incompatible stream/durable topology is not transient.
                    logger.error("maintenance_topology_incompatible")
                    raise
                except Exception:
                    attempts += 1
                    log = logger.error if attempts >= 5 else logger.warning
                    log("maintenance_runtime_retry attempt=%s", attempts)
                finally:
                    self._tick_ok = False
                    try:
                        await asyncio.to_thread(path.write_text, "0")
                    finally:
                        await self.client.close()
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
        finally:
            await asyncio.to_thread(path.write_text, "0")


async def serve(
    *, runtime: MaintenanceRuntime, stop: asyncio.Event, close_database: Callable[[], Awaitable[None]]
) -> None:
    worker = asyncio.create_task(runtime.run(), name="seo-maintenance")
    stopping = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait((worker, stopping), return_when=asyncio.FIRST_COMPLETED)
        if worker.done():
            await worker
    finally:
        worker.cancel()
        stopping.cancel()
        try:
            with suppress(asyncio.CancelledError):
                await worker
        finally:
            with suppress(asyncio.CancelledError):
                await stopping
            await close_database()
