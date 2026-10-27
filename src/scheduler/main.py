"""Run with PYTHONPATH=src python -m scheduler.main (no HTTP listener)."""

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Protocol

from scheduler.runner import SchedulerRunner

logger = logging.getLogger(__name__)


class SchedulerNatsProtocol(Protocol):
    async def connect(self) -> None: ...
    async def setup_streams(self) -> None: ...
    async def close(self) -> None: ...


async def setup_nats(
    client: SchedulerNatsProtocol,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    delay = 1.0
    reported = False
    while True:
        try:
            await client.connect()
            await client.setup_streams()
            return
        except Exception:
            if not reported:
                logger.error("Scheduler NATS unavailable; retrying connection/topology setup")
                reported = True
            else:
                logger.warning("Scheduler NATS setup still unavailable; retrying")
            await sleep(delay)
            delay = min(delay * 2, 30)


async def serve(
    *,
    runner: SchedulerRunner,
    client: SchedulerNatsProtocol,
    stop: asyncio.Event,
    close_database: Callable[[], Awaitable[None]],
) -> None:
    async def work() -> None:
        await setup_nats(client)
        await runner.run()

    worker = asyncio.create_task(work(), name="parse-scheduler")
    stopping = asyncio.create_task(stop.wait(), name="parse-scheduler-stop")
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
            try:
                await client.close()
            finally:
                await close_database()


async def main() -> None:
    # Composition boundary: shared client/publisher, ledger owns short DB sessions.
    from containers.application import clients_container, services_container, settings_container
    from utils.configure_sentry import configure_sentry
    from utils.database import close_database

    configure_sentry()
    settings = settings_container.parse_settings()
    runner = SchedulerRunner(
        registry=clients_container.parse_provider_registry(),
        slots=services_container.slot_calculator(),
        dispatch=services_container.parse_dispatch_service(),
        enabled=settings.parse_schedule_enabled,
        tick_seconds=settings.parse_scheduler_tick_seconds,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGTERM, signal.SIGINT)
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)
    try:
        await serve(runner=runner, client=clients_container.nats_client(), stop=stop, close_database=close_database)
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
