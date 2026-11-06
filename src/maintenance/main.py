"""Explicit opt-in process: PYTHONPATH=src python -m maintenance.main."""

import asyncio
import logging
import signal

from settings import NormalizationSettings


async def main() -> None:
    settings = NormalizationSettings()
    if not settings.normalization_enabled:
        raise SystemExit("SEO normalization runtime is disabled")
    from depends.normalization_runtime import build_runtime
    from maintenance.runtime import serve
    from utils.configure_sentry import configure_sentry
    from utils.database import close_database

    configure_sentry()
    runtime = build_runtime(settings=settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGTERM, signal.SIGINT)
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)
    try:
        await serve(runtime=runtime, stop=stop, close_database=close_database)
    except Exception:
        logging.getLogger(__name__).error("maintenance_process_failed")
        raise SystemExit(1) from None
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
