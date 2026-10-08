import logging

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from settings import settings


def configure_sentry() -> None:
    if not settings.sentry_enabled:
        return

    # Игнорируем NATS aio client логгер, чтобы избежать шума reconnect'ов в Sentry
    # Собственная политика логирования в NatsConnectionErrorPolicy эскалирует ровно один раз за инцидент
    logging.getLogger("nats.aio.client").setLevel(logging.CRITICAL)

    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.sentry_environment,
        release=settings.sentry_release,
        traces_sample_rate=settings.sentry_traces_sample_rate,
        send_default_pii=False,
        integrations=[
            FastApiIntegration(),
            SqlalchemyIntegration(),
            LoggingIntegration(level=logging.INFO, event_level=logging.ERROR),
        ],
    )
