"""NATS JetStream client for seo-service."""

import logging

import nats.errors
from infrastructure.nats.lifecycle import NatsConnectionErrorPolicy
from nats import NATS
from nats.js import JetStreamContext
from nats.js.api import PubAck
from settings import NatsSettings

logger = logging.getLogger(__name__)


class NatsJetstreamClient:
    """NATS JetStream client wrapper для публикации задач парсерам."""

    def __init__(self, settings: NatsSettings) -> None:
        self._settings = settings
        self._connection: NATS | None = None
        self._jetstream: JetStreamContext | None = None
        self._error_policy = NatsConnectionErrorPolicy(
            service_name="seo-service",
            report_after_attempts=settings.nats_error_report_after_attempts,
        )

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and self._connection.is_connected

    def _get_jetstream(self) -> JetStreamContext:
        if self._jetstream is None or not self.is_connected:
            raise RuntimeError("NATS JetStream client is not connected")
        return self._jetstream

    @property
    def jetstream(self) -> JetStreamContext:
        return self._get_jetstream()

    async def connect(self) -> None:
        """Подключиться к NATS и инициализировать JetStream context."""
        if self.is_connected:
            return

        self._connection = NATS()
        self._error_policy.reset()

        await self._connection.connect(
            servers=self._settings.nats_servers,
            name="seo-service",
            connect_timeout=5,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
            error_cb=self._error_policy.on_error,
            disconnected_cb=self._error_policy.on_disconnected,
            reconnected_cb=self._error_policy.on_reconnected,
            closed_cb=self._error_policy.on_closed,
        )
        self._jetstream = self._connection.jetstream()
        logger.info("NATS JetStream client connected to %s", self._settings.nats_servers)

    async def close(self) -> None:
        """Gracefully закрыть NATS connection при shutdown."""
        if self._connection is None:
            return

        try:
            if not self._connection.is_closed:
                try:
                    await self._connection.drain()
                except (TimeoutError, nats.errors.Error) as error:
                    # Остановка во время reconnect — не повод ронять lifespan.
                    logger.warning("NATS drain failed on shutdown, closing connection: %s", error)
                    await self._connection.close()
        finally:
            self._connection = None
            self._jetstream = None
            logger.info("NATS JetStream client closed")

    async def publish(
        self,
        *,
        subject: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> PubAck:
        """Опубликовать сообщение в NATS JetStream."""
        jetstream = self._get_jetstream()
        return await jetstream.publish(
            subject=subject,
            payload=payload,
            headers=headers,
        )
