import logging

import nats.errors
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import PubAck, RetentionPolicy, StorageType, StreamConfig

from clients.nats.lifecycle import NatsConnectionErrorPolicy
from settings import NatsSettings

logger = logging.getLogger(__name__)


class NatsJetstreamClient:
    def __init__(self, settings: NatsSettings) -> None:
        self._settings = settings
        self._connection: NATS | None = None
        self._jetstream: JetStreamContext | None = None
        self._error_policy = NatsConnectionErrorPolicy(
            service_name="seo-service", report_after_attempts=settings.nats_error_report_after_attempts
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
        if self.is_connected:
            return
        # A failed initial connect must not leave a reconnecting connection behind.
        await self.close()
        connection = NATS()
        self._connection = connection
        try:
            await connection.connect(
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
        except Exception:
            await self.close()
            raise
        self._jetstream = connection.jetstream()
        self._error_policy.reset()

    async def close(self) -> None:
        connection = self._connection
        try:
            if connection is not None and not connection.is_closed:
                try:
                    await connection.drain()
                except TimeoutError, nats.errors.Error:
                    logger.warning("NATS drain failed on shutdown; closing connection")
                    await connection.close()
        finally:
            self._connection = None
            self._jetstream = None

    async def setup(self) -> None:
        await self.setup_streams()
        await self.setup_consumers()

    async def setup_streams(self) -> None:
        await self._get_jetstream().add_stream(
            config=StreamConfig(
                name=self._settings.nats_stream_parse_commands,
                subjects=self._settings.nats_subjects_parse_commands,
                storage=StorageType.FILE,
                retention=RetentionPolicy.LIMITS,
                max_age=self._settings.nats_parse_commands_max_age_seconds,
            )
        )

    async def setup_consumers(self) -> None:
        """Producer owns no consumers; each provider executor registers its durable."""

    async def publish(self, *, subject: str, payload: bytes, headers: dict[str, str] | None = None) -> PubAck:
        return await self._get_jetstream().publish(
            subject=subject,
            payload=payload,
            headers=headers,
            timeout=self._settings.nats_publish_timeout_seconds,
        )
