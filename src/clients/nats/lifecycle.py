import logging

logger = logging.getLogger(__name__)


class NatsConnectionErrorPolicy:
    """
    Политика логирования сбоев соединения с NATS.

    Переподключение — штатная ситуация, поэтому первые
    `report_after_attempts` последовательных неудач остаются на уровне
    `warning` и не превращаются в события мониторинга. Затяжная
    недоступность брокера эскалируется ровно один раз за инцидент;
    успешный reconnect сбрасывает состояние.
    """

    def __init__(self, *, service_name: str, report_after_attempts: int) -> None:
        self._service_name = service_name
        self._report_after_attempts = report_after_attempts

        self._consecutive_failures = 0
        self._escalated = False

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def escalated(self) -> bool:
        return self._escalated

    def reset(self) -> None:
        self._consecutive_failures = 0
        self._escalated = False

    async def on_error(self, error: Exception) -> None:
        self._consecutive_failures += 1

        if self._consecutive_failures > self._report_after_attempts and not self._escalated:
            self._escalated = True
            logger.error(
                "NATS is unavailable for %s: %s consecutive failed attempts",
                self._service_name,
                self._consecutive_failures,
                exc_info=error,
            )
            return

        logger.warning(
            "NATS connection error for %s (attempt %s): %s",
            self._service_name,
            self._consecutive_failures,
            error,
        )

    async def on_disconnected(self) -> None:
        logger.warning("NATS disconnected for %s", self._service_name)

    async def on_reconnected(self) -> None:
        logger.info(
            "NATS reconnected for %s after %s failed attempts",
            self._service_name,
            self._consecutive_failures,
        )
        self.reset()

    async def on_closed(self) -> None:
        logger.info("NATS connection closed for %s", self._service_name)
