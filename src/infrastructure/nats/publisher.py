"""NATS publisher для постановки задач парсерам."""

import json
import logging
import uuid
from datetime import datetime

from infrastructure.nats.client import NatsJetstreamClient
from settings import NatsSettings

logger = logging.getLogger(__name__)


class TaskPublisher:
    """Publisher для постановки задач парсинга в NATS JetStream."""

    def __init__(self, client: NatsJetstreamClient, settings: NatsSettings) -> None:
        self._client = client
        self._settings = settings

    async def publish_task(
        self,
        *,
        parser_type: str,
        task_id: str,
        site_id: int,
        params: dict,
        trace_id: str | None = None,
    ) -> tuple[str, bool]:
        """
        Опубликовать задачу парсинга в NATS JetStream.

        Args:
            parser_type: Тип парсера (например, "yandex_metrics")
            task_id: UUID задачи
            site_id: ID сайта
            params: Параметры парсинга
            trace_id: Trace ID для корреляции (опционально, генерируется автоматически)

        Returns:
            Tuple[message_id, duplicate]: ID сообщения и флаг дедупликации
        """
        subject = f"seo.tasks.{parser_type}"

        # Генерируем trace_id если не передан
        if trace_id is None:
            trace_id = str(uuid.uuid4())

        # Message payload
        payload = {
            "task_id": task_id,
            "site_id": site_id,
            "params": params,
        }

        # Headers с trace_id и Nats-Msg-Id для идемпотентности
        headers = {
            "trace_id": trace_id,
            "Nats-Msg-Id": task_id,  # Используем task_id как message ID
        }

        # Structured logging для публикации
        logger.info(
            "Publishing task to NATS",
            extra={
                "event": "nats_publish",
                "trace_id": trace_id,
                "subject": subject,
                "task_id": task_id,
                "site_id": site_id,
                "parser_type": parser_type,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

        try:
            ack = await self._client.publish(
                subject=subject,
                payload=json.dumps(payload).encode("utf-8"),
                headers=headers,
            )

            # Проверяем дедупликацию
            duplicate = bool(getattr(ack, "duplicate", False))
            if duplicate:
                logger.warning(
                    "Task was deduplicated by broker",
                    extra={
                        "event": "nats_publish_duplicate",
                        "trace_id": trace_id,
                        "subject": subject,
                        "task_id": task_id,
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )
            else:
                logger.info(
                    "Task published successfully",
                    extra={
                        "event": "nats_publish_success",
                        "trace_id": trace_id,
                        "subject": subject,
                        "task_id": task_id,
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )

            return task_id, duplicate

        except Exception as error:
            logger.error(
                "Failed to publish task to NATS",
                extra={
                    "event": "nats_publish_error",
                    "trace_id": trace_id,
                    "subject": subject,
                    "task_id": task_id,
                    "error": str(error),
                    "timestamp": datetime.utcnow().isoformat(),
                },
                exc_info=True,
            )
            raise
