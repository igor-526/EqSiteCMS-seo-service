"""Celery app configuration for seo-service."""

import logging
from datetime import datetime

from celery import Celery, Task
from celery.schedules import crontab

from settings import settings

logger = logging.getLogger(__name__)

# Создаём Celery app
celery_app = Celery(
    "seo-service",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

# Конфигурация Celery
celery_app.conf.update(
    # Таймзона
    timezone="UTC",
    enable_utc=True,
    # Retry политика
    task_acks_late=True,  # Acknowledge только после успешного выполнения
    task_reject_on_worker_lost=True,  # Reject task если worker упал
    # Результаты
    result_expires=3600,  # Результаты хранятся 1 час
    result_extended=True,  # Расширенная информация в результатах
    # Сериализация
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Логирование
    worker_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    worker_task_log_format="[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s",
)

# Celery Beat schedule - запускаем schedule_parsing каждые 5 минут
celery_app.conf.beat_schedule = {
    "schedule-parsing-every-5-minutes": {
        "task": "src.tasks.schedule_parsing.schedule_parsing",
        "schedule": crontab(minute="*/5"),  # Каждые 5 минут
        "options": {
            "expires": 240,  # Task expires через 4 минуты (меньше чем интервал)
        },
    },
}

# Автодискавери задач
celery_app.autodiscover_tasks(["src.tasks"])


class StructuredLoggingTask(Task):
    """Base task с structured logging."""

    def __call__(self, *args, **kwargs):
        """Wrapper для выполнения задачи с structured logging."""
        task_id = self.request.id
        task_name = self.name

        # Log start
        logger.info(
            "Celery task started",
            extra={
                "event": "celery_task_start",
                "task_id": task_id,
                "task_name": task_name,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )

        try:
            result = super().__call__(*args, **kwargs)

            # Log success
            logger.info(
                "Celery task completed successfully",
                extra={
                    "event": "celery_task_success",
                    "task_id": task_id,
                    "task_name": task_name,
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )

            return result

        except Exception as error:
            # Log failure
            logger.error(
                "Celery task failed",
                extra={
                    "event": "celery_task_failure",
                    "task_id": task_id,
                    "task_name": task_name,
                    "error": str(error),
                    "timestamp": datetime.utcnow().isoformat(),
                },
                exc_info=True,
            )
            raise


# Устанавливаем base task для всех задач
celery_app.Task = StructuredLoggingTask
