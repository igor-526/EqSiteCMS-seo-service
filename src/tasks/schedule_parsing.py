"""Schedule parsing task for Celery."""

import asyncio
import logging
import uuid
from datetime import datetime

from celery import shared_task
from croniter import croniter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.nats.publisher import TaskPublisher
from main import get_task_publisher
from models.parsing_schedule import ParsingSchedule
from utils.database import SessionFactory

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    name="src.tasks.schedule_parsing.schedule_parsing",
    autoretry_for=(Exception,),  # Retry на любые исключения
    retry_kwargs={"max_retries": 3},
    retry_backoff=True,  # Exponential backoff
    retry_backoff_max=600,  # Максимум 10 минут между попытками
    retry_jitter=True,  # Добавляем jitter для распределения нагрузки
)
def schedule_parsing(self) -> dict:
    """
    Периодическая задача для проверки расписания парсингов.

    Читает активные ParsingSchedule из БД, проверяет cron_expression
    и ставит задачи парсерам в NATS JetStream.

    Returns:
        dict: Статистика выполнения задачи
    """
    task_id = self.request.id
    start_time = datetime.utcnow()

    logger.info(
        "Starting schedule_parsing task",
        extra={
            "event": "schedule_parsing_start",
            "task_id": task_id,
            "timestamp": start_time.isoformat(),
        },
    )

    # Запускаем async функцию в sync контексте Celery
    result = asyncio.run(_schedule_parsing_async(task_id))

    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()

    logger.info(
        "Completed schedule_parsing task",
        extra={
            "event": "schedule_parsing_complete",
            "task_id": task_id,
            "duration_seconds": duration,
            "schedules_checked": result["schedules_checked"],
            "tasks_published": result["tasks_published"],
            "errors": result["errors"],
            "timestamp": end_time.isoformat(),
        },
    )

    return result


async def _schedule_parsing_async(task_id: str) -> dict:
    """
    Async implementation of schedule_parsing.

    Args:
        task_id: Celery task ID для логирования

    Returns:
        dict: Статистика выполнения {schedules_checked, tasks_published, errors}
    """
    schedules_checked = 0
    tasks_published = 0
    errors = 0
    current_time = datetime.utcnow()

    # Получаем TaskPublisher
    task_publisher: TaskPublisher = get_task_publisher()

    try:
        # Создаём async сессию БД
        async with SessionFactory() as session:
            session: AsyncSession

            # Читаем активные расписания
            stmt = select(ParsingSchedule).where(ParsingSchedule.is_active)
            result = await session.execute(stmt)
            schedules = result.scalars().all()

            schedules_checked = len(schedules)

            logger.info(
                "Found active parsing schedules",
                extra={
                    "event": "schedules_found",
                    "task_id": task_id,
                    "count": schedules_checked,
                    "timestamp": current_time.isoformat(),
                },
            )

            # Проверяем каждое расписание
            for schedule in schedules:
                try:
                    # Явно извлекаем значения для type checker
                    cron_expr = str(schedule.cron_expression)
                    parser_type_val = str(schedule.parser_type)
                    site_id_val = schedule.site_id if isinstance(schedule.site_id, int) else 0

                    # Проверяем, пора ли запускать задачу по cron
                    if _should_run_now(cron_expr, current_time):
                        # Генерируем уникальный task_id
                        parsing_task_id = str(uuid.uuid4())
                        trace_id = str(uuid.uuid4())

                        # Публикуем задачу в NATS
                        await task_publisher.publish_task(
                            parser_type=parser_type_val,
                            task_id=parsing_task_id,
                            site_id=site_id_val,
                            params={},  # Пока пустые параметры
                            trace_id=trace_id,
                        )

                        tasks_published += 1

                        logger.info(
                            "Published parsing task",
                            extra={
                                "event": "task_published",
                                "celery_task_id": task_id,
                                "parsing_task_id": parsing_task_id,
                                "trace_id": trace_id,
                                "schedule_id": schedule.id,
                                "site_id": schedule.site_id,
                                "parser_type": schedule.parser_type,
                                "timestamp": current_time.isoformat(),
                            },
                        )

                except Exception as error:
                    errors += 1
                    logger.error(
                        "Error processing schedule",
                        extra={
                            "event": "schedule_error",
                            "task_id": task_id,
                            "schedule_id": schedule.id,
                            "site_id": schedule.site_id,
                            "parser_type": schedule.parser_type,
                            "error": str(error),
                            "timestamp": current_time.isoformat(),
                        },
                        exc_info=True,
                    )
                    # Продолжаем обработку остальных расписаний
                    continue

    except Exception as error:
        logger.error(
            "Critical error in schedule_parsing",
            extra={
                "event": "schedule_parsing_critical_error",
                "task_id": task_id,
                "error": str(error),
                "timestamp": current_time.isoformat(),
            },
            exc_info=True,
        )
        # Пробрасываем исключение для retry
        raise

    return {
        "schedules_checked": schedules_checked,
        "tasks_published": tasks_published,
        "errors": errors,
    }


def _should_run_now(cron_expression: str, current_time: datetime) -> bool:
    """
    Проверяет, должна ли задача запуститься сейчас согласно cron выражению.

    Используем простую логику: если текущее время попадает в интервал
    последних 5 минут от cron schedule, то запускаем.

    Args:
        cron_expression: Cron выражение (например, "0 */6 * * *")
        current_time: Текущее время для проверки

    Returns:
        bool: True если пора запускать задачу
    """
    try:
        # Создаём croniter с текущим временем
        cron = croniter(cron_expression, current_time)

        # Получаем предыдущее запланированное время
        prev_time = cron.get_prev(datetime)

        # Если разница между текущим временем и предыдущим запланированным
        # меньше 5 минут (интервал проверки), то пора запускать
        time_diff = (current_time - prev_time).total_seconds()

        # 5 минут = 300 секунд (совпадает с интервалом Celery Beat)
        return 0 <= time_diff <= 300

    except Exception as error:
        logger.warning(
            "Invalid cron expression",
            extra={
                "event": "invalid_cron",
                "cron_expression": cron_expression,
                "error": str(error),
            },
        )
        return False
