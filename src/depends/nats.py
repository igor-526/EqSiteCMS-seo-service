"""NATS dependency injection для FastAPI endpoints."""

from infrastructure.nats.publisher import TaskPublisher
from main import get_task_publisher


async def get_publisher() -> TaskPublisher:
    """Dependency для получения TaskPublisher в endpoints."""
    return get_task_publisher()
