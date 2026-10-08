from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from core.exceptions import AppError
from infrastructure.nats.client import NatsJetstreamClient
from infrastructure.nats.publisher import TaskPublisher
from settings import nats_settings, settings
from utils.configure_sentry import configure_sentry
from utils.database import close_database

configure_sentry()

# Модульные singletons для NATS
_nats_client: NatsJetstreamClient | None = None
_task_publisher: TaskPublisher | None = None


def get_nats_client() -> NatsJetstreamClient:
    """Получить singleton NATS client."""
    global _nats_client
    if _nats_client is None:
        _nats_client = NatsJetstreamClient(nats_settings)
    return _nats_client


def get_task_publisher() -> TaskPublisher:
    """Получить singleton TaskPublisher."""
    global _task_publisher
    if _task_publisher is None:
        _task_publisher = TaskPublisher(get_nats_client(), nats_settings)
    return _task_publisher


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Startup: подключаемся к NATS
    nats_client = get_nats_client()
    await nats_client.connect()

    try:
        yield
    finally:
        # Shutdown: gracefully закрываем NATS connection
        await nats_client.close()
        await close_database()


app = FastAPI(title=settings.app_title, debug=settings.debug, lifespan=lifespan)


@app.get("/health", tags=["Health"])
async def health() -> dict[str, str]:
    """
    Health check endpoint (Public Read - no auth required).
    
    Returns:
        dict: Service status
    """
    return {"status": "healthy", "service": "seo-service"}


@app.exception_handler(AppError)
async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": exc.errors()})
