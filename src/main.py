import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError, ResponseValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from api import (
    blacklisted_hashes,
    blacklisted_hashes_router,
    blacklisted_query_params,
    blacklisted_query_params_router,
    pages,
    pages_router,
    parse_jobs,
    parse_jobs_router,
    search_queries,
    search_queries_router,
)
from clients.nats.client import NatsJetstreamClient
from containers import services_container
from containers.application import clients_container
from core.exceptions import AppError, MetricaUnavailableError
from depends.tenant import get_equestrian_id
from settings import settings
from utils.configure_sentry import configure_sentry
from utils.database import close_database

configure_sentry()
logger = logging.getLogger(__name__)


async def setup_nats(client: NatsJetstreamClient) -> None:
    """Retry initial connection/topology without delaying HTTP readiness."""
    delay = 1
    while True:
        try:
            await client.connect()
            await client.setup_streams()
            return
        except Exception:
            # Never expose connection parameters or server responses in logs.
            logger.warning("parse_queue_setup_unavailable")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 30)


@asynccontextmanager
async def lifespan(_: FastAPI):
    client = clients_container.nats_client()
    startup = asyncio.create_task(setup_nats(client), name="parse-queue-startup")
    try:
        yield
    finally:
        startup.cancel()
        try:
            with suppress(asyncio.CancelledError):
                await startup
            await client.close()
        finally:
            await close_database()


# Composition root: shared settings and peer providers, without repository changes.
services_container.page_service.add_kwargs(
    ya_metrica_client=clients_container.ya_metrica_client,
    metrics_period_resolver=services_container.metrics_period_resolver,
    metrics_table_builder=services_container.metrics_table_builder,
)
services_container.search_query_service.add_kwargs(
    ya_metrica_client=clients_container.ya_metrica_client,
    metrics_period_resolver=services_container.metrics_period_resolver,
    metrics_table_builder=services_container.metrics_table_builder,
)
services_container.wire(modules=[search_queries, pages, blacklisted_query_params, blacklisted_hashes, parse_jobs])


class SafeHttpErrorBoundary:
    """Catch request/cleanup failures without exposing diagnostics or traceback."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        response_started = False

        async def track_response(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, track_response)
        except Exception as exc:
            if response_started:
                raise
            response = await unexpected_error_handler(Request(scope), exc)
            await response(scope, receive, send)


# Never let Starlette's debug middleware expose tracebacks, even in development.
app = FastAPI(title=settings.app_title, debug=False, lifespan=lifespan, redirect_slashes=False)
app.add_middleware(SafeHttpErrorBoundary)
app.state.container = services_container
router = APIRouter(prefix="/api", dependencies=[Depends(get_equestrian_id)])
router.include_router(search_queries_router, prefix="/search-queries")
router.include_router(pages_router, prefix="/pages")
router.include_router(parse_jobs_router, prefix="/parse-jobs")
router.include_router(blacklisted_query_params_router, prefix="/page-normalization/blacklisted-query-params")
router.include_router(blacklisted_hashes_router, prefix="/page-normalization/blacklisted-hashes")
app.include_router(router)


@app.get("/health", tags=["Health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(MetricaUnavailableError)
@app.exception_handler(AppError)
async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
    # Instance messages may include diagnostics: only class constants are public.
    return JSONResponse(status_code=exc.status_code, content={"detail": type(exc).message})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    # Unknown field names can be attacker-controlled too; redact them in loc.
    safe_keys = {
        "body",
        "provider",
        "query",
        "path",
        "header",
        "items",
        "value",
        "item_id",
        "search",
        "ordering",
        "mode",
        "metric",
        "date_from",
        "date_to",
        "limit",
        "offset",
    }
    detail = [
        {
            "type": "request_validation",
            "loc": [part if isinstance(part, int) or part in safe_keys else "field" for part in error["loc"]],
            "msg": "Некорректные параметры запроса",
        }
        for error in exc.errors()
    ]
    return JSONResponse(status_code=400, content={"detail": detail})


@app.exception_handler(HTTPException)
async def http_error_handler(_: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": "Запрос отклонён"})


@app.exception_handler(ResponseValidationError)
@app.exception_handler(Exception)
async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
    category = "response_validation" if isinstance(exc, ResponseValidationError) else "unexpected"
    logger.error("request_failure category=%s", category)
    return JSONResponse(status_code=500, content={"detail": "Внутренняя ошибка сервиса"})
