import logging
from contextvars import ContextVar
from typing import TypeVar
from uuid import UUID

import httpx
from pydantic import BaseModel

from core.exceptions import ParseTargetsUnavailableError
from core.schemas.clients.yandex_service import ConnectedMetricaCounter, MetricaBinding, MetricaBindingList

ResponseDto = TypeVar("ResponseDto", bound=BaseModel)
_request_active: ContextVar[bool] = ContextVar("yandex_request_active", default=False)


class _PeerLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not _request_active.get()


# HTTPX INFO and httpcore DEBUG can include peer URLs and response headers.
# Filters are task-local: unrelated requests in parallel are not suppressed.
for _logger in ("httpx", "httpcore.connection", "httpcore.http11", "httpcore.http2", "httpcore.proxy"):
    logging.getLogger(_logger).addFilter(_PeerLogFilter())


class YandexServiceClient:
    def __init__(
        self, *, base_url: str, timeout: float = 4.0, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._transport = transport

    async def _get(self, path: str, params: list[tuple[str, str]], schema: type[ResponseDto]) -> ResponseDto:
        token = _request_active.set(True)
        try:
            # A fresh client cannot retain cookies from earlier responses; environment
            # proxies and .netrc auth are deliberately ignored. Redirects are not followed.
            url = httpx.URL(f"{self._base_url}{path}")
            if url.username or url.password:
                raise ParseTargetsUnavailableError()
            async with httpx.AsyncClient(
                timeout=self._timeout, transport=self._transport, trust_env=False, follow_redirects=False
            ) as client:
                response = await client.get(url, params=httpx.QueryParams(tuple(params)))
                if response.status_code != 200:
                    raise ParseTargetsUnavailableError()
                return schema.model_validate(response.json())
        except httpx.HTTPError, ValueError, TypeError, ParseTargetsUnavailableError:
            logging.getLogger(__name__).warning("Источник целей парсинга временно недоступен")
            raise ParseTargetsUnavailableError() from None
        finally:
            _request_active.reset(token)

    async def list_metrica_bindings(self, *, offset: int, limit: int) -> MetricaBindingList:
        page = await self._get(
            "/api/connections/metrica", [("offset", str(offset)), ("limit", str(limit))], MetricaBindingList
        )
        if len(page.items) > limit or len(page.items) > max(0, page.total - offset):
            raise ParseTargetsUnavailableError()
        if not page.items and offset < page.total:
            raise ParseTargetsUnavailableError()
        if len({item.equestrian_id for item in page.items}) != len(page.items):
            raise ParseTargetsUnavailableError()
        return page

    async def get_metrica_binding(self, *, equestrian_id: UUID) -> MetricaBinding | None:
        result = await self._get(f"/api/connections/metrica/{equestrian_id}", [], ConnectedMetricaCounter)
        if result.counter is None:
            return None
        if result.counter.equestrian_id != equestrian_id:
            raise ParseTargetsUnavailableError()
        return MetricaBinding(equestrian_id=result.counter.equestrian_id, counter_id=result.counter.counter_id)
