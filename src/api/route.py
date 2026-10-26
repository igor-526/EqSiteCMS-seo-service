from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import Request, Response

from depends.tenant import get_equestrian_id
from utils.transactional_route import TransactionalRoute


class TenantTransactionalRoute(TransactionalRoute):
    """Reject missing tenants before session creation AND FastAPI JSON parsing."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        transactional_handler = super().get_route_handler()

        async def handler(request: Request) -> Response:
            get_equestrian_id(request.headers.get("X-Equestrian-Id"))
            return await transactional_handler(request)

        return handler
