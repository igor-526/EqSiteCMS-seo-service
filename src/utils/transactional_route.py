from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import Request, Response
from fastapi.routing import APIRoute

from containers import repositories_container


class TransactionalRoute(APIRoute):
    """Complete the request transaction before sending the response.

    Initialize the context-local session here, never during application startup.
    Background tasks and streaming bodies must open their own sessions.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original_handler = super().get_route_handler()

        async def handler(request: Request) -> Response:
            session = await repositories_container.session.async_()
            try:
                async with session.begin():
                    return await original_handler(request)
            finally:
                shutdown = repositories_container.session.shutdown()
                if shutdown is not None:
                    await shutdown

        return handler
