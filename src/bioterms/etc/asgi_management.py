import asyncio
from collections.abc import Awaitable, Callable

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


ASGIAppFactory = Callable[[], Awaitable[ASGIApp]]


class ReloadableASGIApp:
    def __init__(self, factory: ASGIAppFactory) -> None:
        self._factory = factory
        self._target: ASGIApp | None = None
        self._reload_lock = asyncio.Lock()

    @property
    def is_ready(self) -> bool:
        return self._target is not None

    async def initialise(self) -> None:
        await self.reload()

    async def reload(self) -> None:
        async with self._reload_lock:
            # Database calls, status checks, schema construction and
            # middleware construction all happen before replacement.
            new_target = await self._factory()

            # Atomic reference replacement for future requests.
            self._target = new_target

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        # Capture one target for the complete request/WebSocket connection.
        target = self._target

        if target is None:
            response = JSONResponse(
                {"detail": "GraphQL service is not initialised"},
                status_code=503,
            )
            await response(scope, receive, send)
            return

        await target(scope, receive, send)
