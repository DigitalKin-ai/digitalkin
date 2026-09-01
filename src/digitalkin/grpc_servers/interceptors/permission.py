"""Client-side permission middleware for gRPC service-data access.

A single cross-cutting interceptor: any unary call a backend rejects with
``PERMISSION_DENIED`` surfaces as :class:`PermissionDeniedError`, so callers
never handle permission per service. Attached on every channel, it covers all
data services and module-to-module calls uniformly.

The interceptor *returns* a terminal denied call rather than raising: raising a
non-``AioRpcError`` from an aio interceptor leaks into the intercepted call's
``__del__`` (grpc only swallows ``AioRpcError``/``CancelledError`` there), so we
mirror grpc's own ``UnaryUnaryCallResponse`` pattern and raise from ``__await__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import grpc
import grpc.aio

from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.logger import logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class _DeniedUnaryUnaryCall(grpc.aio.UnaryUnaryCall):
    """A finished unary call that raises ``PermissionDeniedError`` when awaited."""

    def __init__(self, method: str, details: str) -> None:
        self._message = f"[{method}] {details or 'permission denied'}"
        self._details = details

    def cancel(self) -> bool:  # noqa: PLR6301
        return False

    def cancelled(self) -> bool:  # noqa: PLR6301
        return False

    def done(self) -> bool:  # noqa: PLR6301
        return True

    def add_done_callback(self, unused_callback: Any) -> None:
        """No-op: the call is already finished.

        Args:
            unused_callback: Ignored; present to satisfy the call interface.
        """

    def time_remaining(self) -> float | None:  # noqa: PLR6301
        return None

    async def initial_metadata(self) -> grpc.aio.Metadata:  # noqa: PLR6301
        return grpc.aio.Metadata()

    async def trailing_metadata(self) -> grpc.aio.Metadata:  # noqa: PLR6301
        return grpc.aio.Metadata()

    async def code(self) -> grpc.StatusCode:  # noqa: PLR6301
        return grpc.StatusCode.PERMISSION_DENIED

    async def details(self) -> str:
        return self._details

    async def debug_error_string(self) -> None:  # noqa: PLR6301
        return None

    async def wait_for_connection(self) -> None:
        """No-op: the call already resolved to a permission denial."""

    def __await__(self) -> Any:
        raise PermissionDeniedError(self._message)
        yield  # unreachable; marks this method a generator so it is awaitable


class PermissionClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """Map a ``PERMISSION_DENIED`` unary reply to ``PermissionDeniedError``."""

    async def intercept_unary_unary(  # noqa: PLR6301
        self,
        continuation: Callable[[grpc.aio.ClientCallDetails, Any], Awaitable[Any]],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        """Return a denied call on PERMISSION_DENIED, else pass the real call through.

        Args:
            continuation: Downstream call continuation.
            client_call_details: Original call details.
            request: Request message.

        Returns:
            The downstream call, or a terminal call raising PermissionDeniedError on await.
        """
        call = await continuation(client_call_details, request)
        if await call.code() == grpc.StatusCode.PERMISSION_DENIED:
            method = client_call_details.method
            method_name = method.decode() if isinstance(method, bytes) else method
            details = await call.details()
            logger.warning("permission denied on %s: %s", method_name, details or "")
            return _DeniedUnaryUnaryCall(method_name, details)
        return call
