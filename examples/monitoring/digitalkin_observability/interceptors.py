"""gRPC interceptors for automatic metrics collection.

This module provides gRPC server interceptors that automatically track
request duration and errors. Requires grpcio package.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Any

    import grpc

from digitalkin_observability.metrics import get_metrics


class MetricsServerInterceptor:
    """Intercepts all gRPC calls to collect metrics.

    This interceptor automatically tracks:
    - Request duration (histogram)
    - Error counts

    Usage:
        import grpc
        from digitalkin_observability import MetricsServerInterceptor

        interceptors = [MetricsServerInterceptor()]
        server = grpc.aio.server(interceptors=interceptors)
    """

    async def intercept_service(
        self,
        continuation: Callable[["grpc.HandlerCallDetails"], Awaitable["grpc.RpcMethodHandler"]],
        handler_call_details: "grpc.HandlerCallDetails",
    ) -> "grpc.RpcMethodHandler":
        """Intercept a gRPC service call to collect metrics.

        Args:
            continuation: The next interceptor or the actual handler.
            handler_call_details: Details about the call being intercepted.

        Returns:
            The RPC method handler.
        """
        start = time.perf_counter()
        metrics = get_metrics()

        try:
            handler = await continuation(handler_call_details)
            return _MetricsWrappedHandler(handler, start, handler_call_details.method)
        except Exception:
            metrics.inc_errors()
            metrics.observe_grpc_duration(time.perf_counter() - start)
            raise


class _MetricsWrappedHandler:
    """Wrapper that measures actual handler execution time."""

    def __init__(
        self,
        handler: "grpc.RpcMethodHandler",
        start_time: float,
        method: str,
    ) -> None:
        self._handler = handler
        self._start_time = start_time
        self._method = method

        # Copy attributes from original handler
        self.request_streaming = handler.request_streaming
        self.response_streaming = handler.response_streaming
        self.request_deserializer = handler.request_deserializer
        self.response_serializer = handler.response_serializer

        # Wrap the appropriate method based on streaming type
        if handler.unary_unary:
            self.unary_unary = self._wrap_unary_unary(handler.unary_unary)
            self.unary_stream = None
            self.stream_unary = None
            self.stream_stream = None
        elif handler.unary_stream:
            self.unary_unary = None
            self.unary_stream = self._wrap_unary_stream(handler.unary_stream)
            self.stream_unary = None
            self.stream_stream = None
        elif handler.stream_unary:
            self.unary_unary = None
            self.unary_stream = None
            self.stream_unary = self._wrap_stream_unary(handler.stream_unary)
            self.stream_stream = None
        elif handler.stream_stream:
            self.unary_unary = None
            self.unary_stream = None
            self.stream_unary = None
            self.stream_stream = self._wrap_stream_stream(handler.stream_stream)
        else:
            self.unary_unary = None
            self.unary_stream = None
            self.stream_unary = None
            self.stream_stream = None

    def _wrap_unary_unary(
        self,
        handler: Callable[["Any", "grpc.aio.ServicerContext"], Awaitable["Any"]],
    ) -> Callable[["Any", "grpc.aio.ServicerContext"], Awaitable["Any"]]:
        """Wrap a unary-unary handler."""
        async def wrapped(request: "Any", context: "grpc.aio.ServicerContext") -> "Any":
            metrics = get_metrics()
            try:
                return await handler(request, context)
            except Exception:
                metrics.inc_errors()
                raise
            finally:
                metrics.observe_grpc_duration(time.perf_counter() - self._start_time)

        return wrapped

    def _wrap_unary_stream(
        self,
        handler: Callable[["Any", "grpc.aio.ServicerContext"], "Any"],
    ) -> Callable[["Any", "grpc.aio.ServicerContext"], "Any"]:
        """Wrap a unary-stream handler."""
        async def wrapped(request: "Any", context: "grpc.aio.ServicerContext") -> "Any":
            metrics = get_metrics()
            try:
                async for response in handler(request, context):
                    yield response
            except Exception:
                metrics.inc_errors()
                raise
            finally:
                metrics.observe_grpc_duration(time.perf_counter() - self._start_time)

        return wrapped

    def _wrap_stream_unary(
        self,
        handler: Callable[["Any", "grpc.aio.ServicerContext"], Awaitable["Any"]],
    ) -> Callable[["Any", "grpc.aio.ServicerContext"], Awaitable["Any"]]:
        """Wrap a stream-unary handler."""
        async def wrapped(request_iterator: "Any", context: "grpc.aio.ServicerContext") -> "Any":
            metrics = get_metrics()
            try:
                return await handler(request_iterator, context)
            except Exception:
                metrics.inc_errors()
                raise
            finally:
                metrics.observe_grpc_duration(time.perf_counter() - self._start_time)

        return wrapped

    def _wrap_stream_stream(
        self,
        handler: Callable[["Any", "grpc.aio.ServicerContext"], "Any"],
    ) -> Callable[["Any", "grpc.aio.ServicerContext"], "Any"]:
        """Wrap a stream-stream handler."""
        async def wrapped(request_iterator: "Any", context: "grpc.aio.ServicerContext") -> "Any":
            metrics = get_metrics()
            try:
                async for response in handler(request_iterator, context):
                    yield response
            except Exception:
                metrics.inc_errors()
                raise
            finally:
                metrics.observe_grpc_duration(time.perf_counter() - self._start_time)

        return wrapped
