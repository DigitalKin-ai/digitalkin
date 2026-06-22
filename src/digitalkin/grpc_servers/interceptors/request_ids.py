"""Request-ID propagation across gRPC via ambient context + interceptors.

Carries ``task_id``/``setup_id``/``mission_id`` as ``x-*`` metadata on every
outbound call (client interceptor) and reads them back into the ambient context
server-side (server interceptor), so the log filter surfaces them on every
record without threading them through call sites.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, ClassVar

import grpc
import grpc.aio

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from contextvars import Token


class RequestContext:
    """Ambient task/setup/mission IDs for the current async context."""

    _ids: ClassVar[ContextVar[dict[str, str]]] = ContextVar("dk_request_ids", default={})

    @classmethod
    def bind(cls, task_id: str = "", setup_id: str = "", mission_id: str = "") -> Token[dict[str, str]]:
        """Set the ambient IDs (non-empty only) and return a reset token.

        Args:
            task_id: Task ID.
            setup_id: Setup ID.
            mission_id: Mission ID.

        Returns:
            Token to pass to ``reset`` in a finally block.
        """
        ids = {k: v for k, v in (("task_id", task_id), ("setup_id", setup_id), ("mission_id", mission_id)) if v}
        return cls._ids.set(ids)

    @classmethod
    def reset(cls, token: Token[dict[str, str]]) -> None:
        """Restore the previous ambient IDs.

        Args:
            token: Token returned by ``bind``.
        """
        cls._ids.reset(token)

    @classmethod
    def current(cls) -> dict[str, str]:
        """Return the current ambient IDs.

        Returns:
            Mapping of the non-empty IDs (empty if unset).
        """
        return cls._ids.get()

    @classmethod
    def as_metadata(cls) -> list[tuple[str, str]]:
        """Return the ambient IDs as gRPC metadata pairs.

        Returns:
            ``x-task-id``/``x-setup-id``/``x-mission-id`` pairs for non-empty IDs.
        """
        return [(f"x-{k.replace('_', '-')}", v) for k, v in cls._ids.get().items()]


class RequestIdClientInterceptor(
    grpc.aio.UnaryUnaryClientInterceptor,
    grpc.aio.UnaryStreamClientInterceptor,
    grpc.aio.StreamUnaryClientInterceptor,
    grpc.aio.StreamStreamClientInterceptor,
):
    """Append ambient request IDs as ``x-*`` metadata on every outbound call."""

    @staticmethod
    def _augment(details: grpc.aio.ClientCallDetails) -> grpc.aio.ClientCallDetails:
        """Return call details with request-ID headers appended.

        Existing keys are preserved (not duplicated). Returns the details
        unchanged when no IDs are bound.

        Args:
            details: Original client call details.

        Returns:
            Call details carrying the request-ID metadata.
        """
        pairs = RequestContext.as_metadata()
        if not pairs:
            return details
        md = grpc.aio.Metadata()
        present: set[str] = set()
        if details.metadata is not None:
            for key, value in details.metadata:
                md.add(key, value)
                present.add(key.lower())
        for key, value in pairs:
            if key not in present:
                md.add(key, value)
        return grpc.aio.ClientCallDetails(
            method=details.method,
            timeout=details.timeout,
            metadata=md,
            credentials=details.credentials,
            wait_for_ready=details.wait_for_ready,
        )

    async def intercept_unary_unary(
        self,
        continuation: Callable[[grpc.aio.ClientCallDetails, Any], Awaitable[Any]],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        """Inject IDs on a unary-unary call.

        Args:
            continuation: Downstream call continuation.
            client_call_details: Original call details.
            request: Request message.

        Returns:
            The downstream call.
        """
        return await continuation(self._augment(client_call_details), request)

    async def intercept_unary_stream(
        self,
        continuation: Callable[[grpc.aio.ClientCallDetails, Any], Awaitable[Any]],
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        """Inject IDs on a unary-stream call.

        Args:
            continuation: Downstream call continuation.
            client_call_details: Original call details.
            request: Request message.

        Returns:
            The downstream call.
        """
        return await continuation(self._augment(client_call_details), request)

    async def intercept_stream_unary(
        self,
        continuation: Callable[[grpc.aio.ClientCallDetails, Any], Awaitable[Any]],
        client_call_details: grpc.aio.ClientCallDetails,
        request_iterator: Any,
    ) -> Any:
        """Inject IDs on a stream-unary call.

        Args:
            continuation: Downstream call continuation.
            client_call_details: Original call details.
            request_iterator: Request message iterator.

        Returns:
            The downstream call.
        """
        return await continuation(self._augment(client_call_details), request_iterator)

    async def intercept_stream_stream(
        self,
        continuation: Callable[[grpc.aio.ClientCallDetails, Any], Awaitable[Any]],
        client_call_details: grpc.aio.ClientCallDetails,
        request_iterator: Any,
    ) -> Any:
        """Inject IDs on a stream-stream call.

        Args:
            continuation: Downstream call continuation.
            client_call_details: Original call details.
            request_iterator: Request message iterator.

        Returns:
            The downstream call.
        """
        return await continuation(self._augment(client_call_details), request_iterator)


class RequestIdServerInterceptor(grpc.aio.ServerInterceptor):
    """Bind inbound ``x-*`` request IDs into the ambient context per call."""

    async def intercept_service(  # noqa: C901, PLR6301
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler[Any, Any] | None]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler[Any, Any] | None:
        """Wrap the resolved handler so it runs with the caller's IDs bound.

        Args:
            continuation: Resolves the next handler.
            handler_call_details: Inbound call details (carries metadata).

        Returns:
            The handler, wrapped to bind/reset the ambient IDs when IDs are present.
        """
        handler = await continuation(handler_call_details)
        if handler is None:
            return handler
        md = {
            k.lower(): (v.decode() if isinstance(v, bytes) else v)
            for k, v in (handler_call_details.invocation_metadata or ())
        }
        task_id = md.get("x-task-id", "")
        setup_id = md.get("x-setup-id", "")
        mission_id = md.get("x-mission-id", "")
        if not (task_id or setup_id or mission_id):
            return handler

        def _wrap_unary(behavior: Any) -> Callable[[Any, Any], Awaitable[Any]]:
            async def _run(request: Any, context: Any) -> Any:
                token = RequestContext.bind(task_id, setup_id, mission_id)
                try:
                    return await behavior(request, context)
                finally:
                    RequestContext.reset(token)

            return _run

        def _wrap_stream(behavior: Any) -> Callable[[Any, Any], Any]:
            async def _run(request: Any, context: Any) -> Any:
                token = RequestContext.bind(task_id, setup_id, mission_id)
                try:
                    async for response in behavior(request, context):
                        yield response
                finally:
                    RequestContext.reset(token)

            return _run

        if handler.request_streaming and handler.response_streaming:
            return grpc.stream_stream_rpc_method_handler(
                _wrap_stream(handler.stream_stream),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if handler.request_streaming:
            return grpc.stream_unary_rpc_method_handler(
                _wrap_unary(handler.stream_unary),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        if handler.response_streaming:
            return grpc.unary_stream_rpc_method_handler(
                _wrap_stream(handler.unary_stream),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        return grpc.unary_unary_rpc_method_handler(
            _wrap_unary(handler.unary_unary),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
