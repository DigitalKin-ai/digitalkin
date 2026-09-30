"""Inbound request validation against the protocol's ``buf.validate`` rules."""

from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any

import celpy
import grpc
import grpc.aio
import protovalidate

from digitalkin.logger import logger
from digitalkin.models.settings.server.server import get_server_settings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable


class ValidationServerInterceptor(grpc.aio.ServerInterceptor):
    """Reject a unary request that breaks its ``buf.validate`` rules with ``INVALID_ARGUMENT``.

    A validator failure (rule compilation or evaluation) aborts with ``INTERNAL``.

    Request-streaming RPCs pass through untouched: the gateway ``Stream`` reports
    failures in-band as ``stream.error`` and never aborts. Synchronous handlers
    (e.g. the ``grpc_health`` ``HealthServicer``) also pass through untouched:
    ``grpc.aio`` runs them on its migration thread pool, which an ``async`` wrapper
    would bypass by awaiting their plain return value.
    """

    async def intercept_service(  # ruff: ignore[no-self-use, complex-structure]
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler[Any, Any] | None]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler[Any, Any] | None:
        """Wrap unary-request handlers so the request is validated before the servicer runs.

        Args:
            continuation: Resolves the next handler.
            handler_call_details: Inbound call details.

        Returns:
            The handler, wrapped when its request is unary.
        """
        handler = await continuation(handler_call_details)
        if handler is None or handler.request_streaming:
            return handler
        behavior = handler.unary_stream if handler.response_streaming else handler.unary_unary
        if not (inspect.iscoroutinefunction(behavior) or inspect.isasyncgenfunction(behavior)):
            return handler

        async def _validate(request: Any, context: grpc.aio.ServicerContext) -> None:
            try:
                # Validation costs ~0.3 ms/KiB of CPU: large requests leave the event loop.
                if request.ByteSize() > get_server_settings().grpc.validation_offload_bytes:
                    await asyncio.to_thread(protovalidate.validate, request)
                else:
                    protovalidate.validate(request)
            except protovalidate.ValidationError as e:
                details = "; ".join(
                    f"{'.'.join(el.field_name for el in v.proto.field.elements) or v.proto.rule_id}: {v.proto.message}"
                    for v in e.violations[:10]
                )
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"invalid {request.DESCRIPTOR.name}: {details}")
            except (protovalidate.CompilationError, celpy.CELEvalError):
                # A rule that fails to compile or evaluate is a server defect; keep its text out of the reply.
                logger.exception("Request validation could not run for %s", request.DESCRIPTOR.full_name)
                await context.abort(grpc.StatusCode.INTERNAL, f"could not validate {request.DESCRIPTOR.name}")

        if handler.response_streaming:
            stream_behavior: Any = handler.unary_stream

            async def _run_stream(request: Any, context: grpc.aio.ServicerContext) -> AsyncIterator[Any]:
                await _validate(request, context)
                async for response in stream_behavior(request, context):
                    yield response

            return grpc.unary_stream_rpc_method_handler(
                _run_stream,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        unary_behavior: Any = handler.unary_unary

        async def _run(request: Any, context: grpc.aio.ServicerContext) -> Any:
            await _validate(request, context)
            return await unary_behavior(request, context)

        return grpc.unary_unary_rpc_method_handler(
            _run,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
