"""Tests for PermissionClientInterceptor — the client-side permission middleware.

Unit tests (``@pytest.mark.unit``) drive the interceptor with a fake continuation;
integration tests (``@pytest.mark.grpc``/``integration``) run real ``grpc.aio``
round-trips — including a regression guard that the interceptor emits no ``__del__``
GC noise, and a concurrency check that a shared interceptor keeps mixed calls isolated.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

import grpc
import grpc.aio
import pytest
from google.protobuf import struct_pb2

from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.grpc_servers.interceptors.permission import PermissionClientInterceptor

pytestmark = [pytest.mark.timeout(15)]


class _FakeCall:
    """Minimal stand-in for an aio call exposing awaitable code()/details()."""

    def __init__(self, code: grpc.StatusCode, details: str = "") -> None:
        self._code = code
        self._details = details

    async def code(self) -> grpc.StatusCode:
        return self._code

    async def details(self) -> str:
        return self._details


def _details() -> grpc.aio.ClientCallDetails:
    return grpc.aio.ClientCallDetails(
        method="/svc/Method",
        timeout=None,
        metadata=None,
        credentials=None,
        wait_for_ready=None,
    )


def _continuation(call: _FakeCall) -> Any:
    async def _run(details: Any, request: Any) -> _FakeCall:  # noqa: RUF029
        return call

    return _run


def _struct(data: dict[str, Any]) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    s.update(data)
    return s


@pytest.mark.unit
class TestPermissionClientInterceptorUnit:
    @pytest.mark.smoke
    async def test_permission_denied_returns_call_that_raises(self) -> None:
        """PERMISSION_DENIED yields a terminal call raising PermissionDeniedError when awaited."""
        call = _FakeCall(grpc.StatusCode.PERMISSION_DENIED, "tenant mismatch")

        result = await PermissionClientInterceptor().intercept_unary_unary(_continuation(call), _details(), object())

        assert await result.code() == grpc.StatusCode.PERMISSION_DENIED
        with pytest.raises(PermissionDeniedError, match="tenant mismatch"):
            await result

    @pytest.mark.parametrize(
        "code",
        [
            grpc.StatusCode.OK,
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.UNAUTHENTICATED,
        ],
    )
    async def test_non_permission_codes_pass_through(self, code: grpc.StatusCode) -> None:
        """Only PERMISSION_DENIED is intercepted; every other code returns the real call untouched."""
        call = _FakeCall(code, "detail")

        result = await PermissionClientInterceptor().intercept_unary_unary(_continuation(call), _details(), object())
        assert result is call


class _GenericHandler(grpc.GenericRpcHandler):
    """Serves a single /probe.Svc/Call whose behavior is injected."""

    def __init__(self, behavior: Any) -> None:
        self._handlers = {
            "/probe.Svc/Call": grpc.unary_unary_rpc_method_handler(
                behavior,
                request_deserializer=struct_pb2.Struct.FromString,
                response_serializer=struct_pb2.Struct.SerializeToString,
            )
        }

    def service(self, handler_call_details: Any) -> Any:
        return self._handlers.get(handler_call_details.method)


@pytest.mark.grpc
@pytest.mark.integration
class TestPermissionClientInterceptorIntegration:
    async def _serve(self, behavior: Any) -> Any:
        """Start a real server + intercepted channel; return (call_method, aclose)."""
        server = grpc.aio.server()
        server.add_generic_rpc_handlers((_GenericHandler(behavior),))
        port = server.add_insecure_port("[::]:0")
        await server.start()
        channel = grpc.aio.insecure_channel(f"localhost:{port}", interceptors=[PermissionClientInterceptor()])
        method = channel.unary_unary(
            "/probe.Svc/Call",
            request_serializer=struct_pb2.Struct.SerializeToString,
            response_deserializer=struct_pb2.Struct.FromString,
        )

        async def _aclose() -> None:
            await channel.close()
            await server.stop(0)

        return method, _aclose

    async def _roundtrip(self, behavior: Any) -> Any:
        method, aclose = await self._serve(behavior)
        try:
            return await method(struct_pb2.Struct())
        finally:
            await aclose()

    @pytest.mark.smoke
    @pytest.mark.regression
    async def test_permission_denied_converted_without_gc_noise(self) -> None:
        """A real PERMISSION_DENIED becomes PermissionDeniedError and leaves no unraisable __del__ noise."""
        import gc

        async def _deny(request: Any, context: Any) -> Any:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "tenant mismatch")

        unraisable: list[str] = []
        previous_hook = sys.unraisablehook
        sys.unraisablehook = lambda arg: unraisable.append(repr(arg.exc_value))
        try:
            with pytest.raises(PermissionDeniedError, match="tenant mismatch"):
                await self._roundtrip(_deny)
            gc.collect()
        finally:
            sys.unraisablehook = previous_hook

        assert unraisable == []

    @pytest.mark.edge_case
    async def test_other_error_stays_aio_rpc_error(self) -> None:
        """Non-permission codes still surface as AioRpcError so retry/breaker logic is unaffected."""

        async def _not_found(request: Any, context: Any) -> Any:
            await context.abort(grpc.StatusCode.NOT_FOUND, "missing")

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await self._roundtrip(_not_found)
        assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    @pytest.mark.smoke
    async def test_success_passes_through(self) -> None:
        """A successful call returns its response unchanged."""

        async def _ok(request: Any, context: Any) -> Any:  # noqa: RUF029
            return struct_pb2.Struct()

        result = await self._roundtrip(_ok)
        assert isinstance(result, struct_pb2.Struct)

    @pytest.mark.concurrency
    async def test_concurrent_mixed_calls_are_isolated(self) -> None:
        """A shared interceptor keeps concurrent denied/allowed calls isolated (no cross-talk)."""

        async def _behavior(request: Any, context: Any) -> Any:
            if request.fields["deny"].bool_value:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "denied")
            return request

        method, aclose = await self._serve(_behavior)
        try:
            requests = [_struct({"deny": bool(i % 2 == 0), "i": i}) for i in range(20)]
            results = await asyncio.gather(*(method(r) for r in requests), return_exceptions=True)
        finally:
            await aclose()

        for i, result in enumerate(results):
            if i % 2 == 0:
                assert isinstance(result, PermissionDeniedError)
            else:
                assert isinstance(result, struct_pb2.Struct)
                assert result.fields["i"].number_value == i  # response matches its own request
