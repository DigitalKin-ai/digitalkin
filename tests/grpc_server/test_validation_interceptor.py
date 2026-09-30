"""Tests for ValidationServerInterceptor: inbound requests checked against buf.validate rules."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest
from agentic_mesh_protocol.module.v1 import module_dto_pb2
from google.protobuf import struct_pb2
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_health.v1.health import HealthServicer

from digitalkin.grpc_servers.interceptors.validation import ValidationServerInterceptor
from digitalkin.grpc_servers.module_server import ModuleServer
from digitalkin.models.services.services import ServicesMode
from digitalkin.utils.env_manager import EnvManager

pytestmark = [pytest.mark.timeout(10), pytest.mark.grpc, pytest.mark.validation]


class _AbortError(Exception):
    """Stands in for the exception ``ServicerContext.abort`` raises."""


class TestValidationServerInterceptor:
    """Unary requests are validated; request streams pass through untouched."""

    @staticmethod
    async def _intercept(handler: Any) -> Any:
        return await ValidationServerInterceptor().intercept_service(AsyncMock(return_value=handler), MagicMock())

    @staticmethod
    def _context() -> MagicMock:
        context = MagicMock()
        context.abort = AsyncMock(side_effect=_AbortError)
        return context

    @staticmethod
    def _valid_start_request() -> module_dto_pb2.StartModuleRequest:
        payload = struct_pb2.Struct()
        payload.update({"text": "hello"})
        return module_dto_pb2.StartModuleRequest(setup_id="setups:x", mission_id="missions:x", input=payload)

    async def test_valid_unary_request_reaches_the_servicer(self) -> None:
        behavior = AsyncMock(return_value="ok")
        wrapped = await self._intercept(grpc.unary_unary_rpc_method_handler(behavior))
        request = module_dto_pb2.GetModuleInputRequest(module_id="modules:x")
        context = self._context()

        assert await wrapped.unary_unary(request, context) == "ok"
        behavior.assert_awaited_once_with(request, context)
        context.abort.assert_not_awaited()

    async def test_invalid_unary_request_aborts_with_invalid_argument(self) -> None:
        behavior = AsyncMock(return_value="ok")
        wrapped = await self._intercept(grpc.unary_unary_rpc_method_handler(behavior))
        context = self._context()

        with pytest.raises(_AbortError):
            await wrapped.unary_unary(module_dto_pb2.GetModuleInputRequest(), context)

        behavior.assert_not_awaited()
        code, details = context.abort.await_args.args
        assert code == grpc.StatusCode.INVALID_ARGUMENT
        assert details == "invalid GetModuleInputRequest: module_id: value is required"

    async def test_valid_server_stream_request_yields_every_response(self) -> None:
        async def behavior(request: Any, context: Any) -> Any:  # ruff: ignore[unused-async]
            for chunk in ("a", "b", "c"):
                yield chunk

        wrapped = await self._intercept(grpc.unary_stream_rpc_method_handler(behavior))

        responses = [r async for r in wrapped.unary_stream(self._valid_start_request(), self._context())]

        assert responses == ["a", "b", "c"]

    async def test_invalid_server_stream_request_aborts_before_any_response(self) -> None:
        started = False

        async def behavior(request: Any, context: Any) -> Any:  # ruff: ignore[unused-async]
            nonlocal started
            started = True
            yield "never"

        wrapped = await self._intercept(grpc.unary_stream_rpc_method_handler(behavior))
        context = self._context()

        with pytest.raises(_AbortError):
            async for _ in wrapped.unary_stream(module_dto_pb2.StartModuleRequest(), context):
                pass

        assert started is False
        code, details = context.abort.await_args.args
        assert code == grpc.StatusCode.INVALID_ARGUMENT
        assert details.startswith("invalid StartModuleRequest: ")
        assert "setup_id: value is required" in details
        assert "mission_id: value is required" in details

    async def test_request_streaming_handler_is_returned_unwrapped(self) -> None:
        handler = grpc.stream_stream_rpc_method_handler(MagicMock())

        assert await self._intercept(handler) is handler

    async def test_unknown_method_returns_none(self) -> None:
        assert await self._intercept(None) is None

    @pytest.mark.parametrize(
        "handler",
        [
            grpc.unary_unary_rpc_method_handler(lambda _request, _context: "ok"),
            grpc.unary_stream_rpc_method_handler(lambda _request, _context: iter(["ok"])),
        ],
        ids=["unary_unary", "unary_stream"],
    )
    async def test_sync_handler_is_returned_unwrapped(self, handler: Any) -> None:
        assert await self._intercept(handler) is handler

    async def test_sync_health_check_is_served_through_the_interceptor(self) -> None:
        """The SDK registers grpc_health's synchronous HealthServicer on the aio server."""
        server = grpc.aio.server(interceptors=[ValidationServerInterceptor()])
        servicer = HealthServicer()
        servicer.set("", health_pb2.HealthCheckResponse.SERVING)
        health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                response = await health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest(), timeout=5)
        finally:
            await server.stop(None)

        assert response.status == health_pb2.HealthCheckResponse.SERVING


class TestModuleServerWiring:
    """``ModuleServer`` puts the validation interceptor first, ahead of caller-supplied ones."""

    def test_validation_interceptor_leads_the_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(EnvManager, "services_mode", lambda: ServicesMode.LOCAL)
        custom = MagicMock(spec=grpc.aio.ServerInterceptor)

        server = ModuleServer(MagicMock(), interceptors=[custom])

        assert isinstance(server._interceptors[0], ValidationServerInterceptor)
        assert server._interceptors[1:] == [custom]

    def test_validation_interceptor_installed_without_caller_interceptors(
            self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(EnvManager, "services_mode", lambda: ServicesMode.LOCAL)

        server = ModuleServer(MagicMock())

        assert [type(i) for i in server._interceptors] == [ValidationServerInterceptor]
