"""End-to-end M2M test: ``GrpcCommunication.call_module``.

The sub-task id is minted by the **backend** GatewayService (``AssociateTask``), then the
tool call is StartStream'd to the **target** module, which dials back with a canned output
stream handled by the caller's real ``GatewayServicer``.

Spins up three real gRPC servers:
- **backend_gateway** — a fake ``GatewayService`` that only serves ``AssociateTask`` (mints the child).
- **callee_gateway** — a fake target that accepts ``StartStream`` and dials back.
- **caller_gateway** — a real ``GatewayServicer`` whose ``Stream`` handles the dial-back.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import grpc
import grpc.aio
import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from google.protobuf import struct_pb2

from digitalkin.grpc_servers.exceptions import ServerError
from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.grpc_servers.interceptors.request_ids import RequestContext
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.utils.channel import SecurityMode
from digitalkin.services.communication.grpc_communication import GrpcCommunication

pytestmark = [pytest.mark.timeout(15)]


def _client(host: str, port: int) -> ClientConfig:
    return ClientConfig(host=host, port=port, security=SecurityMode.INSECURE)


class _FakeBackendGateway(gateway_service_pb2_grpc.GatewayServiceServicer):
    """Backend GatewayService: mints the child task id for AssociateTask."""

    def __init__(self, *, child_task_id: str = "child-1", error: bool = False) -> None:
        self._child = child_task_id
        self._error = error
        self.received_parent_task_id: str = ""
        self.received_metadata: dict[str, str] = {}

    async def AssociateTask(  # noqa: N802
        self, request: Any, context: grpc.aio.ServicerContext
    ) -> Any:
        self.received_parent_task_id = request.parent_task_id
        for k, v in context.invocation_metadata() or ():
            self.received_metadata[k] = v if isinstance(v, str) else v.decode("utf-8")
        if self._error:
            await context.abort(grpc.StatusCode.INTERNAL, "mint boom")
        return gateway_pb2.AssociateTaskResponse(task_id=self._child, parent_task_id=request.parent_task_id)


class _FakeCalleeGatewayServicer(gateway_service_pb2_grpc.GatewayServiceServicer):
    """Target module: accepts StartStream, then dials back to the caller's gateway."""

    def __init__(self, outputs: list[dict[str, Any]]) -> None:
        self._outputs = outputs
        self.received_start: gateway_pb2.StartStreamRequest | None = None
        self.received_metadata: dict[str, str] = {}
        self._dial_tasks: list[asyncio.Task] = []

    async def StartStream(  # noqa: N802
        self, request: Any, context: grpc.aio.ServicerContext
    ) -> Any:
        self.received_start = request
        for k, v in context.invocation_metadata() or ():
            self.received_metadata[k] = v if isinstance(v, str) else v.decode("utf-8")

        dial_back_addr = self.received_metadata.get("x-client-address", "")
        self._dial_tasks.append(
            asyncio.create_task(self._dial_back(dial_back_addr, request.task_id)),
        )
        return gateway_pb2.StartStreamResponse(accepted=True, task_id=request.task_id)

    async def SendSignal(  # noqa: N802
        self,
        request: Any,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> Any:
        return gateway_pb2.ClientSignalResponse(success=True, task_id=request.task_id)

    async def Stream(  # noqa: N802
        self,
        request_iterator: Any,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> AsyncIterator[Any]:
        async for _msg in request_iterator:
            return
        return
        yield  # pragma: no cover — generator typing

    async def _dial_back(self, address: str, task_id: str) -> None:
        async with grpc.aio.insecure_channel(address) as channel:
            stub = gateway_service_pb2_grpc.GatewayServiceStub(channel)

            async def _outgoing() -> AsyncIterator[Any]:
                init = struct_pb2.Struct()
                init.update({"root": {"protocol": "stream.init"}})
                yield gateway_pb2.StreamServer(task_id=task_id, seq=0, data=init)
                for i, payload in enumerate(self._outputs, start=1):
                    out = struct_pb2.Struct()
                    out.update(payload)
                    yield gateway_pb2.StreamServer(task_id=task_id, seq=i, data=out)
                end = struct_pb2.Struct()
                end.update({"root": {"protocol": "stream.end"}})
                yield gateway_pb2.StreamServer(task_id=task_id, seq=len(self._outputs) + 1, data=end)

            responses = stub.Stream(_outgoing(), timeout=10.0)
            try:
                async for _reply in responses:
                    pass  # caller's GatewayServicer yields the query as a StreamClient; we don't need it
            except grpc.aio.AioRpcError:
                pass


@pytest.fixture
async def start_gateway() -> AsyncIterator[Any]:
    """Factory that starts a GatewayService server and returns its port; auto-stopped."""
    servers: list[grpc.aio.Server] = []

    async def _start(servicer: gateway_service_pb2_grpc.GatewayServiceServicer) -> int:
        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        servers.append(server)
        return port

    yield _start
    for s in servers:
        await s.stop(grace=0.1)


@pytest.fixture
async def callee_server(start_gateway: Any) -> tuple[_FakeCalleeGatewayServicer, str, int]:
    servicer = _FakeCalleeGatewayServicer(
        outputs=[
            {"root": {"protocol": "transform", "value": "hello-1"}},
            {"root": {"protocol": "transform", "value": "hello-2"}},
        ]
    )
    port = await start_gateway(servicer)
    return servicer, "127.0.0.1", port


@pytest.fixture
async def backend_server(start_gateway: Any) -> tuple[_FakeBackendGateway, str, int]:
    servicer = _FakeBackendGateway(child_task_id="child-1")
    port = await start_gateway(servicer)
    return servicer, "127.0.0.1", port


@pytest.fixture
async def caller_gateway() -> AsyncIterator[tuple[GatewayServicer, str, int]]:
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock()
    fake_redis.xlen = AsyncMock(return_value=0)
    fake_redis.verify = AsyncMock(return_value=True)
    fake_redis.close = AsyncMock()
    runner = MagicMock()
    runner.run = AsyncMock()
    gw = GatewayServicer(
        redis_client=fake_redis,
        client_config=_client("127.0.0.1", 1),
        module_runner=runner,
    )
    server = grpc.aio.server()
    gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(gw, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    await gw.start()  # start the TTL sweeper
    # Override the gateway's advertise to the actual bound port so dial-back lands here.
    gw._m2m.effective_advertise_address = lambda: f"127.0.0.1:{port}"  # type: ignore[method-assign]
    try:
        yield gw, "127.0.0.1", port
    finally:
        await gw.stop()
        await server.stop(grace=0.1)


class TestM2MCallModule:
    async def test_round_trip_mints_via_backend_then_streams(
        self,
        backend_server: tuple[_FakeBackendGateway, str, int],
        callee_server: tuple[_FakeCalleeGatewayServicer, str, int],
        caller_gateway: tuple[GatewayServicer, str, int],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        backend, backend_host, backend_port = backend_server
        callee_servicer, callee_host, callee_port = callee_server
        gw, _caller_host, _caller_port = caller_gateway

        comm = GrpcCommunication(
            mission_id="missions:test",
            setup_id="setups:test",
            setup_version_id="setup_versions:test",
            client_config=_client(callee_host, callee_port),
            m2m_calls=gw._m2m,
            gateway_backend_config=_client(backend_host, backend_port),
        )

        outputs: list[Any] = []
        token = RequestContext.bind(task_id="task:parent")
        try:
            with caplog.at_level(logging.DEBUG, logger="digitalkin"):
                async for out_struct in comm.call_module(
                    module_address=callee_host,
                    module_port=callee_port,
                    input_data={"root": {"protocol": "transform", "text": "hello"}},
                    setup_id="setups:test",
                    mission_id="missions:test",
                ):
                    outputs.append(out_struct)
        finally:
            RequestContext.reset(token)
            await comm.close()

        # The m2m handshake lines describe the wire protocol, not an outcome — DEBUG only.
        info_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
        assert not any("AssociateTask minted" in m or "StartStream accepted" in m for m in info_messages)

        domain = [o for o in outputs if o.fields["root"].struct_value.fields["protocol"].string_value == "transform"]
        assert [o.fields["root"].struct_value.fields["value"].string_value for o in domain] == [
            "hello-1",
            "hello-2",
        ]

        # The BACKEND minted the child (parent propagated) with an idempotency key.
        assert backend.received_parent_task_id == "task:parent"
        assert "x-idempotency-key" in backend.received_metadata

        # The backend-minted child — not a client uuid — drove StartStream on the TARGET.
        assert callee_servicer.received_start is not None
        assert callee_servicer.received_start.task_id == "child-1"
        assert callee_servicer.received_metadata.get("x-client-address", "").startswith("127.0.0.1:")

        # Registry cleared, semaphore restored.
        from digitalkin.models.settings.gateway import get_gateway_settings

        assert not gw._m2m.entries
        assert gw._m2m._semaphore._value == get_gateway_settings().m2m.call_max_concurrent

    async def test_backend_mint_empty_raises(
        self,
        caller_gateway: tuple[GatewayServicer, str, int],
        start_gateway: Any,
    ) -> None:
        """An empty backend mint aborts before StartStream and leaks nothing."""
        gw, _host, _port = caller_gateway
        backend = _FakeBackendGateway(child_task_id="")
        backend_port = await start_gateway(backend)
        comm = GrpcCommunication(
            mission_id="missions:test",
            setup_id="setups:test",
            setup_version_id="setup_versions:test",
            client_config=_client("127.0.0.1", 9),
            m2m_calls=gw._m2m,
            gateway_backend_config=_client("127.0.0.1", backend_port),
        )

        with pytest.raises(RuntimeError, match="no task_id"):
            async for _ in comm.call_module(
                module_address="127.0.0.1",
                module_port=9,
                input_data={"root": {"protocol": "transform"}},
                setup_id="setups:test",
                mission_id="missions:test",
            ):
                pass

        assert not gw._m2m.entries
        await comm.close()

    async def test_backend_mint_error_raises(
        self,
        caller_gateway: tuple[GatewayServicer, str, int],
        start_gateway: Any,
    ) -> None:
        """A backend mint RPC error surfaces as ServerError, before StartStream, leaking nothing."""
        gw, _host, _port = caller_gateway
        backend = _FakeBackendGateway(error=True)
        backend_port = await start_gateway(backend)
        comm = GrpcCommunication(
            mission_id="missions:test",
            setup_id="setups:test",
            setup_version_id="setup_versions:test",
            client_config=_client("127.0.0.1", 9),
            m2m_calls=gw._m2m,
            gateway_backend_config=_client("127.0.0.1", backend_port),
        )

        with pytest.raises(ServerError):
            async for _ in comm.call_module(
                module_address="127.0.0.1",
                module_port=9,
                input_data={"root": {"protocol": "transform"}},
                setup_id="setups:test",
                mission_id="missions:test",
            ):
                pass

        assert not gw._m2m.entries
        await comm.close()

    async def test_missing_backend_config_raises(
        self,
        caller_gateway: tuple[GatewayServicer, str, int],
    ) -> None:
        """No gateway_backend_config → fail-closed before any network call."""
        gw, _host, _port = caller_gateway
        comm = GrpcCommunication(
            mission_id="missions:test",
            setup_id="setups:test",
            setup_version_id="setup_versions:test",
            client_config=_client("127.0.0.1", 9),
            m2m_calls=gw._m2m,
        )

        with pytest.raises(RuntimeError, match="gateway_backend_config is required"):
            async for _ in comm.call_module(
                module_address="127.0.0.1",
                module_port=9,
                input_data={"root": {"protocol": "transform"}},
                setup_id="setups:test",
                mission_id="missions:test",
            ):
                pass

        assert not gw._m2m.entries
