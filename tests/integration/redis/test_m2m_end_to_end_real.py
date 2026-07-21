"""Real-Redis pair of ``tests/gateway/test_m2m_end_to_end.py``.

Same live stack — stateful backend (AssociateTask mint+register, CheckResourceAccess
authenticating the child), real target GatewayServicer + ModuleServicer + ModuleRunner +
module trigger, real caller — but the target's stream persistence runs on the real Redis
from docker-compose instead of fakeredis.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import grpc.aio
import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_service_pb2_grpc
from agentic_mesh_protocol.user_profile.v1 import user_profile_service_pb2_grpc

from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from tests.gateway.test_m2m_end_to_end import (
    PARENT_TASK_ID,
    SETUP_ID,
    _BackendGateway,
    _BackendState,
    _BackendUserProfile,
    _call_tool,
    _client,
    _protocols,
    _stream_errors,
    _TargetStack,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Generator

pytestmark = [pytest.mark.integration, pytest.mark.grpc, pytest.mark.timeout(30)]


@pytest.fixture(autouse=True)
def _clear_singletons() -> Generator[None]:
    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()
    yield
    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()


@pytest.fixture
async def start_backend() -> AsyncIterator[Any]:
    servers: list[grpc.aio.Server] = []

    async def _start(gateway: _BackendGateway, user_profile: _BackendUserProfile) -> int:
        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(gateway, server)
        user_profile_service_pb2_grpc.add_UserProfileServiceServicer_to_server(user_profile, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        servers.append(server)
        return port

    yield _start
    for s in servers:
        await s.stop(grace=0.1)


@pytest.fixture
async def caller() -> AsyncIterator[tuple[GatewayServicer, int]]:
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock()
    fake_redis.xlen = AsyncMock(return_value=0)
    fake_redis.verify = AsyncMock(return_value=True)
    fake_redis.close = AsyncMock()
    gw = GatewayServicer(
        redis_client=fake_redis,
        client_config=_client("127.0.0.1", 1),
        module_runner=MagicMock(run=AsyncMock()),
    )
    server = grpc.aio.server()
    gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(gw, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    await gw.start()
    gw._m2m.effective_advertise_address = lambda: f"127.0.0.1:{port}"  # type: ignore[method-assign]
    try:
        yield gw, port
    finally:
        await gw.stop()
        await server.stop(grace=0.1)


class TestM2MEndToEndReal:
    @pytest.mark.smoke
    async def test_full_tool_call_on_real_redis(
        self,
        redis_client: Any,
        start_backend: Any,
        caller: tuple[GatewayServicer, int],
    ) -> None:
        """Happy path against real Redis: backend-minted child authenticated, output streamed."""
        state = _BackendState()
        backend_port = await start_backend(_BackendGateway(state), _BackendUserProfile(state))
        target = _TargetStack(backend_port, redis=redis_client)
        await target.start()
        caller_gw, _ = caller
        try:
            outputs = await _call_tool(caller_gw, target, backend_port)
        finally:
            await target.stop()

        assert state.mint_parents == [PARENT_TASK_ID]
        assert state.access_task_ids == ["child-1"]
        assert state.access_setup_ids == [SETUP_ID]
        assert "healthcheck_ping" in _protocols(outputs)
        assert _stream_errors(outputs) == []
        assert not caller_gw._m2m.entries

    @pytest.mark.regression
    async def test_unregistered_child_unauthenticated_on_real_redis(
        self,
        redis_client: Any,
        start_backend: Any,
        caller: tuple[GatewayServicer, int],
    ) -> None:
        """Prod-bug regression against real Redis: unknown child → fatal stream.error."""
        state = _BackendState()
        backend_port = await start_backend(_BackendGateway(state, register_on_mint=False), _BackendUserProfile(state))
        target = _TargetStack(backend_port, redis=redis_client)
        await target.start()
        caller_gw, _ = caller
        try:
            outputs = await _call_tool(caller_gw, target, backend_port)
        finally:
            await target.stop()

        errors = _stream_errors(outputs)
        assert len(errors) == 1
        code, message = errors[0]
        assert code == "MODULE_RUNTIME_ERROR"
        assert "UNAUTHENTICATED" in message
        assert "healthcheck_ping" not in _protocols(outputs)
        assert not caller_gw._m2m.entries
