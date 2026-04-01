"""Functional tests for GatewayServicer — all 4 RPCs.

Tests with mocked CommunicationStrategy, RegistryStrategy, and RedisClient.
Covers: StartStream ACK, ProduceStream BiDi, ConsumeStream BiDi, SendSignal,
ModuleStartInfo injection, utility protocol fast path, session lifecycle.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

pytestmark = [pytest.mark.timeout(15)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeRequestIterator:
    """Simulates a gRPC BiDi request stream."""

    def __init__(self, messages: list[Any]) -> None:
        self._messages = list(messages)
        self._index = 0

    def __aiter__(self) -> _FakeRequestIterator:
        return self

    async def __anext__(self) -> Any:
        if self._index >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._index]
        self._index += 1
        return msg


def _make_proto_msg(oneof_field: str, **kwargs: Any) -> MagicMock:
    """Build a mock proto message with WhichOneof support."""
    msg = MagicMock()
    msg.WhichOneof.return_value = oneof_field
    for k, v in kwargs.items():
        setattr(msg, k, v)
    return msg


def _make_init_msg(task_id: str, from_seq: int = 0) -> MagicMock:
    """Build a ConsumeStream init message."""
    msg = _make_proto_msg("init")
    msg.init.task_id = task_id
    msg.init.from_seq = from_seq
    return msg


def _make_produce_init(task_id: str) -> MagicMock:
    """Build a ProduceStream init message."""
    msg = _make_proto_msg("init")
    msg.init.task_id = task_id
    return msg


def _make_produce_output(data_struct: Any) -> MagicMock:
    """Build a ProduceStream output message."""
    msg = _make_proto_msg("output")
    msg.output.data = data_struct
    return msg


def _make_consume_data(task_id: str, data_struct: Any) -> MagicMock:
    """Build a ConsumeStream data message."""
    msg = _make_proto_msg("data")
    msg.data.task_id = task_id
    msg.data.data = data_struct
    return msg


def _mock_context() -> MagicMock:
    """Build a mock gRPC ServicerContext with invocation_metadata."""
    ctx = MagicMock()
    ctx.invocation_metadata.return_value = []
    return ctx


def _mock_servicer(
    redis_client: Any = "default_mock",
    **kwargs: Any,
) -> Any:
    """Create a GatewayServicer with mocked dependencies."""
    from digitalkin.grpc_servers.gateway_servicer import GatewayServicer

    if redis_client == "default_mock":
        redis_client = MagicMock()
        redis_client.eval = AsyncMock(return_value=1)
        redis_client.xlen = AsyncMock(return_value=0)
        redis_client.xadd = AsyncMock(return_value=b"1-0")
        redis_client.xread = AsyncMock(return_value=[])
        redis_client.xrevrange = AsyncMock(return_value=[])
        redis_client.expire = AsyncMock(return_value=True)
        redis_client.hset = AsyncMock(return_value=1)
        redis_client.publish = AsyncMock(return_value=0)
        redis_client.get = AsyncMock(return_value=None)
        redis_client.set = AsyncMock(return_value=True)
        pipe_mock = MagicMock()
        pipe_mock.xadd = MagicMock(return_value=pipe_mock)
        pipe_mock.execute = AsyncMock(return_value=[])
        redis_client.pipeline = MagicMock(return_value=pipe_mock)

    return GatewayServicer(
        redis_client=redis_client,
        max_streams=100,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_registry() -> Generator[None]:
    """Ensure clean state between tests."""
    yield


# ===========================================================================
# StartStream
# ===========================================================================


class TestStartStream:
    """StartStream: unary RPC, ACK-only response."""

    async def test_returns_ack_with_task_id(self) -> None:
        """StartStream returns accepted=True and echoes task_id."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = "task_start_1"
        request.setup_id = "setups:s1"
        request.mission_id = "missions:m1"

        # Non-utility input
        from google.protobuf import struct_pb2

        input_struct = struct_pb2.Struct()
        input_struct.update({"root": {"protocol": "message", "content": "hello"}})
        request.input = input_struct

        context = _mock_context()
        response = await servicer.StartStream(request, context)

        assert response.task_id == "task_start_1"
        assert response.accepted is True

    async def test_utility_protocol_goes_through_full_path(self) -> None:
        """Utility protocols go through the same path as regular protocols."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = "task_utility"
        request.setup_id = "setups:test"
        request.mission_id = "missions:test"

        from google.protobuf import struct_pb2

        input_struct = struct_pb2.Struct()
        input_struct.update({"root": {"protocol": "healthcheck_ping"}})
        request.input = input_struct

        context = _mock_context()
        response = await servicer.StartStream(request, context)

        assert response.accepted is True
        # Session should be registered (not shortcutted)
        assert servicer._registry.get("task_utility") is not None

    async def test_capacity_exceeded_returns_not_accepted(self) -> None:
        """When max_streams is exceeded, StartStream returns accepted=False."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2
        except ImportError:
            pytest.skip("Gateway proto not installed")

        mock_redis = MagicMock()
        mock_redis.eval = AsyncMock(return_value=0)  # Lua returns 0 = at capacity
        mock_redis.xlen = AsyncMock(return_value=0)
        servicer = _mock_servicer(redis_client=mock_redis)

        request = MagicMock()
        request.task_id = "task_overflow"
        request.setup_id = "setups:test"
        request.mission_id = "missions:test"

        from google.protobuf import struct_pb2

        input_struct = struct_pb2.Struct()
        input_struct.update({"root": {"protocol": "message"}})
        request.input = input_struct

        context = _mock_context()
        response = await servicer.StartStream(request, context)

        assert response.accepted is False


# ===========================================================================
# SendSignal
# ===========================================================================


class TestSendSignal:
    """SendSignal: unary RPC, signal forwarding."""

    async def test_forwards_signal_via_redis(self) -> None:
        """SendSignal publishes to Redis signal channel."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        # Register a session first
        from digitalkin.grpc_servers.stream_session import StreamSession

        session = StreamSession(task_id="task_sig")
        await servicer._registry.register(session)

        request = MagicMock()
        request.task_id = "task_sig"
        request.action = gateway_pb2.SIGNAL_ACTION_CANCEL

        context = _mock_context()
        response = await servicer.SendSignal(request, context)

        assert response.success is True
        servicer._redis_client.publish.assert_awaited_once()

    async def test_unknown_task_returns_false(self) -> None:
        """SendSignal for unknown task returns success=False."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = "nonexistent"
        request.action = gateway_pb2.SIGNAL_ACTION_CANCEL

        context = _mock_context()
        response = await servicer.SendSignal(request, context)

        assert response.success is False


# ===========================================================================
# ConsumeStream
# ===========================================================================


class TestConsumeStream:
    """ConsumeStream: BiDi RPC, Module B reads output."""

    async def test_unknown_task_yields_error(self) -> None:
        """ConsumeStream for unknown task yields error response."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        init_msg = _make_init_msg("nonexistent_task")
        request_iter = _FakeRequestIterator([init_msg])

        context = _mock_context()
        responses = []
        async for resp in servicer.ConsumeStream(request_iter, context):
            responses.append(resp)

        assert len(responses) == 1
        assert responses[0].HasField("error")

    async def test_first_message_must_be_init(self) -> None:
        """ConsumeStream rejects if first message is not init."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        bad_msg = _make_proto_msg("data")
        request_iter = _FakeRequestIterator([bad_msg])

        context = _mock_context()
        responses = []
        async for resp in servicer.ConsumeStream(request_iter, context):
            responses.append(resp)

        assert len(responses) == 1
        assert responses[0].HasField("error")

    async def test_raises_when_no_redis(self) -> None:
        """GatewayServicer raises at init when no Redis is provided."""
        from digitalkin.grpc_servers.gateway_servicer import GatewayServicer

        with pytest.raises(RuntimeError, match="requires Redis"):
            GatewayServicer(redis_client=None, max_streams=100)


# ===========================================================================
# ProduceStream
# ===========================================================================


class TestProduceStream:
    """ProduceStream: BiDi RPC, Module A sends output."""

    async def test_unknown_task_returns_nothing(self) -> None:
        """ProduceStream for unknown task exits silently."""
        servicer = _mock_servicer()

        init_msg = _make_produce_init("nonexistent")
        request_iter = _FakeRequestIterator([init_msg])

        context = _mock_context()
        responses = []
        async for resp in servicer.ProduceStream(request_iter, context):
            responses.append(resp)

        assert len(responses) == 0

    async def test_persists_output_to_session_queue(self) -> None:
        """ProduceStream puts Module A output on session.output_queue."""
        servicer = _mock_servicer()

        from digitalkin.grpc_servers.stream_session import StreamSession

        session = StreamSession(task_id="task_produce")
        await servicer._registry.register(session)

        from google.protobuf import struct_pb2

        data = struct_pb2.Struct()
        data.update({"msg": "from_module_a"})

        init_msg = _make_produce_init("task_produce")
        output_msg = _make_produce_output(data)

        request_iter = _FakeRequestIterator([init_msg, output_msg])

        context = _mock_context()

        # Consume in background (ProduceStream blocks reading input_queue)
        async def consume() -> list:
            results = []
            async for resp in servicer.ProduceStream(request_iter, context):
                results.append(resp)
            return results

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.1)

        # With Redis, output goes to proto stream (batch mode uses pipeline)
        pipe = servicer._redis_client.pipeline.return_value
        assert pipe.execute.await_count > 0 or servicer._redis_client.xadd.await_count > 0

        session.stop()
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
