"""Functional tests for GatewayServicer — 3 RPCs.

Tests with mocked RedisClient. Covers: StartStream ACK, Stream BiDi
(success + sentinel-based error paths), SendSignal, stream.start
seeding, session lifecycle.

Errors are emitted as ``stream.error(fatal=true)`` followed by
``stream.end`` — never via ``context.abort``. Tests assert the
sentinel sequence on the failure paths.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from google.protobuf import struct_pb2

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


def _make_stream_request(task_id: str = "", seq: int = 0, data_dict: dict | None = None) -> Any:
    """Build a real Stream request proto (dev2: client sends StreamServer)."""
    from agentic_mesh_protocol.gateway.v1 import gateway_pb2

    data = struct_pb2.Struct()
    if data_dict:
        data.update(data_dict)
    return gateway_pb2.StreamServer(task_id=task_id, seq=seq, data=data)


def _protocol_of(stream_output: Any) -> str:
    """Extract data.root.protocol string from a StreamOutput sentinel."""
    return stream_output.data.fields["root"].struct_value.fields["protocol"].string_value


def _mock_context(client_address: str | None = "127.0.0.1:50057") -> MagicMock:
    """Build a mock gRPC ServicerContext with invocation_metadata.

    Default carries a valid x-client-address so StartStream proceeds;
    pass ``None`` to omit it (e.g. to assert the rejection path).
    """
    ctx = MagicMock()
    md = [("x-client-address", client_address)] if client_address is not None else []
    ctx.invocation_metadata.return_value = md
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
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2  # noqa: F401
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = "task_start_1"
        request.setup_id = "setups:s1"
        request.mission_id = "missions:m1"

        context = _mock_context()
        response = await servicer.StartStream(request, context)

        assert response.task_id == "task_start_1"
        assert response.accepted is True

    async def test_session_registered(self) -> None:
        """StartStream registers the session for downstream Stream calls."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2  # noqa: F401
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = "task_registered"
        request.setup_id = "setups:test"
        request.mission_id = "missions:test"

        context = _mock_context()
        response = await servicer.StartStream(request, context)

        assert response.accepted is True
        assert servicer._registry.get("task_registered") is not None

    async def test_capacity_exceeded_returns_not_accepted(self) -> None:
        """When max_streams is exceeded, StartStream returns accepted=False.

        Capacity is now enforced process-locally via _local_cache; pre-fill it
        to the max_streams limit so the next register() returns False.
        """
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2  # noqa: F401
        except ImportError:
            pytest.skip("Gateway proto not installed")

        from digitalkin.grpc_servers.stream_session import StreamSession

        servicer = _mock_servicer()
        # Fill the registry to its max_streams capacity.
        for i in range(servicer._registry._max_streams):
            await servicer._registry.register(StreamSession(task_id=f"prefill_{i}"))

        request = MagicMock()
        request.task_id = "task_overflow"
        request.setup_id = "setups:test"
        request.mission_id = "missions:test"

        context = _mock_context()
        response = await servicer.StartStream(request, context)

        assert response.accepted is False

    async def test_seeds_stream_start_sentinel(self) -> None:
        """StartStream writes a stream.start sentinel as first Redis entry."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2  # noqa: F401
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = "task_seed"
        request.setup_id = "setups:s"
        request.mission_id = "missions:m"

        context = _mock_context()
        await servicer.StartStream(request, context)

        # First xadd is the stream.start seed (key = task:<tid>:stream)
        first_call = servicer._redis_client.xadd.await_args_list[0]
        assert first_call.args[0] == "task:task_seed:stream"
        # Decode the seeded Struct: protocol field == "stream.start"
        pb_bytes = first_call.args[1]["pb"]
        seeded = struct_pb2.Struct()
        seeded.ParseFromString(pb_bytes)
        assert seeded.fields["root"].struct_value.fields["protocol"].string_value == "stream.start"


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

        from digitalkin.grpc_servers.stream_session import StreamSession

        session = StreamSession(task_id="task_sig")
        await servicer._registry.register(session)

        request = MagicMock()
        request.task_id = "task_sig"
        request.action = gateway_pb2.CANCEL

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
        request.action = gateway_pb2.CANCEL

        context = _mock_context()
        response = await servicer.SendSignal(request, context)

        assert response.success is False


# ===========================================================================
# Stream
# ===========================================================================


class TestStream:
    """Stream: BiDi RPC, sentinel-based lifecycle and errors."""

    async def test_unknown_task_yields_fatal_error_then_end(self) -> None:
        """Stream for unknown task yields stream.error(fatal=true) + stream.end."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2  # noqa: F401
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        init_msg = _make_stream_request(task_id="nonexistent_task")
        request_iter = _FakeRequestIterator([init_msg])

        context = _mock_context()
        responses = []
        async for resp in servicer.Stream(request_iter, context):
            responses.append(resp)

        assert len(responses) == 2
        assert _protocol_of(responses[0]) == "stream.error"
        assert responses[0].data.fields["root"].struct_value.fields["fatal"].bool_value is True
        assert responses[0].data.fields["root"].struct_value.fields["code"].string_value == "NOT_FOUND"
        assert _protocol_of(responses[1]) == "stream.end"

    async def test_invalid_task_id_yields_fatal_error_then_end(self) -> None:
        """Stream with an invalid task_id yields the sentinel error sequence."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2  # noqa: F401
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer()

        # Empty task_id fails validation
        init_msg = _make_stream_request(task_id="")
        request_iter = _FakeRequestIterator([init_msg])

        context = _mock_context()
        responses = []
        async for resp in servicer.Stream(request_iter, context):
            responses.append(resp)

        assert len(responses) == 2
        assert _protocol_of(responses[0]) == "stream.error"
        assert responses[0].data.fields["root"].struct_value.fields["fatal"].bool_value is True
        assert responses[0].data.fields["root"].struct_value.fields["code"].string_value == "INVALID_ARGUMENT"
        assert _protocol_of(responses[1]) == "stream.end"

    async def test_from_seq_out_of_range_yields_fatal_error(self) -> None:
        """Stream with from_seq above ``GatewayStreamSettings.from_seq_limit`` yields the sentinel error sequence."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2  # noqa: F401
        except ImportError:
            pytest.skip("Gateway proto not installed")

        from digitalkin.models.settings.gateway import GatewaySettings

        servicer = _mock_servicer()

        init_msg = _make_stream_request(task_id="task_oor", seq=GatewaySettings().stream.from_seq_limit + 1)
        request_iter = _FakeRequestIterator([init_msg])

        context = _mock_context()
        responses = []
        async for resp in servicer.Stream(request_iter, context):
            responses.append(resp)

        assert len(responses) == 2
        assert _protocol_of(responses[0]) == "stream.error"
        assert _protocol_of(responses[1]) == "stream.end"

    async def test_upstream_data_xadds_to_redis_input_stream(self) -> None:
        """Stream: subsequent messages XADD raw proto bytes onto task:{id}:input."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2  # noqa: F401
        except ImportError:
            pytest.skip("Gateway proto not installed")

        from digitalkin.grpc_servers.stream_session import StreamSession

        servicer = _mock_servicer()
        session = StreamSession(task_id="task_up")
        upstream_msg = _make_stream_request(task_id="task_up", data_dict={"msg": "from_consumer"})

        request_iter = _FakeRequestIterator([upstream_msg])

        await servicer._read_peer_upstream(request_iter, "task_up", session)  # noqa: SLF001

        # One XADD on the input stream key with raw proto bytes.
        servicer._redis_client.xadd.assert_awaited_once()  # noqa: SLF001
        args, kwargs = servicer._redis_client.xadd.call_args  # noqa: SLF001
        assert args[0] == "task:task_up:input"
        assert b"pb" in args[1] or "pb" in args[1]

    async def test_upstream_empty_data_skipped(self) -> None:
        """Empty Struct upstream messages are skipped — no XADD."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2  # noqa: F401
        except ImportError:
            pytest.skip("Gateway proto not installed")

        from digitalkin.grpc_servers.stream_session import StreamSession

        servicer = _mock_servicer()
        session = StreamSession(task_id="task_empty")
        empty_msg = _make_stream_request(task_id="task_empty")  # data is empty Struct

        request_iter = _FakeRequestIterator([empty_msg])
        await servicer._read_peer_upstream(request_iter, "task_empty", session)  # noqa: SLF001

        servicer._redis_client.xadd.assert_not_called()  # noqa: SLF001
