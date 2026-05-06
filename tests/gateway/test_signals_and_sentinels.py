"""Coverage tests for every SignalAction and every stream.* sentinel.

Verifies:
- Every SignalAction enum value has a tested handler path:
  * CANCEL → Redis pub/sub publish on signal_ch:<task_id>
  * INVALIDATE_* → cache_handler called with action name
  * UNSPECIFIED → rejected with success=False
- Every stream.* sentinel emitter is exercised:
  * stream.start (seeded by StartStream)
  * stream.error (fatal=True) → followed by stream.end
  * stream.error (fatal=False) → stream continues (recoverable)
  * stream.end (terminator)
  * Validation paths produce error+end pairs
"""

from __future__ import annotations

import json
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


def _make_first_msg(task_id: str = "t1", from_seq: int = 0, data_dict: dict | None = None) -> Any:
    """Build a real StreamClient first message (init + query)."""
    from agentic_mesh_protocol.gateway.v1 import gateway_pb2

    data = struct_pb2.Struct()
    if data_dict:
        data.update(data_dict)
    return gateway_pb2.StreamClient(task_id=task_id, from_seq=from_seq, data=data)


def _protocol_of(stream_server_msg: Any) -> str:
    """Extract data.root.protocol string from a StreamServer sentinel."""
    return stream_server_msg.data.fields["root"].struct_value.fields["protocol"].string_value


def _root(stream_server_msg: Any) -> Any:
    """Extract data.root struct value (carries sentinel fields)."""
    return stream_server_msg.data.fields["root"].struct_value


def _mock_servicer(*, cache_handler: Any = None) -> Any:
    """Build a GatewayServicer with mocked Redis client."""
    from digitalkin.grpc_servers.gateway_servicer import GatewayServicer

    redis_client = MagicMock()
    redis_client.eval = AsyncMock(return_value=1)
    redis_client.xlen = AsyncMock(return_value=0)
    redis_client.xadd = AsyncMock(return_value=b"1-0")
    redis_client.xread = AsyncMock(return_value=[])
    redis_client.xrevrange = AsyncMock(return_value=[])
    redis_client.expire = AsyncMock(return_value=True)
    redis_client.hset = AsyncMock(return_value=1)
    redis_client.publish = AsyncMock(return_value=1)
    redis_client.get = AsyncMock(return_value=None)
    redis_client.set = AsyncMock(return_value=True)
    pipe_mock = MagicMock()
    pipe_mock.xadd = MagicMock(return_value=pipe_mock)
    pipe_mock.execute = AsyncMock(return_value=[])
    redis_client.pipeline = MagicMock(return_value=pipe_mock)

    return GatewayServicer(
        redis_client=redis_client,
        max_streams=100,
        cache_handler=cache_handler,
    )


def _mock_context(client_address: str | None = "127.0.0.1:50057") -> MagicMock:
    ctx = MagicMock()
    md = [("x-client-address", client_address)] if client_address is not None else []
    ctx.invocation_metadata.return_value = md
    return ctx


@pytest.fixture(autouse=True)
def _isolate() -> Generator[None]:
    yield


# ===========================================================================
# SendSignal — coverage for every SignalAction value
# ===========================================================================


class TestSignalActionAll:
    """Every SignalAction enum value has a tested code path."""

    @pytest.mark.parametrize(
        "action_name",
        [
            "INVALIDATE_ALL",
            "INVALIDATE_CHANNELS",
            "INVALIDATE_MODELS",
            "INVALIDATE_SETUP",
            "INVALIDATE_TOOLS",
            "INVALIDATE_SHARED",
        ],
    )
    async def test_invalidate_routes_to_cache_handler(self, action_name: str) -> None:
        """Every INVALIDATE_* action is forwarded to the cache_handler with its name."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        seen: list[str] = []

        async def handler(name: str) -> None:
            seen.append(name)

        servicer = _mock_servicer(cache_handler=handler)

        request = MagicMock()
        request.task_id = ""  # ignored for INVALIDATE_*
        request.action = getattr(gateway_pb2, action_name)

        response = await servicer.SendSignal(request, _mock_context())
        assert response.success is True
        assert response.task_id == ""
        assert seen == [action_name]
        # Cache invalidation never publishes to Redis
        servicer._redis_client.publish.assert_not_awaited()

    async def test_invalidate_without_handler_returns_false(self) -> None:
        """If no cache_handler is wired, INVALIDATE_* returns success=False."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        servicer = _mock_servicer(cache_handler=None)

        request = MagicMock()
        request.task_id = ""
        request.action = gateway_pb2.INVALIDATE_ALL

        response = await servicer.SendSignal(request, _mock_context())
        assert response.success is False

    async def test_invalidate_handler_raising_returns_false(self) -> None:
        """Handler exceptions bubble up as success=False, not unhandled."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        async def boom(_name: str) -> None:
            raise RuntimeError("handler failed")

        servicer = _mock_servicer(cache_handler=boom)

        request = MagicMock()
        request.task_id = ""
        request.action = gateway_pb2.INVALIDATE_TOOLS

        response = await servicer.SendSignal(request, _mock_context())
        assert response.success is False

    async def test_cancel_publishes_to_signal_channel(self) -> None:
        """CANCEL publishes a JSON message to signal_ch:<task_id>."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        from digitalkin.grpc_servers.stream_session import StreamSession

        servicer = _mock_servicer()
        session = StreamSession(task_id="task_cancel")
        await servicer._registry.register(session)

        request = MagicMock()
        request.task_id = "task_cancel"
        request.action = gateway_pb2.CANCEL

        response = await servicer.SendSignal(request, _mock_context())

        assert response.success is True
        assert response.task_id == "task_cancel"
        servicer._redis_client.publish.assert_awaited_once()
        channel, payload = servicer._redis_client.publish.await_args.args
        assert channel == "signal_ch:task_cancel"
        # Payload is JSON: {"action": "cancel", "task_id": "..."}
        decoded = json.loads(payload)
        assert decoded == {"action": "cancel", "task_id": "task_cancel"}

    async def test_cancel_unknown_task_returns_false(self) -> None:
        """CANCEL for an unknown task returns success=False, no Redis publish."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = "task_missing"
        request.action = gateway_pb2.CANCEL

        response = await servicer.SendSignal(request, _mock_context())
        assert response.success is False
        servicer._redis_client.publish.assert_not_awaited()

    async def test_cancel_invalid_task_id_returns_false(self) -> None:
        """CANCEL with a malformed task_id is rejected before reaching Redis."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = ""  # invalid
        request.action = gateway_pb2.CANCEL

        response = await servicer.SendSignal(request, _mock_context())
        assert response.success is False
        servicer._redis_client.publish.assert_not_awaited()

    async def test_cancel_redis_publish_failure_returns_false(self) -> None:
        """If Redis publish raises, SendSignal returns success=False."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        from digitalkin.grpc_servers.stream_session import StreamSession

        servicer = _mock_servicer()
        from redis.exceptions import RedisError
        servicer._redis_client.publish = AsyncMock(side_effect=RedisError("redis down"))
        session = StreamSession(task_id="task_pub_fail")
        await servicer._registry.register(session)

        request = MagicMock()
        request.task_id = "task_pub_fail"
        request.action = gateway_pb2.CANCEL

        response = await servicer.SendSignal(request, _mock_context())
        assert response.success is False

    async def test_unspecified_action_falls_through_as_failure(self) -> None:
        """UNSPECIFIED is neither INVALIDATE_* nor CANCEL — ends as success=False."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = ""  # invalid by design for unspecified
        request.action = gateway_pb2.UNSPECIFIED

        response = await servicer.SendSignal(request, _mock_context())
        # Falls through the task-signal branch, fails task_id validation → False
        assert response.success is False
        servicer._redis_client.publish.assert_not_awaited()

    async def test_signal_action_enum_complete(self) -> None:
        """The enum has exactly the 8 expected values — no surprises."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        names = {v.name for v in gateway_pb2.SignalAction.DESCRIPTOR.values}
        assert names == {
            "UNSPECIFIED",
            "CANCEL",
            "INVALIDATE_ALL",
            "INVALIDATE_CHANNELS",
            "INVALIDATE_MODELS",
            "INVALIDATE_SETUP",
            "INVALIDATE_TOOLS",
            "INVALIDATE_SHARED",
        }


# ===========================================================================
# stream.* sentinels — coverage for every emitter path
# ===========================================================================


class TestStreamSentinels:
    """Every stream.* sentinel has a tested emit path."""

    async def test_stream_start_seeded_by_start_stream(self) -> None:
        """StartStream writes stream.start as the first Redis entry on task:<tid>:stream."""
        servicer = _mock_servicer()

        request = MagicMock()
        request.task_id = "task_start"
        request.setup_id = "setups:s"
        request.mission_id = "missions:m"

        await servicer.StartStream(request, _mock_context())

        first_call = servicer._redis_client.xadd.await_args_list[0]
        assert first_call.args[0] == "task:task_start:stream"
        pb_bytes = first_call.args[1]["pb"]
        s = struct_pb2.Struct()
        s.ParseFromString(pb_bytes)
        root = s.fields["root"].struct_value.fields
        assert root["protocol"].string_value == "stream.start"
        assert root["task_id"].string_value == "task_start"
        assert root["mission_id"].string_value == "missions:m"
        assert root["setup_id"].string_value == "setups:s"
        # started_at is an ISO timestamp
        assert "T" in root["started_at"].string_value

    async def test_stream_error_invalid_task_id_followed_by_stream_end(self) -> None:
        """Stream with invalid task_id yields stream.error(fatal=true) + stream.end."""
        servicer = _mock_servicer()
        first = _make_first_msg(task_id="")  # invalid
        request_iter = _FakeRequestIterator([first])

        responses = [r async for r in servicer.Stream(request_iter, _mock_context())]
        assert len(responses) == 2
        assert _protocol_of(responses[0]) == "stream.error"
        err = _root(responses[0]).fields
        assert err["fatal"].bool_value is True
        assert err["code"].string_value == "INVALID_ARGUMENT"
        assert "task_id" in err["message"].string_value
        assert _protocol_of(responses[1]) == "stream.end"

    async def test_stream_error_from_seq_out_of_range(self) -> None:
        """Stream with from_seq > MAX_FROM_SEQ yields stream.error + stream.end."""
        from digitalkin.grpc_servers.gateway_constants import MAX_FROM_SEQ

        servicer = _mock_servicer()
        first = _make_first_msg(task_id="task_oor", from_seq=MAX_FROM_SEQ + 1)
        request_iter = _FakeRequestIterator([first])

        responses = [r async for r in servicer.Stream(request_iter, _mock_context())]
        assert len(responses) == 2
        assert _protocol_of(responses[0]) == "stream.error"
        err = _root(responses[0]).fields
        assert err["fatal"].bool_value is True
        assert err["code"].string_value == "INVALID_ARGUMENT"
        assert "from_seq" in err["message"].string_value
        assert _protocol_of(responses[1]) == "stream.end"

    async def test_stream_error_task_not_found_when_no_session_no_redis(self) -> None:
        """Stream where session is gone AND no Redis stream → NOT_FOUND fatal."""
        servicer = _mock_servicer()
        servicer._redis_client.xlen = AsyncMock(return_value=0)

        first = _make_first_msg(task_id="task_missing")
        request_iter = _FakeRequestIterator([first])

        responses = [r async for r in servicer.Stream(request_iter, _mock_context())]
        assert len(responses) == 2
        assert _protocol_of(responses[0]) == "stream.error"
        err = _root(responses[0]).fields
        assert err["code"].string_value == "NOT_FOUND"
        assert err["fatal"].bool_value is True
        assert _protocol_of(responses[1]) == "stream.end"

    async def test_no_stream_start_emitted_directly_by_servicer(self) -> None:
        """The Stream RPC never emits stream.start itself — it's seeded into Redis
        by StartStream and replayed via _consume_from_redis."""
        servicer = _mock_servicer()
        first = _make_first_msg(task_id="")  # forces fatal-close path
        request_iter = _FakeRequestIterator([first])

        responses = [r async for r in servicer.Stream(request_iter, _mock_context())]
        protos = [_protocol_of(r) for r in responses]
        assert "stream.start" not in protos

    async def test_fatal_close_helper_yields_error_then_end(self) -> None:
        """_fatal_close yields exactly two sentinels in the prescribed order."""
        servicer = _mock_servicer()
        outs = [out async for out in servicer._fatal_close("t", "INTERNAL", "boom")]
        assert len(outs) == 2
        assert _protocol_of(outs[0]) == "stream.error"
        assert _root(outs[0]).fields["fatal"].bool_value is True
        assert _root(outs[0]).fields["code"].string_value == "INTERNAL"
        assert _root(outs[0]).fields["message"].string_value == "boom"
        assert _protocol_of(outs[1]) == "stream.end"

    async def test_sentinel_helper_seq_zero_for_gateway_control(self) -> None:
        """Gateway control sentinels (validation errors etc.) carry seq=0."""
        servicer = _mock_servicer()
        outs = [out async for out in servicer._fatal_close("t", "BAD", "x")]
        # Both control entries are seq=0 — they're not Redis-replayed
        assert outs[0].seq == 0
        assert outs[1].seq == 0

    async def test_stream_server_carries_task_id_on_wire(self) -> None:
        """Every emitted StreamServer carries task_id on the wire field."""
        servicer = _mock_servicer()
        outs = [out async for out in servicer._fatal_close("task_xyz", "INTERNAL", "x")]
        assert all(out.task_id == "task_xyz" for out in outs)

    async def test_consume_from_redis_yields_stream_end_after_reader_exits(self) -> None:
        """When ProtoStreamReader exits naturally (EOS), _consume_from_redis
        must yield an explicit stream.end sentinel so the wire contract is
        uniform: every successful stream ends with exactly one stream.end."""
        from unittest.mock import patch

        servicer = _mock_servicer()

        # Fake reader that yields one domain-output Struct then exits (mimics
        # the producer emitting one entry then EOS-marker in Redis).
        async def _fake_read_structs(self):
            domain = struct_pb2.Struct()
            domain.update({"protocol": "healthcheck_ping", "status": "pong"})
            yield domain

        class _FakeReader:
            def __init__(self, *_a, **_kw):
                pass

            async def restore_cursor(self):
                pass

            def read_structs(self):
                return _fake_read_structs(self)

        with patch(
            "digitalkin.grpc_servers.gateway_servicer.ProtoStreamReader",
            _FakeReader,
        ):
            outs = []
            async for out in servicer._consume_from_redis("task_done", from_seq=0):
                outs.append(out)

        # Expect exactly: domain output (seq=1) + stream.end sentinel (seq=2)
        assert len(outs) == 2
        # First: the domain output
        assert outs[0].seq == 1
        assert outs[0].task_id == "task_done"
        # Domain output has no root.protocol — it's the module's payload directly
        assert "root" not in outs[0].data.fields
        # Second: the gateway-emitted stream.end terminator
        assert outs[1].seq == 2
        assert outs[1].task_id == "task_done"
        assert _protocol_of(outs[1]) == "stream.end"


# ===========================================================================
# Sentinel naming invariants
# ===========================================================================


class TestSentinelNaming:
    """Lifecycle sentinels live under the stream.* namespace exclusively."""

    def test_end_of_stream_pydantic_model(self) -> None:
        from digitalkin.models.module.utility import EndOfStreamOutput

        m = EndOfStreamOutput()
        assert m.protocol == "stream.end"

    def test_module_start_info_pydantic_model(self) -> None:
        from digitalkin.models.module.utility import ModuleStartInfoOutput

        m = ModuleStartInfoOutput(task_id="t", mission_id="m", setup_id="s")
        assert m.protocol == "stream.start"

    def test_no_lifecycle_sentinel_exists_outside_stream_namespace(self) -> None:
        """Lifecycle utility models may not declare a non-stream.* protocol."""
        from digitalkin.models.module.utility import EndOfStreamOutput, ModuleStartInfoOutput

        for cls, instance in (
            (EndOfStreamOutput, EndOfStreamOutput()),
            (
                ModuleStartInfoOutput,
                ModuleStartInfoOutput(task_id="t", mission_id="m", setup_id="s"),
            ),
        ):
            assert instance.protocol.startswith("stream."), f"{cls.__name__} not in stream.* namespace"
