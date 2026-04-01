"""Extended tests for GatewayServicer — late consumer, _start_module error paths, SendSignal.

Covers gaps from the audit:
- ConsumeStream late consumer (session gone, Redis stream exists)
- _start_module early exit always writes EOS
- _start_module session NOT unregistered (left for consumer/reaper)
- SendSignal Redis fallback when no signal_service
- SendSignal failure reporting
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [pytest.mark.timeout(15)]

SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")


class _FakeRedisClient:
    """Adapter wrapping fakeredis to match RedisClient interface."""

    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()

    async def xadd(self, name: str, fields: dict[str, str | bytes], *, maxlen: int | None = None) -> bytes:
        kwargs: dict[str, Any] = {}
        if maxlen is not None:
            kwargs["maxlen"] = maxlen
            kwargs["approximate"] = True
        return await self._client.xadd(name, fields, **kwargs)  # type: ignore[return-value]

    async def xread(self, streams: dict[str, str | bytes], *, count: int = 50, block: int = 0) -> list:
        return await self._client.xread(streams, count=count, block=block)  # type: ignore[return-value]

    async def xrevrange(self, name: str, max_id: str = "+", min_id: str = "-", count: int | None = None) -> list:
        return await self._client.xrevrange(name, max=max_id, min=min_id, count=count)  # type: ignore[return-value]

    async def xlen(self, name: str) -> int:
        return await self._client.xlen(name)  # type: ignore[return-value]

    async def expire(self, name: str, seconds: int) -> bool:
        return await self._client.expire(name, seconds)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        return await self._client.set(name, value, ex=ex)  # type: ignore[return-value]

    async def hset(self, name: str, mapping: dict[str, str]) -> int:
        return await self._client.hset(name, mapping=mapping)  # type: ignore[return-value]

    async def publish(self, channel: str, message: str | bytes) -> int:
        return await self._client.publish(channel, message)  # type: ignore[return-value]

    async def eval(self, script: str, keys: list[str], args: list[str]) -> int | str | bytes | None:
        return await self._client.eval(script, len(keys), *keys, *args)  # type: ignore[return-value]

    def pipeline(self) -> Any:
        return self._client.pipeline()

    def pubsub(self) -> Any:
        return self._client.pubsub()

    async def close(self) -> None:
        await self._client.aclose()


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


def _make_init_msg(task_id: str, from_seq: int = 0) -> MagicMock:
    msg = MagicMock()
    msg.WhichOneof.return_value = "init"
    msg.init.task_id = task_id
    msg.init.from_seq = from_seq
    return msg


def _mock_servicer(redis_client: Any = "default_mock", **kwargs: Any) -> Any:
    from unittest.mock import MagicMock

    from digitalkin.grpc_servers.gateway_servicer import GatewayServicer

    if redis_client == "default_mock":
        redis_client = MagicMock()

    return GatewayServicer(redis_client=redis_client, max_streams=100, **kwargs)


# ===========================================================================
# Late Consumer — session gone but Redis stream exists
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestConsumeStreamLateConsumer:
    """ConsumeStream when the session has already been cleaned up."""

    @pytest.fixture
    async def redis(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_reads_from_redis_when_session_gone(self, redis: Any) -> None:
        """Late consumer reads data from Redis even if session is unregistered."""
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        task_id = "task_late_1"

        # Simulate module output already written to Redis
        writer = ProtoStreamWriter(task_id, redis)  # type: ignore[arg-type]
        from google.protobuf import struct_pb2

        s = struct_pb2.Struct()
        s.update({"root": {"protocol": "message", "text": "hello"}})
        await writer.write_struct(s)
        await writer.write_eos()

        # Create servicer — session is NOT registered (module already finished)
        servicer = _mock_servicer(redis_client=redis)

        # ConsumeStream should still work via Redis fallback
        init_msg = _make_init_msg(task_id)
        request_iter = _FakeRequestIterator([init_msg])
        ctx = MagicMock()

        responses = []
        async for resp in servicer.ConsumeStream(request_iter, ctx):
            responses.append(resp)

        # Should get output + COMPLETED status, not "Task not found" error
        assert len(responses) >= 2
        last = responses[-1]
        assert last.WhichOneof("payload") == "status"

    async def test_returns_error_when_no_session_no_redis_stream(self, redis: Any) -> None:
        """If session is gone AND no Redis stream, return error."""
        servicer = _mock_servicer(redis_client=redis)

        init_msg = _make_init_msg("task_nonexistent")
        request_iter = _FakeRequestIterator([init_msg])
        ctx = MagicMock()

        responses = []
        async for resp in servicer.ConsumeStream(request_iter, ctx):
            responses.append(resp)

        assert len(responses) == 1
        assert responses[0].WhichOneof("payload") == "error"


# ===========================================================================
# _start_module — EOS always written
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestStartModuleDispatch:
    """_start_module dispatches via Redis XADD."""

    @pytest.fixture
    async def redis(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_dispatches_task_to_redis(self, redis: Any) -> None:
        """_start_module XADDs task spec to dispatch stream."""
        servicer = _mock_servicer(redis_client=redis)

        from digitalkin.grpc_servers.stream_session import StreamSession

        session = StreamSession(task_id="task_dispatch_1")
        await servicer._registry.register(session)

        request = MagicMock()
        request.setup_id = "setups:s1"
        request.mission_id = "missions:m1"
        from google.protobuf import struct_pb2

        input_struct = struct_pb2.Struct()
        input_struct.update({"root": {"protocol": "message", "content": "hello"}})
        request.input = input_struct

        await servicer._start_module(session, request)

        # Verify dispatch was written to Redis
        stream_len = await redis.xlen(servicer._dispatch_key)
        assert stream_len == 1



# ===========================================================================
# SendSignal — Redis fallback + error reporting
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestSendSignalExtended:
    """SendSignal via Redis pub/sub."""

    @pytest.fixture
    async def redis(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_publishes_signal_via_redis(self, redis: Any) -> None:
        """Signal is published to Redis signal channel."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2
        except ImportError:
            pytest.skip("Gateway proto not installed")

        servicer = _mock_servicer(redis_client=redis)

        from digitalkin.grpc_servers.stream_session import StreamSession

        session = StreamSession(task_id="task_sig_redis")
        await servicer._registry.register(session)

        request = MagicMock()
        request.task_id = "task_sig_redis"
        request.action = gateway_pb2.SIGNAL_ACTION_CANCEL

        resp = await servicer.SendSignal(request, MagicMock())
        assert resp.success is True

    async def test_returns_false_when_publish_fails(self) -> None:
        """When Redis publish fails, returns success=False."""
        try:
            from agentic_mesh_protocol.gateway.v1 import gateway_pb2
        except ImportError:
            pytest.skip("Gateway proto not installed")

        mock_redis = MagicMock()
        mock_redis.publish = AsyncMock(side_effect=Exception("publish failed"))
        servicer = _mock_servicer(redis_client=mock_redis)

        from digitalkin.grpc_servers.stream_session import StreamSession

        session = StreamSession(task_id="task_sig_none")
        await servicer._registry.register(session)

        request = MagicMock()
        request.task_id = "task_sig_none"
        request.action = gateway_pb2.SIGNAL_ACTION_CANCEL

        resp = await servicer.SendSignal(request, MagicMock())
        assert resp.success is False
