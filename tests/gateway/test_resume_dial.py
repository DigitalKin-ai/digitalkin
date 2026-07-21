"""Tests for server-side dial-back reconnection (cursor-based resume).

Covers:
- ``StartStream`` uniqueness: a task that is already claimed/running, or has a
  live session, is refused (reconnection is server-driven, not client-triggered).
- ``_run_dial_attempt(resume=True)`` end-to-end against an in-process fake
  consumer: sends ``stream.resume``, reads the cursor from the reply, does NOT
  re-run the module, and drains from the cursor with stored-seq wire labels.
- Full server-side auto re-dial: a fresh consumer dies mid-stream; the gateway
  re-dials the SAME address on its own and resumes from the consumer's cursor,
  delivering only the tail, with exactly one module run.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import grpc.aio
import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from google.protobuf import struct_pb2

from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.grpc_servers.stream_session import StreamSession
from digitalkin.models.core.redis import ClaimResult
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.models.settings.utils.channel import SecurityMode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")
pytestmark = [pytest.mark.timeout(30)]


def _mock_context(metadata: dict[str, str] | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.invocation_metadata.return_value = list(metadata.items()) if metadata else []
    return ctx


def _start_request(task_id: str) -> Any:
    request = MagicMock()
    request.task_id = task_id
    request.setup_id = "setups:test"
    request.mission_id = "missions:test"
    return request


def _protocol_of(msg: Any) -> str:
    root = msg.data.fields.get("root")
    if root is None:
        return ""
    pf = root.struct_value.fields.get("protocol")
    return pf.string_value if pf is not None else ""


async def _seed_stream(redis: Any, task_id: str, n_chunks: int = 6) -> None:
    """Seed a durable stream: stream.start (seq 0) + n chunks (seq 1..n) + eos."""
    key = f"task:{task_id}:stream"
    start = struct_pb2.Struct()
    start.update({"root": {"protocol": "stream.start"}})
    await redis.xadd(key, {"pb": start.SerializeToString(), "seq": "0"})
    for i in range(1, n_chunks + 1):
        s = struct_pb2.Struct()
        s.update({"root": {"protocol": "chunk", "i": i}})
        await redis.xadd(key, {"pb": s.SerializeToString(), "seq": str(i)})
    await redis.xadd(key, {"eos": b"true"})


# ---------------------------------------------------------------------------
# StartStream uniqueness (mocked redis + patched _dial_consumer)
# ---------------------------------------------------------------------------


def _servicer_with_claim(claim: ClaimResult, xlen: int) -> GatewayServicer:
    redis = MagicMock()
    redis.xadd = AsyncMock()
    redis.xlen = AsyncMock(return_value=xlen)
    gw = GatewayServicer(
        redis_client=redis,
        client_config=ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE),
        module_runner=MagicMock(),
    )
    gw._idempotency.claim = AsyncMock(return_value=claim)  # type: ignore[method-assign]
    return gw


class TestStartStreamUniqueness:
    @pytest.mark.parametrize("claim", [ClaimResult.RECLAIMED, ClaimResult.TAKEN])
    async def test_already_claimed_task_is_refused(self, claim: ClaimResult) -> None:
        gw = _servicer_with_claim(claim, xlen=5)
        gw._dial_consumer = AsyncMock()  # type: ignore[method-assign]
        ctx = _mock_context({"x-client-address": "127.0.0.1:50999"})

        resp = await gw.StartStream(_start_request("t_claimed"), ctx)
        await asyncio.sleep(0)

        # Reconnection is server-driven; a re-issued StartStream must NOT re-dial.
        assert resp.accepted is False
        gw._dial_consumer.assert_not_called()
        assert gw._registry.get("t_claimed") is None

    async def test_live_session_is_refused(self) -> None:
        gw = _servicer_with_claim(ClaimResult.CLAIMED, xlen=0)
        gw._dial_consumer = AsyncMock()  # type: ignore[method-assign]
        await gw._registry.register(StreamSession(task_id="t_dup"), setup_id="s", mission_id="m")
        ctx = _mock_context({"x-client-address": "127.0.0.1:50999"})

        resp = await gw.StartStream(_start_request("t_dup"), ctx)

        # A 2nd StartStream while a dial is live is refused at the dedup check.
        assert resp.accepted is False
        gw._dial_consumer.assert_not_called()


# ---------------------------------------------------------------------------
# _run_dial_attempt(resume=True) end-to-end against a fake consumer
# ---------------------------------------------------------------------------


class _FakeRedisClient:
    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()

    async def xadd(self, name: str, fields: dict, *, maxlen: int | None = None) -> bytes:
        kwargs: dict[str, Any] = {}
        if maxlen is not None:
            kwargs["maxlen"] = maxlen
            kwargs["approximate"] = True
        return await self._client.xadd(name, fields, **kwargs)  # type: ignore[return-value]

    async def xread(self, streams: dict, *, count: int = 50, block: int = 0) -> list:
        return await self._client.xread(streams, count=count, block=block)  # type: ignore[return-value]

    async def xlen(self, name: str) -> int:
        return await self._client.xlen(name)  # type: ignore[return-value]

    async def expire(self, name: str, seconds: int) -> bool:
        return await self._client.expire(name, seconds)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        return await self._client.set(name, value, ex=ex)  # type: ignore[return-value]

    async def close(self) -> None:
        await self._client.aclose()


class _ResumeConsumerServicer(gateway_service_pb2_grpc.GatewayServiceServicer):
    """Replies to ``stream.resume`` with its cursor (in ``seq``), then drains."""

    def __init__(self, cursor: int) -> None:
        self.cursor = cursor
        self.received: list[Any] = []

    async def StartStream(self, request, context) -> Any:
        return gateway_pb2.StartStreamResponse(accepted=False, task_id=request.task_id)

    async def SendSignal(self, request, context) -> Any:
        return gateway_pb2.ClientSignalResponse(success=False, task_id=request.task_id)

    async def Stream(self, request_iterator, context) -> AsyncIterator[Any]:
        first = await anext(request_iterator)
        self.received.append(first)
        # Reply with the resume cursor in seq (empty data). StreamServer.seq
        # shares the wire tag with StreamClient.from_seq the gateway reads.
        yield gateway_pb2.StreamServer(seq=self.cursor, task_id=first.task_id)
        async for msg in request_iterator:
            self.received.append(msg)
            if _protocol_of(msg) == "stream.end":
                return


class _FakeModuleRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run(self, query: Any, **kwargs: Any) -> None:
        self.calls.append({"query": query, **kwargs})


@pytest.fixture
async def resume_gateway() -> AsyncIterator[Any]:
    redis = _FakeRedisClient()
    runner = _FakeModuleRunner()
    gw = GatewayServicer(
        redis_client=redis,  # type: ignore[arg-type]
        client_config=ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE),
        module_runner=runner,  # type: ignore[arg-type]
    )
    gw._fake_runner = runner  # type: ignore[attr-defined]
    try:
        yield gw
    finally:
        await gw._registry.shutdown()
        await redis.close()


@SKIP_NO_FAKEREDIS
class TestDialAttemptResume:
    async def _seed(self, gw: Any, task_id: str) -> None:
        await _seed_stream(gw._redis_client, task_id)

    async def _run_resume(self, gw: Any, task_id: str, cursor: int) -> _ResumeConsumerServicer:
        servicer = _ResumeConsumerServicer(cursor=cursor)
        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            await gw._registry.register(StreamSession(task_id=task_id), setup_id="setups:t", mission_id="missions:t")
            await gw._run_dial_attempt(
                task_id=task_id,
                mission_id="missions:t",
                setup_id="setups:t",
                address=f"127.0.0.1:{port}",
                resume=True,
                on_runner_spawn=lambda: None,
            )
        finally:
            await server.stop(grace=0.1)
        return servicer

    async def test_resume_reads_cursor_skips_runner_drains_from_cursor(self, resume_gateway) -> None:
        await self._seed(resume_gateway, "t_e2e")
        servicer = await self._run_resume(resume_gateway, "t_e2e", cursor=4)

        # First inbound message is stream.resume.
        assert _protocol_of(servicer.received[0]) == "stream.resume"
        # Module runner is NOT invoked on resume.
        assert resume_gateway._fake_runner.calls == []
        # Drained frames: stored seq 4,5,6 → wire 5,6,7; then stream.end at 8.
        drained = servicer.received[1:]
        assert _protocol_of(drained[-1]) == "stream.end"
        assert [m.seq for m in drained] == [5, 6, 7, 8]

    async def test_resume_cursor_zero_replays_everything(self, resume_gateway) -> None:
        await self._seed(resume_gateway, "t_full")
        servicer = await self._run_resume(resume_gateway, "t_full", cursor=0)

        drained = servicer.received[1:]
        # cursor 0 → skip_to_seq -1 → full replay incl. stream.start (wire 1).
        assert [m.seq for m in drained] == [1, 2, 3, 4, 5, 6, 7, 8]
        assert _protocol_of(drained[0]) == "stream.start"


# ---------------------------------------------------------------------------
# Full server-side auto re-dial: one StartStream, gateway re-dials on its own
# ---------------------------------------------------------------------------


class _WritingRunner:
    """A ModuleRunner stand-in that writes real output to the durable stream.

    ``pace_s`` spaces the writes so the gateway's drain delivers one frame at a
    time instead of dumping every frame + ``eos`` into the BiDi flow-control
    window at once. A reconnect test relies on this (plus a consumer that aborts
    mid-stream) so the fresh dial provably dies before ``eos`` is delivered and
    the re-dial is deterministic.
    """

    def __init__(self, redis: Any, n_chunks: int = 6, *, pace_s: float = 0.0) -> None:
        self._redis = redis
        self._n = n_chunks
        self._pace_s = pace_s
        self.calls: list[str] = []

    async def run(self, query: Any, *, task_id: str, setup_id: str, mission_id: str, on_fatal: Any) -> None:
        self.calls.append(task_id)
        key = f"task:{task_id}:stream"
        for i in range(1, self._n + 1):
            if self._pace_s:
                await asyncio.sleep(self._pace_s)
            s = struct_pb2.Struct()
            s.update({"root": {"protocol": "chunk", "i": i}})
            await self._redis.xadd(key, {"pb": s.SerializeToString(), "seq": str(i)})
        await self._redis.xadd(key, {"eos": b"true"})


class _ReconnectingConsumer(gateway_service_pb2_grpc.GatewayServiceServicer):
    """Two connections on one address: a fresh dial that dies, then a resume.

    The fresh dial dies after ``read_limit`` frames; the gateway's auto re-dial
    (2nd connection) resumes from the last seq the fresh connection saw.
    """

    def __init__(self, read_limit: int) -> None:
        self.read_limit = read_limit
        self.connections = 0
        self.fresh_received: list[Any] = []
        self.resume_received: list[Any] = []
        self.last_seq = 0

    async def StartStream(self, request, context) -> Any:
        return gateway_pb2.StartStreamResponse(accepted=False, task_id=request.task_id)

    async def SendSignal(self, request, context) -> Any:
        return gateway_pb2.ClientSignalResponse(success=False, task_id=request.task_id)

    async def Stream(self, request_iterator, context) -> AsyncIterator[Any]:
        self.connections += 1
        first = await anext(request_iterator)
        if self.connections == 1:
            # Fresh dial: reply with a query, then die after read_limit frames.
            self.fresh_received.append(first)
            query = struct_pb2.Struct()
            query.update({"root": {"protocol": "ask"}})
            yield gateway_pb2.StreamServer(seq=0, task_id=first.task_id, data=query)
            seen = 0
            async for msg in request_iterator:
                self.fresh_received.append(msg)
                self.last_seq = max(self.last_seq, msg.seq)
                seen += 1
                if seen >= self.read_limit:
                    # Hard-abort mid-stream (before eos) so the gateway's dial
                    # deterministically sees a disconnect and re-dials.
                    await context.abort(grpc.StatusCode.CANCELLED, "consumer done")
        else:
            # Auto re-dial: resume from the last seq the fresh connection saw.
            self.resume_received.append(first)
            yield gateway_pb2.StreamServer(seq=self.last_seq, task_id=first.task_id)
            async for msg in request_iterator:
                self.resume_received.append(msg)
                if _protocol_of(msg) == "stream.end":
                    return


async def _serve(servicer: Any) -> tuple[Any, str]:
    server = grpc.aio.server()
    gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


def _new_gateway(redis: Any, runner: Any) -> GatewayServicer:
    return GatewayServicer(
        redis_client=redis,
        client_config=ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE),
        module_runner=runner,
    )


@SKIP_NO_FAKEREDIS
class TestServerSideReconnect:
    async def test_auto_redial_delivers_only_tail_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Gateway auto re-dials a dead consumer and delivers only the tail, once.

        One StartStream; the fresh consumer dies after 3 frames; the gateway
        auto re-dials the SAME address and resumes from the cursor — no gap, no
        dup, exactly one module run.
        """
        monkeypatch.setenv("DIGITALKIN_GATEWAY_DIAL_BACK_RECONNECT_BACKOFF_BASE_S", "0.05")
        monkeypatch.setenv("DIGITALKIN_GATEWAY_DIAL_BACK_RECONNECT_BACKOFF_MAX_S", "0.1")
        monkeypatch.setenv("DIGITALKIN_GATEWAY_DIAL_BACK_RECONNECT_WINDOW_S", "10")
        get_gateway_settings.cache_clear()

        redis = _FakeRedisClient()
        # Pace writes so the drain delivers one frame at a time; the consumer
        # aborts after 3 → the fresh dial dies well before eos (frame 7).
        runner = _WritingRunner(redis, n_chunks=6, pace_s=0.01)
        gw = _new_gateway(redis, runner)
        gw._idempotency.claim = AsyncMock(return_value=ClaimResult.CLAIMED)  # type: ignore[method-assign]
        task_id = "t_autoredial"
        consumer = _ReconnectingConsumer(read_limit=3)
        server, address = await _serve(consumer)
        try:
            resp = await gw.StartStream(_start_request(task_id), _mock_context({"x-client-address": address}))
            assert resp.accepted is True
            for _ in range(200):
                if any(_protocol_of(m) == "stream.end" for m in consumer.resume_received):
                    break
                await asyncio.sleep(0.05)
        finally:
            await gw._registry.shutdown()  # cancel the dial-back task; don't leak it past the test
            await server.stop(grace=0.1)
            get_gateway_settings.cache_clear()

        assert runner.calls == [task_id]  # module ran exactly once (never re-run)
        seen1 = [m.seq for m in consumer.fresh_received[1:]]  # skip inbound stream.init
        assert seen1 == [1, 2, 3]
        assert _protocol_of(consumer.resume_received[0]) == "stream.resume"
        seen2 = [m.seq for m in consumer.resume_received[1:]]
        assert seen2 == [4, 5, 6, 7, 8]  # continues from cursor 3 through stream.end
        assert sorted(seen1 + seen2) == [1, 2, 3, 4, 5, 6, 7, 8]  # every label once
        for _ in range(40):
            if gw._registry.get(task_id) is None:
                break
            await asyncio.sleep(0.05)
        assert gw._registry.get(task_id) is None  # session torn down after completion
