"""Tests for the server-initiated dial-back flow.

Covers `GatewayServicer._dial_consumer`:
- Happy path: stream.init → client query → output drain → stream.end.
- No metadata header: gateway does not dial back.
- Consumer never replies: gate is set defensively, no leak.
- Multi-turn upstream: every consumer reply lands on session.input_queue.
- Co-existence with the M2M client-initiated Stream BiDi (regression).

The fake consumer is a real gRPC server (in-process) implementing
`GatewayService.Stream`. The dispatcher is bypassed — tests prime Redis
directly via fakeredis to drive `_consume_from_redis`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Generator
from typing import Any
from unittest.mock import MagicMock

import grpc
import grpc.aio
import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from google.protobuf import struct_pb2

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [pytest.mark.timeout(30)]
SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")


# ---------------------------------------------------------------------------
# Fake Redis adapter (matches the RedisClient interface used by the gateway)
# ---------------------------------------------------------------------------


class _FakeRedisClient:
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


# ---------------------------------------------------------------------------
# Fake consumer-side GatewayService implementing only Stream
# ---------------------------------------------------------------------------


class _FakeConsumerServicer(gateway_service_pb2_grpc.GatewayServiceServicer):
    """Records the BiDi traffic and drives the consumer-side handshake."""

    def __init__(
        self,
        *,
        query_data: dict | None = None,
        extra_upstream: list[dict] | None = None,
        hang: bool = False,
    ) -> None:
        self.received: list[Any] = []
        self.query_data = query_data
        self.extra_upstream = extra_upstream or []
        self.hang = hang

    async def StartStream(self, request, context):
        return gateway_pb2.StartStreamResponse(accepted=False, task_id=request.task_id)

    async def SendSignal(self, request, context):
        return gateway_pb2.ClientSignalResponse(success=False, task_id=request.task_id)

    async def Stream(self, request_iterator, context):
        # Pull the first incoming StreamClient (must be stream.init).
        first = await anext(request_iterator)
        self.received.append(first)

        if self.hang:
            # Don't reply, just keep reading until the call deadline-exceeds.
            try:
                async for msg in request_iterator:
                    self.received.append(msg)
            except Exception:
                return
            return

        # Reply with the user query.
        if self.query_data is not None:
            qstruct = struct_pb2.Struct()
            qstruct.update(self.query_data)
            yield gateway_pb2.StreamServer(seq=0, task_id=first.task_id, data=qstruct)

        # Optional follow-up upstream messages.
        for payload in self.extra_upstream:
            ustruct = struct_pb2.Struct()
            ustruct.update(payload)
            yield gateway_pb2.StreamServer(seq=0, task_id=first.task_id, data=ustruct)

        # Drain any outputs the gateway pushes (it's pushing StreamClients to us).
        try:
            async for msg in request_iterator:
                self.received.append(msg)
                # Stop reading once we see stream.end.
                root = msg.data.fields.get("root")
                if root is not None:
                    pf = root.struct_value.fields.get("protocol")
                    if pf is not None and pf.string_value == "stream.end":
                        return
        except Exception:
            return


@pytest.fixture
async def fake_consumer_server() -> AsyncIterator[tuple[_FakeConsumerServicer, str]]:
    """Spin up an in-process gRPC server with `_FakeConsumerServicer`.

    Yields the servicer (for assertions) and the host:port to dial.
    """
    servicer = _FakeConsumerServicer()
    server = grpc.aio.server()
    gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        yield servicer, f"127.0.0.1:{port}"
    finally:
        await server.stop(grace=0.1)


# ---------------------------------------------------------------------------
# Gateway servicer fixture (uses fakeredis)
# ---------------------------------------------------------------------------


class _FakeModuleRunner:
    """Records ModuleRunner.run invocations; never blocks."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run(
        self,
        query: Any,
        *,
        task_id: str,
        setup_id: str,
        mission_id: str,
        on_fatal: Any,  # noqa: ARG002
    ) -> None:
        self.calls.append({
            "query": query, "task_id": task_id,
            "setup_id": setup_id, "mission_id": mission_id,
        })


@pytest.fixture
async def gateway() -> AsyncIterator[Any]:
    from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
    from digitalkin.models.grpc_servers.models import ClientConfig
    from digitalkin.models.settings.utils.channel import SecurityMode

    redis = _FakeRedisClient()
    cfg = ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    runner = _FakeModuleRunner()
    servicer = GatewayServicer(
        redis_client=redis,  # type: ignore[arg-type]
        max_streams=100,
        client_config=cfg,
        module_runner=runner,  # type: ignore[arg-type]
    )
    servicer._fake_runner = runner  # type: ignore[attr-defined]  # for tests to introspect
    try:
        yield servicer
    finally:
        await redis.close()


def _mock_context(metadata: dict[str, str] | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.invocation_metadata.return_value = list(metadata.items()) if metadata else []
    return ctx


def _start_request(task_id: str = "task_dial") -> Any:
    request = MagicMock()
    request.task_id = task_id
    request.setup_id = "setups:test"
    request.mission_id = "missions:test"
    return request


def _protocol_of(stream_msg: Any) -> str:
    root = stream_msg.data.fields.get("root")
    if root is None:
        return ""
    pf = root.struct_value.fields.get("protocol")
    return pf.string_value if pf is not None else ""


# ===========================================================================
# Tests
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestDialConsumer:
    async def test_no_metadata_no_dial(self, gateway, fake_consumer_server) -> None:
        """Without `x-client-address` metadata, gateway does not dial back."""
        servicer, address = fake_consumer_server  # noqa: ARG002 (address unused intentionally)

        # Issue StartStream WITHOUT metadata
        await gateway.StartStream(_start_request("task_no_meta"), _mock_context())
        # Give the event loop a tick — if a dial-back were scheduled it would
        # have started.
        await asyncio.sleep(0.1)
        assert servicer.received == []

    async def test_happy_path_handshake_and_output(self, gateway) -> None:
        """stream.init → query → 2 outputs from Redis → stream.end."""
        servicer = _FakeConsumerServicer(query_data={"protocol": "test", "x": 1})
        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            task_id = "task_happy"

            # Pre-populate Redis with two domain outputs + EOS so when
            # _consume_from_redis runs it has something to drain.
            from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

            writer = ProtoStreamWriter(task_id, gateway._redis_client)  # type: ignore[arg-type]
            for i in range(2):
                s = struct_pb2.Struct()
                s.update({"protocol": "healthcheck_ping", "status": "pong", "i": i})
                await writer.write_struct(s)
            await writer.write_eos()

            ctx = _mock_context({"x-client-address": f"127.0.0.1:{port}"})
            # Let StartStream register the session (avoid dedup early-return).
            await gateway.StartStream(_start_request(task_id), ctx)

            # Wait until consumer sees stream.end on the wire.
            for _ in range(80):
                if any(_protocol_of(m) == "stream.end" for m in servicer.received):
                    break
                await asyncio.sleep(0.1)

            protos = [_protocol_of(m) for m in servicer.received]
            assert protos[0] == "stream.init", f"got: {protos}"
            assert "stream.end" in protos, f"got: {protos}"

            # Reaper-at-stream-end: session must be unregistered when the
            # dial-back finishes — not 120 s later via heartbeat staleness.
            for _ in range(20):
                if gateway._registry.get(task_id) is None:
                    break
                await asyncio.sleep(0.05)
            assert gateway._registry.get(task_id) is None, (
                "session still registered after stream.end — reaper would log a false zombie"
            )
        finally:
            await server.stop(grace=0.1)

    async def test_first_reply_invokes_module_runner(self, gateway) -> None:
        """The consumer's first StreamServer reply (the query) is handed to ModuleRunner.run."""
        servicer = _FakeConsumerServicer(
            query_data={"protocol": "agui_stream", "user_prompt": "hello"},
        )
        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            task_id = "task_runner"
            ctx = _mock_context({"x-client-address": f"127.0.0.1:{port}"})
            await gateway.StartStream(_start_request(task_id), ctx)

            runner = gateway._fake_runner
            for _ in range(50):
                if runner.calls:
                    break
                await asyncio.sleep(0.05)

            assert len(runner.calls) >= 1
            call = runner.calls[0]
            assert call["task_id"] == task_id
            assert call["query"].fields["user_prompt"].string_value == "hello"
        finally:
            await server.stop(grace=0.1)

    async def test_multi_turn_upstream(self, gateway) -> None:
        """First reply → ModuleRunner; subsequent replies → Redis input stream."""
        servicer = _FakeConsumerServicer(
            query_data={"q": "first"},
            extra_upstream=[{"q": "second"}, {"q": "third"}],
        )
        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            task_id = "task_multi"
            ctx = _mock_context({"x-client-address": f"127.0.0.1:{port}"})
            await gateway.StartStream(_start_request(task_id), ctx)

            redis = gateway._redis_client
            input_key = f"task:{task_id}:input"
            for _ in range(80):
                xlen = await redis.xlen(input_key)
                if xlen >= 2:
                    break
                await asyncio.sleep(0.05)

            # First reply went to ModuleRunner (in-memory by-value).
            runner = gateway._fake_runner
            assert len(runner.calls) == 1
            assert runner.calls[0]["query"].fields["q"].string_value == "first"

            # Follow-up replies XADD'd to the Redis input stream as raw bytes.
            entries = await redis._client.xrange(input_key)  # noqa: SLF001
            payloads = []
            for _entry_id, fields in entries:
                pb = fields.get(b"pb")
                assert pb is not None
                s = struct_pb2.Struct()
                s.ParseFromString(pb)
                payloads.append(s.fields["q"].string_value)
            assert payloads == ["second", "third"]
        finally:
            await server.stop(grace=0.1)

    async def test_session_missing_releases_channel(self, gateway, fake_consumer_server) -> None:
        """If session lookup misses, _dial_consumer releases the channel cleanly."""
        servicer, address = fake_consumer_server
        # Drive _dial_consumer directly with a task_id we never registered.
        await gateway._dial_consumer(
            task_id="task_no_session",
            mission_id="missions:none",
            setup_id="setups:none",
            address=address,
        )
        # Should return immediately without dialing (servicer never called).
        assert servicer.received == []

    async def test_stub_stream_usage_error_does_not_escape(
        self, gateway, fake_consumer_server, monkeypatch
    ) -> None:
        """If `stub.Stream(...)` raises cygrpc.UsageError (channel closed before BiDi),
        the spawned task must NOT crash with 'Task exception was never retrieved'.
        Regression test for the production crash where the consumer is unreachable.
        """
        from grpc._cython.cygrpc import UsageError

        from digitalkin.services.communication.grpc_communication import GrpcCommunication

        _servicer, address = fake_consumer_server  # noqa: F841
        emitted: list[dict] = []

        async def _capture(task_id, *, code, message, log_extra=None):  # noqa: ARG001
            emitted.append({"task_id": task_id, "code": code, "message": message})

        monkeypatch.setattr(gateway, "_emit_fatal_to_redis", _capture)

        # Register a session so _dial_consumer proceeds past the registry lookup.
        from digitalkin.grpc_servers.stream_session import StreamSession

        gateway._registry._local_cache["task_usage"] = StreamSession(task_id="task_usage")

        # Patch dial_consumer_stream to return a stub whose Stream raises UsageError.
        class _BoomStub:
            def Stream(self, _outgoing, *, timeout):  # noqa: N802, ARG002
                raise UsageError("Channel is closed.")

        async def _release() -> None:
            return None

        def _fake_dial(self, _address):  # noqa: ANN001, ARG001
            self._channel = MagicMock(_closed=True)
            self._channel_cache_key = "fake:insecure:gzip"
            return _BoomStub(), _release

        monkeypatch.setattr(GrpcCommunication, "dial_consumer_stream", _fake_dial)

        # Must complete normally — no exception escaping the spawned task.
        await gateway._dial_consumer(
            task_id="task_usage",
            mission_id="missions:test",
            setup_id="setups:test",
            address=address,
        )

        # Should have emitted exactly one DIAL_BACK_RPC_ERROR (not DIAL_BACK_NO_QUERY).
        codes = [e["code"] for e in emitted]
        assert "DIAL_BACK_RPC_ERROR" in codes, f"got: {codes}"
        assert "DIAL_BACK_NO_QUERY" not in codes, f"got: {codes}"
