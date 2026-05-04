"""Phase 1.A — every silent failure path emits ``stream.error`` to Redis.

For each failure mode in the dial-back chain, verify that the gateway
or dispatcher writes:

1. a ``stream.error(fatal=true)`` Struct on ``task:{task_id}:stream``
   with a stable code from :class:`StreamErrorCode`,
2. an EOS marker on the same stream.

A late client (or the dial-back BiDi itself) reading from Redis then
sees the error sentinel before the stream terminates.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import grpc
import grpc.aio
import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from google.protobuf import struct_pb2
from redis.exceptions import RedisError

from digitalkin.grpc_servers.stream_error_codes import StreamErrorCode
from tests.gateway.test_dial_consumer import (
    SKIP_NO_FAKEREDIS,
    _FakeConsumerServicer,
    _FakeRedisClient,
    _start_request,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = [pytest.mark.timeout(30)]


def _client_md(address: str) -> MagicMock:
    ctx = MagicMock()
    ctx.invocation_metadata.return_value = [("x-client-address", address)]
    return ctx


async def _read_all_stream_entries(redis: _FakeRedisClient, task_id: str) -> list[dict]:
    """Return all entries on ``task:{task_id}:stream`` decoded as dicts.

    Each dict has ``protocol`` (decoded from the embedded Struct's
    ``root.protocol``) and the raw ``fields`` mapping for further checks.
    """
    raw_client = redis._client
    entries = await raw_client.xrange(f"task:{task_id}:stream")
    decoded = []
    for _entry_id, fields in entries:
        if b"eos" in fields:
            decoded.append({"protocol": "_eos", "fields": fields})
            continue
        pb_bytes = fields.get(b"pb")
        if pb_bytes is None:
            continue
        s = struct_pb2.Struct()
        s.ParseFromString(pb_bytes)
        root = s.fields.get("root")
        proto = ""
        code = ""
        message = ""
        if root is not None:
            inner = root.struct_value.fields
            if "protocol" in inner:
                proto = inner["protocol"].string_value
            if "code" in inner:
                code = inner["code"].string_value
            if "message" in inner:
                message = inner["message"].string_value
        decoded.append({
            "protocol": proto, "code": code, "message": message, "fields": fields,
        })
    return decoded


async def _wait_for_error(redis: _FakeRedisClient, task_id: str, *, timeout: float = 5.0) -> dict:
    """Poll ``task:{task_id}:stream`` until a stream.error entry appears."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        entries = await _read_all_stream_entries(redis, task_id)
        for e in entries:
            if e.get("protocol") == "stream.error":
                return e
        await asyncio.sleep(0.05)
    msg = f"no stream.error appeared on task:{task_id}:stream within {timeout}s"
    raise AssertionError(msg)


# ---------------------------------------------------------------------------
# Gateway servicer fixture
# ---------------------------------------------------------------------------


@pytest.fixture
async def gateway_with_redis() -> AsyncIterator[tuple[Any, _FakeRedisClient]]:
    from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
    from digitalkin.models.grpc_servers.models import ClientConfig
    from digitalkin.models.settings.utils.channel import SecurityMode

    redis = _FakeRedisClient()
    cfg = ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    servicer = GatewayServicer(
        redis_client=redis,  # type: ignore[arg-type]
        max_streams=100,
        client_config=cfg,
    )
    try:
        yield servicer, redis
    finally:
        await redis.close()


# ===========================================================================
# Site 1: dispatch XADD failure → DISPATCH_UNAVAILABLE
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestDispatchUnavailable:
    async def test_dispatch_xadd_failure_emits_dispatch_unavailable(self) -> None:
        """Dispatch XADD failure → ``stream.error(code=DISPATCH_UNAVAILABLE)``."""
        from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
        from digitalkin.models.grpc_servers.models import ClientConfig
        from digitalkin.models.settings.utils.channel import SecurityMode

        real_redis = _FakeRedisClient()
        try:
            cfg = ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
            servicer = GatewayServicer(
                redis_client=real_redis,  # type: ignore[arg-type]
                max_streams=100,
                client_config=cfg,
            )
            session = MagicMock()
            session.task_id = "task_dispatch_fail"
            request = _start_request("task_dispatch_fail")

            # Patch xadd to fail only on the dispatch key, succeed on the
            # task stream so the error sentinel + EOS land normally.
            real_xadd = real_redis.xadd
            calls: list[str] = []

            async def patched_xadd(name: str, fields: dict, **kw: Any) -> Any:
                calls.append(name)
                if name == "dispatch:module":
                    msg = "simulated dispatch outage"
                    raise RedisError(msg)
                return await real_xadd(name, fields, **kw)

            real_redis.xadd = patched_xadd  # type: ignore[assignment, method-assign]

            await servicer._start_module(session, request)
            real_redis.xadd = real_xadd  # type: ignore[method-assign]

            error = await _wait_for_error(real_redis, "task_dispatch_fail", timeout=2.0)
            assert error["code"] == StreamErrorCode.DISPATCH_UNAVAILABLE.value
            assert "RedisError" in error["message"]
        finally:
            await real_redis.close()


# ===========================================================================
# Site 5: consumer never replies → DIAL_BACK_NO_QUERY (BiDi closes empty)
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestDialBackNoQuery:
    async def test_no_query_emits_no_query_sentinel(self) -> None:
        """Consumer never replies → ``stream.error(code=DIAL_BACK_NO_QUERY)``."""
        from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
        from digitalkin.models.grpc_servers.models import ClientConfig
        from digitalkin.models.settings.utils.channel import SecurityMode

        # Hanging consumer: accepts Stream() but never yields and reads
        # forever. We close the BiDi from the gateway side via short timeout.
        servicer_consumer = _FakeConsumerServicer(query_data=None)
        # Force "hang" mode so the consumer never replies. (extra_upstream
        # is empty, query_data is None — it will yield nothing and just
        # drain incoming until close.)
        servicer_consumer.hang = True

        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer_consumer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            redis = _FakeRedisClient()
            try:
                cfg = ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
                gateway = GatewayServicer(
                    redis_client=redis,  # type: ignore[arg-type]
                    max_streams=100,
                    client_config=cfg,
                )
                # Pre-register the session so _dial_consumer doesn't bail.
                from digitalkin.grpc_servers.stream_session import StreamSession
                session = StreamSession(task_id="task_no_query")
                await gateway._registry.register(
                    session, setup_id="setups:s1", mission_id="missions:m1",
                )

                # Run the dial-back with a tight BiDi timeout (we override
                # the hardcoded 300s by closing the consumer-side server
                # explicitly after a short delay).
                dial_task = asyncio.create_task(
                    gateway._dial_consumer(
                        task_id="task_no_query",
                        mission_id="missions:m1",
                        setup_id="setups:s1",
                        address=f"127.0.0.1:{port}",
                    ),
                )
                # Let the dial-back open the BiDi, then kill the consumer
                # so the gateway-side BiDi closes (without a reply ever).
                await asyncio.sleep(0.3)
                await server.stop(grace=0.1)
                await asyncio.wait_for(dial_task, timeout=10)

                error = await _wait_for_error(redis, "task_no_query", timeout=2.0)
                # Either NO_QUERY (consumer-stop-after-accept) or
                # RPC_ERROR (the kill races with the BiDi state) — both
                # are valid signals that the consumer never produced.
                assert error["code"] in {
                    StreamErrorCode.DIAL_BACK_NO_QUERY.value,
                    StreamErrorCode.DIAL_BACK_RPC_ERROR.value,
                }
            finally:
                await redis.close()
        finally:
            # Idempotent: we already stopped above.
            with __import__("contextlib").suppress(Exception):
                await server.stop(grace=0.1)


# ===========================================================================
# Site 6: dispatcher input wait timeout → INPUT_WAIT_TIMEOUT
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestInputWaitTimeout:
    async def test_dispatcher_timeout_emits_input_wait_timeout(self) -> None:
        from digitalkin.core.task_manager.task_dispatcher import TaskDispatcher
        from digitalkin.grpc_servers.stream_session import StreamSession

        redis = _FakeRedisClient()
        try:
            registry = MagicMock()
            session = StreamSession(task_id="task_input_to")
            registry.get = MagicMock(return_value=session)

            dispatcher = TaskDispatcher(
                redis_client=redis,  # type: ignore[arg-type]
                servicer=MagicMock(),
                dispatch_key="dispatch:module",
                registry=registry,
                input_wait_timeout_s=0.05,
            )

            await dispatcher._handle_dispatch({
                b"task_id": b"task_input_to",
                b"setup_id": b"setups:s1",
                b"mission_id": b"missions:m1",
                b"ts_ns": b"0",
            })

            entries = await _read_all_stream_entries(redis, "task_input_to")
            error_entries = [e for e in entries if e.get("protocol") == "stream.error"]
            assert len(error_entries) == 1
            assert error_entries[0]["code"] == StreamErrorCode.INPUT_WAIT_TIMEOUT.value
            assert "no upstream input" in error_entries[0]["message"]

            # EOS marker present after the error.
            eos = [e for e in entries if e.get("protocol") == "_eos"]
            assert len(eos) == 1
        finally:
            await redis.close()


# ===========================================================================
# Site 7: module job exception → MODULE_RUNTIME_ERROR
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestModuleRuntimeError:
    async def test_module_exception_emits_runtime_error(self) -> None:
        from digitalkin.core.task_manager.task_dispatcher import TaskDispatcher
        from digitalkin.grpc_servers.stream_session import StreamSession

        redis = _FakeRedisClient()
        try:
            registry = MagicMock()
            session = StreamSession(task_id="task_runtime")
            await session.enqueue_input({"_proto": struct_pb2.Struct()})
            registry.get = MagicMock(return_value=session)

            servicer = MagicMock()
            servicer.module_class.create_input_model = MagicMock(side_effect=ValueError("bad input"))

            dispatcher = TaskDispatcher(
                redis_client=redis,  # type: ignore[arg-type]
                servicer=servicer,
                dispatch_key="dispatch:module",
                registry=registry,
                input_wait_timeout_s=1.0,
            )

            await dispatcher._handle_dispatch({
                b"task_id": b"task_runtime",
                b"setup_id": b"setups:s1",
                b"mission_id": b"missions:m1",
                b"ts_ns": b"0",
            })

            entries = await _read_all_stream_entries(redis, "task_runtime")
            error_entries = [e for e in entries if e.get("protocol") == "stream.error"]
            assert len(error_entries) == 1
            assert error_entries[0]["code"] == StreamErrorCode.MODULE_RUNTIME_ERROR.value
            assert "ValueError" in error_entries[0]["message"]
            assert "bad input" in error_entries[0]["message"]
        finally:
            await redis.close()


# ===========================================================================
# GatewayConsumer.stream_error helper
# ===========================================================================


class TestGatewayConsumerStreamError:
    @staticmethod
    def _build_error(code: str, message: str) -> struct_pb2.Struct:
        s = struct_pb2.Struct()
        s.update({"root": {"protocol": "stream.error", "code": code, "message": message, "fatal": True}})
        return s

    @staticmethod
    def _build_other(protocol: str) -> struct_pb2.Struct:
        s = struct_pb2.Struct()
        s.update({"root": {"protocol": protocol}})
        return s

    def test_decodes_stream_error(self) -> None:
        from digitalkin.services.communication import GatewayConsumer

        data = self._build_error("DIAL_BACK_RPC_ERROR", "boom")
        result = GatewayConsumer.stream_error(data)
        assert result == ("DIAL_BACK_RPC_ERROR", "boom")

    def test_returns_none_for_non_error(self) -> None:
        from digitalkin.services.communication import GatewayConsumer

        assert GatewayConsumer.stream_error(self._build_other("stream.start")) is None
        assert GatewayConsumer.stream_error(self._build_other("agui_stream")) is None
        assert GatewayConsumer.stream_error(struct_pb2.Struct()) is None
