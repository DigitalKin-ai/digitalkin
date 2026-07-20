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
from pydantic import BaseModel, ValidationError
from redis.exceptions import RedisError

from digitalkin.models.grpc_servers.stream_error_codes import StreamErrorCode
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
        client_config=cfg,
    )
    try:
        yield servicer, redis
    finally:
        await redis.close()


# ===========================================================================
# Site 1: DISPATCH_UNAVAILABLE — retired in Phase 2.B (no dispatch:module
# Redis stream; the dial-back is the sole orchestrator). Code preserved
# in StreamErrorCode for forward-compat.
# ===========================================================================


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
# Site 6: INPUT_WAIT_TIMEOUT — retired in Phase 2.B (no dispatcher to time
# out on; the dial-back's DIAL_BACK_NO_QUERY covers consumer-never-replies).
# Code preserved in StreamErrorCode for forward-compat.
# ===========================================================================


# ===========================================================================
# Site 7: module job exception → MODULE_RUNTIME_ERROR (now via ModuleRunner)
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestModuleRuntimeError:
    async def test_module_exception_emits_runtime_error(self) -> None:
        from digitalkin.core.task_manager.module_runner import ModuleRunner

        redis = _FakeRedisClient()
        try:
            servicer = MagicMock()
            # `resolve_setup` is awaited via asyncio.create_task; use AsyncMock so
            # the task scheduler gets a real coroutine. `preload_instance` is also
            # awaited concurrently — same treatment. `create_input_model` is the
            # synchronous raise that drives this test.
            servicer.resolve_setup = AsyncMock(return_value=MagicMock())
            servicer.module_class.create_setup_model = AsyncMock(return_value=MagicMock())
            servicer.module_class.create_input_model = MagicMock(side_effect=ValueError("bad input"))
            # Non-None tool cache skips the get_or_build_tool_cache path so the
            # ValueError from create_input_model is the exception under test.
            servicer.get_tool_cache = MagicMock(return_value=MagicMock())
            servicer.job_manager.preload_instance = AsyncMock(
                return_value=(MagicMock(), "task_runtime", AsyncMock()),
            )
            servicer.job_manager.run_instance = AsyncMock()

            runner = ModuleRunner(redis_client=redis, servicer=servicer)  # type: ignore[arg-type]

            received: list[tuple[str, str]] = []

            async def _on_fatal(code: str, message: str) -> None:
                received.append((code, message))

            await runner.run(
                struct_pb2.Struct(),
                task_id="task_runtime",
                setup_id="setups:s1",
                mission_id="missions:m1",
                on_fatal=_on_fatal,
            )

            assert len(received) == 1
            code, message = received[0]
            assert code == StreamErrorCode.MODULE_RUNTIME_ERROR.value
            assert "ValueError" in message
            assert "bad input" in message
        finally:
            await redis.close()


# ===========================================================================
# Site 8: ValidationError phases in ModuleRunner — setup model vs input model.
# Regression for the staging incident where a setup-phase ValidationError
# crashed the input-phase handler with UnboundLocalError on top_level_keys.
# ===========================================================================


def _real_validation_error() -> ValidationError:
    """Produce a genuine pydantic ValidationError (not directly constructible in v2)."""

    class _Strict(BaseModel):
        required_field: int

    try:
        _Strict.model_validate({})
    except ValidationError as exc:
        return exc
    raise AssertionError("model_validate unexpectedly succeeded")


@SKIP_NO_FAKEREDIS
class TestValidationErrorPhases:
    @staticmethod
    def _servicer() -> MagicMock:
        servicer = MagicMock()
        servicer.module_class.__name__ = "FakeModule"
        servicer.resolve_setup = AsyncMock(return_value=MagicMock())
        servicer.module_class.create_setup_model = AsyncMock(return_value=MagicMock())
        servicer.module_class.create_input_model = MagicMock(return_value=MagicMock())
        servicer.get_tool_cache = MagicMock(return_value=MagicMock())
        servicer.job_manager.preload_instance = AsyncMock(return_value=(MagicMock(), "task_val", AsyncMock()))
        servicer.job_manager.run_instance = AsyncMock()
        return servicer

    async def test_setup_validation_error_emits_setup_code(self) -> None:
        from digitalkin.core.task_manager.module_runner import ModuleRunner

        redis = _FakeRedisClient()
        try:
            servicer = self._servicer()
            servicer.module_class.create_setup_model = AsyncMock(side_effect=_real_validation_error())
            runner = ModuleRunner(redis_client=redis, servicer=servicer)  # type: ignore[arg-type]

            received: list[tuple[str, str]] = []

            async def _on_fatal(code: str, message: str) -> None:
                received.append((code, message))

            await runner.run(
                struct_pb2.Struct(),
                task_id="task_val",
                setup_id="setups:staging_rag",
                mission_id="missions:m1",
                on_fatal=_on_fatal,
            )

            assert len(received) == 1
            code, message = received[0]
            assert code == StreamErrorCode.SETUP_VALIDATION_ERROR.value
            assert "setup validation failed" in message
            assert "setups:staging_rag" in message
            assert "required_field" in message
            servicer.job_manager.preload_instance.assert_not_awaited()
        finally:
            await redis.close()

    async def test_input_validation_error_emits_input_code(self) -> None:
        from digitalkin.core.task_manager.module_runner import ModuleRunner

        redis = _FakeRedisClient()
        try:
            servicer = self._servicer()
            servicer.module_class.create_input_model = MagicMock(side_effect=_real_validation_error())
            servicer.module_class._extended_input_format = None
            servicer.module_class.input_format = type("FakeInput", (), {})
            runner = ModuleRunner(redis_client=redis, servicer=servicer)  # type: ignore[arg-type]

            received: list[tuple[str, str]] = []

            async def _on_fatal(code: str, message: str) -> None:
                received.append((code, message))

            await runner.run(
                struct_pb2.Struct(),
                task_id="task_val",
                setup_id="setups:s1",
                mission_id="missions:m1",
                on_fatal=_on_fatal,
            )

            assert len(received) == 1
            code, message = received[0]
            assert code == StreamErrorCode.INPUT_VALIDATION_ERROR.value
            assert "input validation failed" in message
            assert "FakeInput" in message
        finally:
            await redis.close()


# ===========================================================================
# GrpcCommunication.stream_error helper
# ===========================================================================


class TestStreamErrorHelper:
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
        from digitalkin.services.communication import GrpcCommunication

        data = self._build_error("DIAL_BACK_RPC_ERROR", "boom")
        result = GrpcCommunication.stream_error(data)
        assert result == ("DIAL_BACK_RPC_ERROR", "boom")

    def test_returns_none_for_non_error(self) -> None:
        from digitalkin.services.communication import GrpcCommunication

        assert GrpcCommunication.stream_error(self._build_other("stream.start")) is None
        assert GrpcCommunication.stream_error(self._build_other("agui_stream")) is None
        assert GrpcCommunication.stream_error(struct_pb2.Struct()) is None
