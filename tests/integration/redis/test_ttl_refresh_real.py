"""L1 integration: sliding stream/idem TTLs against REAL Redis.

Pairs ``tests/core/test_module_runner_m4.py::test_every_output_refreshes_stream_and_idem_ttl_in_one_round_trip``.
Every output XADD re-arms ``task:{id}:stream`` (redis_stream_initial_ttl) and ``idem:{id}`` (idem_ttl)
in the same pipeline; EOS shortens both to redis_stream_ttl and later outputs no longer extend them.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_dto_pb2, gateway_messages_pb2
from google.protobuf import struct_pb2

from digitalkin.core.task_manager.module_runner import ModuleRunner
from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.models.settings.redis import get_redis_settings

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


class _Out:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def model_dump(self, mode: str = "json") -> dict[str, Any]:
        return self._payload


async def _run(redis_client: RedisClient, task_id: str, body: Callable[[Any], Awaitable[None]]) -> None:
    setup_version = MagicMock(content={}, setup_id="setups:s1", id="setup_versions:v1")
    servicer = MagicMock()
    servicer.resolve_setup = AsyncMock(return_value=setup_version)
    servicer.module_class.create_setup_model = AsyncMock(return_value=MagicMock())
    servicer.get_tool_cache = MagicMock(return_value=MagicMock())
    servicer.module_class.create_input_model = MagicMock(return_value=MagicMock())

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:
        return MagicMock(), kwargs["job_id"], kwargs["callback"]

    async def _run_instance(*, callback: Any, **_: Any) -> None:
        await body(callback)

    async def _on_fatal(code: str, message: str) -> None:  # noqa: RUF029
        pytest.fail(f"unexpected on_fatal {code}: {message}")

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = _run_instance
    with patch("digitalkin.core.task_manager.module_runner.TaskProfiler"):
        await ModuleRunner(redis_client=redis_client, servicer=servicer).run(  # type: ignore[arg-type]
            struct_pb2.Struct(), task_id=task_id, setup_id="setups:s1", mission_id="missions:m1", on_fatal=_on_fatal
        )


class TestSlidingTtlReal:
    async def test_output_rearms_near_expired_stream_and_idem(self, redis_client: RedisClient) -> None:
        get_gateway_settings.cache_clear()
        get_redis_settings.cache_clear()
        stream = get_gateway_settings().stream
        idem_ttl = get_redis_settings().idem_ttl
        await redis_client.set("idem:tr1", "owner", ex=idem_ttl)
        seen: dict[str, int] = {}

        async def _body(emit: Any) -> None:
            await emit(_Out({"root": {"protocol": "data", "value": 1}}))
            await redis_client.expire("task:tr1:stream", 2)
            await redis_client.expire("idem:tr1", 2)
            await emit(_Out({"root": {"protocol": "data", "value": 2}}))
            seen["stream"] = await redis_client._client.ttl("task:tr1:stream")
            seen["idem"] = await redis_client._client.ttl("idem:tr1")

        await _run(redis_client, "tr1", _body)

        assert stream.redis_stream_initial_ttl - 5 < seen["stream"] <= stream.redis_stream_initial_ttl
        assert idem_ttl - 5 < seen["idem"] <= idem_ttl
        assert await redis_client._client.xlen("task:tr1:stream") == 2

    async def test_eos_shortens_and_later_output_does_not_extend(self, redis_client: RedisClient) -> None:
        get_gateway_settings.cache_clear()
        get_redis_settings.cache_clear()
        ttl = get_gateway_settings().stream.redis_stream_ttl
        await redis_client.set("idem:tr2", "owner", ex=get_redis_settings().idem_ttl)

        async def _body(emit: Any) -> None:
            await emit(_Out({"root": {"protocol": "data", "value": 1}}))
            await emit(_Out({"root": {"protocol": "stream.end"}}))
            await emit(_Out({"root": {"protocol": "data", "value": 2}}))

        await _run(redis_client, "tr2", _body)

        assert 0 < await redis_client._client.ttl("task:tr2:stream") <= ttl
        assert 0 < await redis_client._client.ttl("idem:tr2") <= ttl

    async def test_released_idem_is_not_resurrected(self, redis_client: RedisClient) -> None:
        async def _body(emit: Any) -> None:
            await emit(_Out({"root": {"protocol": "data", "value": 1}}))

        await _run(redis_client, "tr3", _body)

        assert await redis_client.exists("idem:tr3") == 0

    async def test_cancel_after_idem_would_have_expired_still_succeeds(self, redis_client: RedisClient) -> None:
        """SendSignal answers success=True for a task whose original idem TTL has lapsed but kept emitting."""
        await redis_client.set("idem:tr4", "owner", ex=1)

        async def _body(emit: Any) -> None:
            await emit(_Out({"root": {"protocol": "data", "value": 1}}))
            await asyncio.sleep(1.2)
            request = gateway_dto_pb2.SendSignalRequest(cancel=gateway_messages_pb2.CancelSignal(task_id="tr4"))
            resp = await GatewayServicer(redis_client=redis_client).SendSignal(request, MagicMock())
            assert resp.success is True

        await _run(redis_client, "tr4", _body)
