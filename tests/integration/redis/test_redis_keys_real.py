"""L1 integration: gateway Redis key lifecycle against real Redis.

Pairs the unit tests in ``tests/gateway/test_redis_keys_lifecycle.py``,
``tests/gateway/test_gateway_servicer.py`` (R1/R2) and
``tests/core/redis/test_redis_idempotency.py`` (RECLAIMED TTL):
no ``task:{id}:input`` writes, the seeded stream has a TTL, ``idem:{id}`` is
released on a failed dial and shortened after a fatal EOS.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_dto_pb2
from google.protobuf import struct_pb2

from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotency
from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.grpc_servers.stream_session import StreamSession
from digitalkin.models.core.redis import ClaimResult
from digitalkin.models.settings.gateway import get_gateway_settings
from tests.gateway.test_gateway_servicer import _FakeRequestIterator, _make_stream_request

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


def _ctx() -> Any:
    ctx = MagicMock()
    ctx.invocation_metadata.return_value = [("x-client-address", "127.0.0.1:50057")]
    return ctx


def _request(task_id: str) -> Any:
    return gateway_dto_pb2.StartStreamRequest(task_id=task_id, setup_id="setups:s", mission_id="missions:m")


async def test_seed_has_initial_ttl_and_replaces_stale_stream(redis_client: RedisClient) -> None:
    get_gateway_settings.cache_clear()
    gateway = GatewayServicer(redis_client=redis_client)
    gateway._dial_consumer = AsyncMock()  # type: ignore[method-assign]
    await redis_client.xadd("task:t-seed:stream", {"eos": b"true"})
    await redis_client.set("task:t-seed:cursor", "1-0")

    resp = await gateway.StartStream(_request("t-seed"), _ctx())

    assert resp.accepted is True
    ttl = await redis_client._client.ttl("task:t-seed:stream")
    assert 0 < ttl <= get_gateway_settings().stream.redis_stream_initial_ttl
    assert await redis_client.xlen("task:t-seed:stream") == 1
    assert await redis_client.exists("task:t-seed:cursor") == 0


async def test_failed_dial_releases_idem(redis_client: RedisClient) -> None:
    get_gateway_settings.cache_clear()
    gateway = GatewayServicer(redis_client=redis_client)

    resp = await gateway.StartStream(_request("t-nodial"), _ctx())
    assert resp.accepted is True
    for _ in range(50):
        if await redis_client.exists("idem:t-nodial") == 0:
            break
        await asyncio.sleep(0.05)

    assert await redis_client.exists("idem:t-nodial") == 0
    entries = await redis_client._client.xrange("task:t-nodial:stream")
    assert entries[-1][1] == {b"eos": b"true"}
    err = struct_pb2.Struct()
    err.ParseFromString(entries[-2][1][b"pb"])
    assert err.fields["root"].struct_value.fields["protocol"].string_value == "stream.error"
    assert (await RedisIdempotency(redis_client).claim("t-nodial", "other")) is ClaimResult.CLAIMED


async def test_fatal_eos_shortens_idem_ttl(redis_client: RedisClient) -> None:
    get_gateway_settings.cache_clear()
    gateway = GatewayServicer(redis_client=redis_client)
    await RedisIdempotency(redis_client).claim("t-fatal", "inst")

    await gateway._emit_fatal_to_redis("t-fatal", "DIAL_BACK_UNREACHABLE", "unreachable", log_extra={})

    assert 0 < await redis_client._client.ttl("idem:t-fatal") <= get_gateway_settings().stream.redis_stream_ttl


async def test_reclaim_does_not_refresh_idem_ttl(redis_client: RedisClient) -> None:
    idem = RedisIdempotency(redis_client)
    await idem.claim("t-reclaim", "inst")
    await redis_client.expire("idem:t-reclaim", 5)

    assert await idem.claim("t-reclaim", "inst") is ClaimResult.RECLAIMED
    assert await redis_client._client.ttl("idem:t-reclaim") <= 5


async def test_upstream_follow_ups_never_write_input_stream(redis_client: RedisClient) -> None:
    gateway = GatewayServicer(redis_client=redis_client)
    session = StreamSession(task_id="t-input")
    request_iter = _FakeRequestIterator([_make_stream_request(task_id="t-input", data_dict={"q": "follow-up"})])

    await gateway._read_peer_upstream(request_iter, "t-input", session)

    assert await redis_client.exists("task:t-input:input") == 0
