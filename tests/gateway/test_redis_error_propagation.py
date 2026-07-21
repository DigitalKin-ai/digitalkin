"""R2 regression: Redis failures surface in-band, never as an opaque gRPC abort.

Redis transport errors on the gateway data path must become
``stream.error(REDIS_UNAVAILABLE)`` + ``stream.end`` (stream reads) or
``StartStreamResponse(accepted=False)`` (StartStream) — the documented
sentinel contract — instead of bubbling out of the RPC as UNKNOWN.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.models.grpc_servers.stream_error_codes import StreamErrorCode

pytestmark = [pytest.mark.timeout(15), pytest.mark.regression]


def _protocol_of(msg: Any) -> str:
    return msg.data.fields["root"].struct_value.fields["protocol"].string_value


def _ctx(client_address: str = "127.0.0.1:50057") -> MagicMock:
    ctx = MagicMock()
    ctx.invocation_metadata.return_value = [("x-client-address", client_address)]
    return ctx


class _RedisDownOnRead:
    """Fake RedisClient whose stream read raises a transport error."""

    async def get(self, name: str) -> bytes | None:
        return None

    async def set(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def xread(self, streams: Any, *, count: int = 50, block: int = 1000) -> list:
        msg = "redis down"
        raise RedisConnectionError(msg)


async def test_consume_guarded_redis_error_emits_sentinel() -> None:
    servicer = GatewayServicer(redis_client=_RedisDownOnRead())  # type: ignore[arg-type]
    outs = [m async for m in servicer._consume_guarded("task_r2", 0)]
    assert [_protocol_of(m) for m in outs] == ["stream.error", "stream.end"]
    err = outs[0].data.fields["root"].struct_value.fields
    assert err["code"].string_value == StreamErrorCode.REDIS_UNAVAILABLE.value
    assert err["fatal"].bool_value is True


async def test_startstream_claim_redis_error_returns_not_accepted() -> None:
    from agentic_mesh_protocol.gateway.v1 import gateway_pb2

    redis_client = MagicMock()
    redis_client.eval = AsyncMock(side_effect=RedisConnectionError("down"))
    servicer = GatewayServicer(redis_client=redis_client)

    req = gateway_pb2.StartStreamRequest(task_id="task_r2", setup_id="setups:s", mission_id="missions:m")
    resp = await servicer.StartStream(req, _ctx())
    assert resp.accepted is False
    assert resp.task_id == "task_r2"


async def test_startstream_seed_xadd_redis_error_releases_claim_and_rejects() -> None:
    from agentic_mesh_protocol.gateway.v1 import gateway_pb2

    redis_client = MagicMock()
    redis_client.eval = AsyncMock(return_value=1)  # ClaimResult.CLAIMED → fresh-dial path
    redis_client.xadd = AsyncMock(side_effect=RedisConnectionError("down"))
    redis_client.delete = AsyncMock(return_value=1)  # idempotency.release
    servicer = GatewayServicer(redis_client=redis_client)

    req = gateway_pb2.StartStreamRequest(task_id="task_r2b", setup_id="setups:s", mission_id="missions:m")
    resp = await servicer.StartStream(req, _ctx())
    assert resp.accepted is False
    # The claim is released so a retry can re-run.
    redis_client.delete.assert_awaited()
