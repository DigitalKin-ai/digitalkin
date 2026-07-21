"""Regression: a consumer reading an EOS-less Redis stream must not hang.

``TaskExecutor`` closes only the in-memory stream on a module crash/cancel —
it never writes an ``eos`` marker to Redis. ``ProtoStreamReader`` blocks
forever on such a stream, so the gateway wraps the read with an idle deadline
(``_consume_guarded``) that emits ``stream.error(STREAM_IDLE_TIMEOUT)`` +
``stream.end`` instead of hanging the consumer's RPC.
"""

from __future__ import annotations

import asyncio

import pytest

from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.models.grpc_servers.stream_error_codes import StreamErrorCode
from digitalkin.models.settings.gateway import get_gateway_settings

pytestmark = [pytest.mark.timeout(15), pytest.mark.regression]


class _NeverEosRedis:
    """Fake RedisClient whose stream blocks then yields no entry, ever."""

    async def get(self, name: str) -> bytes | None:  # noqa: ARG002
        return None

    async def xread(self, streams, *, count: int = 50, block: int = 1000) -> list:  # noqa: ARG002
        await asyncio.sleep(block / 1000.0)  # mimic XREAD BLOCK with no data
        return []

    async def xlen(self, name: str) -> int:  # noqa: ARG002
        return 1


async def test_consume_guarded_terminates_without_eos(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIGITALKIN_GATEWAY_STREAM_READ_IDLE_TIMEOUT_S", "0.3")
    monkeypatch.setenv("DIGITALKIN_GATEWAY_STREAM_STREAM_READ_BLOCK_MS", "20")
    get_gateway_settings.cache_clear()

    servicer = GatewayServicer(redis_client=_NeverEosRedis())  # type: ignore[arg-type]

    outs = [msg async for msg in servicer._consume_guarded("task_x", 0)]  # noqa: SLF001

    protocols = [
        m.data.fields["root"].struct_value.fields["protocol"].string_value for m in outs
    ]
    assert protocols == ["stream.error", "stream.end"]

    err = outs[0].data.fields["root"].struct_value.fields
    assert err["code"].string_value == StreamErrorCode.STREAM_IDLE_TIMEOUT.value
    assert err["fatal"].bool_value is True

    get_gateway_settings.cache_clear()
