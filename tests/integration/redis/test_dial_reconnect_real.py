"""Integration (real Redis): dial-back resume/dedup mechanics + extended TTL.

Pairs the fakeredis unit tests in ``tests/gateway/test_resume_dial.py``. Verifies
against a real Redis Stream that:
- a resume drain from a mid ``from_seq`` skips already-seen entries (dedup) and
  labels the tail off the stored producer seq;
- the post-EOS stream TTL is extended past the old 60s so a completed stream
  survives a client reboot within the reconnect window.
"""

from __future__ import annotations

from typing import Any

import pytest
from google.protobuf import struct_pb2

from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.models.settings.gateway import get_gateway_settings

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]


def _protocol_of(msg: Any) -> str:
    root = msg.data.fields.get("root")
    if root is None:
        return ""
    pf = root.struct_value.fields.get("protocol")
    return pf.string_value if pf is not None else ""


async def _seed_stream(redis: Any, task_id: str, n_chunks: int = 6) -> None:
    key = f"task:{task_id}:stream"
    start = struct_pb2.Struct()
    start.update({"root": {"protocol": "stream.start"}})
    await redis.xadd(key, {"pb": start.SerializeToString(), "seq": "0"})
    for i in range(1, n_chunks + 1):
        s = struct_pb2.Struct()
        s.update({"root": {"protocol": "chunk", "i": i}})
        await redis.xadd(key, {"pb": s.SerializeToString(), "seq": str(i)})
    await redis.xadd(key, {"eos": b"true"})


async def test_resume_drain_dedups_from_cursor_real(redis_client) -> None:
    task_id = "int_resume"
    await _seed_stream(redis_client, task_id)
    gw = GatewayServicer(redis_client=redis_client)

    out = [m async for m in gw._consume_from_redis(task_id, from_seq=4, resume=True)]

    # cursor 4 → skip stored seq <=3 → stored 4,5,6 relabelled off the stored seq
    # as wire 5,6,7; terminal stream.end at 8. No duplicates of what the consumer saw.
    assert [m.from_seq for m in out] == [5, 6, 7, 8]
    assert _protocol_of(out[-1]) == "stream.end"


async def test_post_eos_ttl_covers_reconnect_window_real(redis_client) -> None:
    settings = get_gateway_settings()
    ttl = settings.stream.redis_stream_ttl
    # Post-EOS retention must cover the reconnect window so a completed stream
    # survives a client reboot within it.
    assert ttl >= settings.dial_reconnect.window_s

    key = "task:int_ttl:stream"
    await redis_client.xadd(key, {"eos": b"true"})
    await redis_client.expire(key, ttl)
    remaining = await redis_client._client.ttl(key)
    assert remaining >= settings.dial_reconnect.window_s
