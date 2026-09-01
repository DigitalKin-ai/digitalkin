"""Tests for ``ProtoStreamReader.read_structs(skip_to_seq=...)`` exact-cursor seek.

The resume-dial path seeks to a consumer's exact cursor by suppressing entries
with stored ``seq <= skip_to_seq`` while still advancing the cursor and gap
detection. Drives a real ``ProtoStreamReader`` over fakeredis.
"""

from __future__ import annotations

import pytest
from google.protobuf import struct_pb2

from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")
pytestmark = [pytest.mark.timeout(10)]


class _FakeRedis:
    def __init__(self) -> None:
        self._c = fakeredis_aio.FakeRedis()

    async def xadd(self, name: str, fields: dict) -> bytes:
        return await self._c.xadd(name, fields)  # type: ignore[return-value]

    async def xread(self, streams: dict, *, count: int = 50, block: int = 0) -> list:
        return await self._c.xread(streams, count=count, block=block)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._c.get(name)  # type: ignore[return-value]

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        return await self._c.set(name, value, ex=ex)  # type: ignore[return-value]

    async def close(self) -> None:
        await self._c.aclose()


def _pb(i: int) -> bytes:
    s = struct_pb2.Struct()
    s.update({"protocol": "chunk", "i": i})
    return s.SerializeToString()


async def _seed(redis: _FakeRedis, task_id: str, seqs: list[int]) -> None:
    key = f"task:{task_id}:stream"
    for sq in seqs:
        await redis.xadd(key, {"pb": _pb(sq), "seq": str(sq)})
    await redis.xadd(key, {"eos": b"true"})


@SKIP_NO_FAKEREDIS
class TestSkipToSeq:
    async def test_skip_suppresses_yields_and_tracks_last_seq(self) -> None:
        redis = _FakeRedis()
        try:
            await _seed(redis, "t1", [0, 1, 2, 3, 4, 5])
            reader = ProtoStreamReader("t1", redis)  # type: ignore[arg-type]
            got = [s async for s in reader.read_structs(skip_to_seq=3)]
            assert len(got) == 2  # only stored seq 4, 5
            assert reader._last_seq == 5  # cursor advanced past all consumed entries
        finally:
            await redis.close()

    async def test_skip_minus_one_yields_all(self) -> None:
        redis = _FakeRedis()
        try:
            await _seed(redis, "t2", [0, 1, 2])
            reader = ProtoStreamReader("t2", redis)  # type: ignore[arg-type]
            got = [s async for s in reader.read_structs(skip_to_seq=-1)]
            assert len(got) == 3  # -1 skips nothing (full replay incl. seq 0)
        finally:
            await redis.close()

    async def test_none_yields_all(self) -> None:
        redis = _FakeRedis()
        try:
            await _seed(redis, "t3", [0, 1, 2])
            reader = ProtoStreamReader("t3", redis)  # type: ignore[arg-type]
            got = [s async for s in reader.read_structs()]
            assert len(got) == 3
        finally:
            await redis.close()

    async def test_skip_across_trim_gap_yields_tail(self) -> None:
        redis = _FakeRedis()
        try:
            # Simulate a trim: seq 0,1 then a jump to 5,6 (2..4 trimmed).
            await _seed(redis, "t4", [0, 1, 5, 6])
            reader = ProtoStreamReader("t4", redis)  # type: ignore[arg-type]
            got = [s async for s in reader.read_structs(skip_to_seq=2)]
            assert len(got) == 2  # stored 5, 6
            assert reader._last_seq == 6
        finally:
            await redis.close()
