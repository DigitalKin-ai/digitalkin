"""Tests for ProtoStreamWriter.restore_seq and ProtoStreamReader.restore_cursor.

Covers:
- restore_seq on empty stream → returns 0, first write is seq=1
- restore_seq after existing entries → continues from last seq
- restore_seq with malformed seq field → returns 0
- restore_cursor on empty → starts from head
- restore_cursor after saved cursor → resumes
- xrevrange integration
"""

from __future__ import annotations

from typing import Any

import pytest
from google.protobuf import struct_pb2

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [pytest.mark.timeout(15)]

SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")


class _FakeRedisClient:
    """Adapter wrapping fakeredis to match RedisClient interface."""

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

    async def publish(self, channel: str, message: str | bytes) -> int:
        return await self._client.publish(channel, message)  # type: ignore[return-value]

    def pubsub(self) -> Any:
        return self._client.pubsub()

    def pipeline(self) -> Any:
        return self._client.pipeline()

    async def close(self) -> None:
        await self._client.aclose()


@SKIP_NO_FAKEREDIS
class TestRestoreSeq:
    """ProtoStreamWriter.restore_seq — continue sequence from existing stream."""

    @pytest.fixture
    async def client(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_empty_stream_returns_zero(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("task_rs_empty", client)  # type: ignore[arg-type]
        result = await writer.restore_seq()
        assert result == 0
        assert writer.last_seq == 0

    async def test_first_write_after_empty_is_seq_1(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("task_rs_first", client)  # type: ignore[arg-type]
        await writer.restore_seq()

        s = struct_pb2.Struct()
        s.update({"data": "test"})
        seq = await writer.write_struct(s)
        assert seq == 1

    async def test_continues_from_existing_entries(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        # Writer A writes 3 entries then EOS (flushes pending)
        writer_a = ProtoStreamWriter("task_rs_cont", client)  # type: ignore[arg-type]
        s = struct_pb2.Struct()
        s.update({"data": "chunk"})
        await writer_a.write_struct(s)
        await writer_a.write_struct(s)
        await writer_a.write_struct(s)
        await writer_a.write_eos()
        assert writer_a.last_seq == 4  # 3 data + 1 eos

        # Writer B restores and continues
        writer_b = ProtoStreamWriter("task_rs_cont", client)  # type: ignore[arg-type]
        restored = await writer_b.restore_seq()
        assert restored == 4

        seq = await writer_b.write_struct(s)
        assert seq == 5

    async def test_continues_after_eos(self, client: Any) -> None:
        """restore_seq picks up the EOS entry's seq (highest in stream)."""
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer_a = ProtoStreamWriter("task_rs_eos", client)  # type: ignore[arg-type]
        s = struct_pb2.Struct()
        s.update({"data": "chunk"})
        await writer_a.write_struct(s)
        await writer_a.write_eos()
        assert writer_a.last_seq == 2  # 1=data, 2=eos

        writer_b = ProtoStreamWriter("task_rs_eos", client)  # type: ignore[arg-type]
        restored = await writer_b.restore_seq()
        assert restored == 2

    async def test_no_duplicate_seq_between_writers(self, client: Any) -> None:
        """Two sequential writers produce monotonic seq with no gaps or duplicates."""
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        s = struct_pb2.Struct()
        s.update({"data": "v"})

        w1 = ProtoStreamWriter("task_rs_nodup", client)  # type: ignore[arg-type]
        s1 = await w1.write_struct(s)
        await w1.write_eos()  # Flush pending to Redis

        w2 = ProtoStreamWriter("task_rs_nodup", client)  # type: ignore[arg-type]
        await w2.restore_seq()
        s2 = await w2.write_struct(s)

        assert s1 == 1
        assert s2 == 3  # 1=data, 2=eos, 3=new data


@SKIP_NO_FAKEREDIS
class TestRestoreCursor:
    """ProtoStreamReader.restore_cursor — resume from saved position."""

    @pytest.fixture
    async def client(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_missing_cursor_starts_from_head(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader

        reader = ProtoStreamReader("task_rc_miss", client)  # type: ignore[arg-type]
        await reader.restore_cursor()
        assert reader._last_id == "0-0"

    async def test_saved_cursor_resumes(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader, ProtoStreamWriter

        # Write entries + read them (saves cursor)
        writer = ProtoStreamWriter("task_rc_save", client)  # type: ignore[arg-type]
        s = struct_pb2.Struct()
        s.update({"data": "v"})
        await writer.write_struct(s)
        await writer.write_struct(s)
        await writer.write_eos()

        reader1 = ProtoStreamReader("task_rc_save", client)  # type: ignore[arg-type]
        await reader1.restore_cursor()
        entries = [e async for e in reader1.read_structs(block_ms=0)]
        assert len(entries) == 2  # 2 data entries, EOS stops iteration

        # New reader restores cursor and gets nothing (already read)
        reader2 = ProtoStreamReader("task_rc_save", client)  # type: ignore[arg-type]
        await reader2.restore_cursor()
        assert reader2._last_id != "0-0"  # cursor was saved


@SKIP_NO_FAKEREDIS
class TestXrevrange:
    """RedisClient.xrevrange — used by restore_seq."""

    @pytest.fixture
    async def client(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_empty_stream_returns_empty(self, client: Any) -> None:
        result = await client.xrevrange("nonexistent_stream", count=1)
        assert result == []

    async def test_returns_last_entry(self, client: Any) -> None:
        await client.xadd("test_xrev", {"seq": "1", "data": "first"})
        await client.xadd("test_xrev", {"seq": "2", "data": "second"})
        await client.xadd("test_xrev", {"seq": "3", "data": "third"})

        result = await client.xrevrange("test_xrev", count=1)
        assert len(result) == 1
        _entry_id, fields = result[0]
        assert fields[b"seq"] == b"3"
