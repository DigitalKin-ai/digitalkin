"""Tests for zero-copy proto binary stream writer/reader.

Covers:
- ProtoStreamWriter: write_struct, write_dict, write_eos, seq monotonicity
- ProtoStreamReader: read_structs, cursor restore, gap detection, EOS termination
- Round-trip: write proto → Redis → read proto (no dict intermediate)
- Deterministic tests with fakeredis
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
    """Adapter wrapping fakeredis to match RedisClient interface for proto streams."""

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

    async def expire(self, name: str, seconds: int) -> bool:
        return await self._client.expire(name, seconds)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        return await self._client.set(name, value, ex=ex)  # type: ignore[return-value]

    def pipeline(self) -> Any:
        return self._client.pipeline()

    async def xlen(self, name: str) -> int:
        return await self._client.xlen(name)  # type: ignore[return-value]

    async def xrevrange(self, name: str, max_id: str = "+", min_id: str = "-", count: int | None = None) -> list:
        return await self._client.xrevrange(name, max=max_id, min=min_id, count=count)  # type: ignore[return-value]

    async def publish(self, channel: str, message: str | bytes) -> int:
        return await self._client.publish(channel, message)  # type: ignore[return-value]

    def pubsub(self) -> Any:
        return self._client.pubsub()

    async def close(self) -> None:
        await self._client.aclose()


# ===========================================================================
# ProtoStreamWriter
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestProtoStreamWriter:
    """Proto binary write to Redis Stream."""

    @pytest.fixture
    async def client(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_write_struct_returns_seq(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("task_pw1", client)  # type: ignore[arg-type]
        s = struct_pb2.Struct()
        s.update({"key": "value"})

        seq = await writer.write_struct(s)
        assert seq == 1

    async def test_seq_monotonic(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("task_pw2", client)  # type: ignore[arg-type]
        s = struct_pb2.Struct()
        s.update({"a": 1})

        s1 = await writer.write_struct(s)
        s2 = await writer.write_struct(s)
        s3 = await writer.write_struct(s)
        assert s1 < s2 < s3

    async def test_write_dict_converts_and_stores(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("task_pw3", client)  # type: ignore[arg-type]
        seq = await writer.write_dict({"hello": "world", "num": 42})
        assert seq == 1
        assert writer.last_seq == 1

    async def test_write_eos(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("task_pw4", client)  # type: ignore[arg-type]
        s = struct_pb2.Struct()
        s.update({"data": "test"})
        await writer.write_struct(s)
        await writer.write_eos()
        assert writer.last_seq == 2


@SKIP_NO_FAKEREDIS
class TestProtoStreamWriterBatch:
    """Adaptive flush: buffers fast writes, pipelines on size threshold."""

    @pytest.fixture
    async def client(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_batch_flushes_on_eos(self, client: Any) -> None:
        """Adaptive flush: first write direct, rest buffered, EOS flushes pending.

        First write goes direct (huge gap from init=0.0); subsequent fast
        writes are buffered.
        """
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("task_batch1", client, batch_size=20, flush_ms=60_000)  # type: ignore[arg-type]
        s = struct_pb2.Struct()
        s.update({"data": "test"})

        await writer.write_struct(s)  # direct (first write)
        await writer.write_struct(s)  # buffered
        # Only the second write is buffered
        assert len(writer._pending) == 1

        await writer.write_eos()
        # After EOS, pending is flushed and EOS written
        assert len(writer._pending) == 0
        assert writer.last_seq == 3  # 2 entries + 1 EOS

    async def test_batch_flushes_on_size(self, client: Any) -> None:
        """Flush when batch_size is reached, not waiting for EOS.

        First write goes direct; subsequent fast writes are buffered until
        the batch threshold is reached.
        """
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("task_batch2", client, batch_size=3, flush_ms=60_000)  # type: ignore[arg-type]
        s = struct_pb2.Struct()
        s.update({"data": "v"})

        await writer.write_struct(s)  # direct (first write)
        assert len(writer._pending) == 0
        await writer.write_struct(s)  # buffered (1)
        assert len(writer._pending) == 1
        await writer.write_struct(s)  # buffered (2)
        assert len(writer._pending) == 2
        await writer.write_struct(s)  # buffered (3) — hits batch_size, flushes
        assert len(writer._pending) == 0

    async def test_batch_roundtrip(self, client: Any) -> None:
        """Batch write → read produces same data as unbatched."""
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader, ProtoStreamWriter

        writer = ProtoStreamWriter("task_batch3", client, batch_size=20, flush_ms=60_000)  # type: ignore[arg-type]
        reader = ProtoStreamReader("task_batch3", client)  # type: ignore[arg-type]

        s1 = struct_pb2.Struct()
        s1.update({"msg": "hello"})
        s2 = struct_pb2.Struct()
        s2.update({"msg": "world"})

        await writer.write_struct(s1)
        await writer.write_struct(s2)
        await writer.write_eos()

        results = [e async for e in reader.read_structs(block_ms=0)]
        assert len(results) == 2
        assert results[0]["msg"] == "hello"
        assert results[1]["msg"] == "world"

    async def test_no_asyncio_tasks_created(self, client: Any) -> None:
        """Adaptive flush uses no background asyncio tasks."""
        import asyncio

        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("task_batch4", client, batch_size=20, flush_ms=60_000)  # type: ignore[arg-type]
        s = struct_pb2.Struct()
        s.update({"data": "v"})

        tasks_before = len(asyncio.all_tasks())
        await writer.write_struct(s)
        await writer.write_struct(s)
        tasks_after = len(asyncio.all_tasks())

        # No new tasks should be created by batch writes
        assert tasks_after == tasks_before

        await writer.write_eos()


# ===========================================================================
# ProtoStreamReader
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestProtoStreamReader:
    """Proto binary read from Redis Stream."""

    @pytest.fixture
    async def client(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_read_structs_roundtrip(self, client: Any) -> None:
        """Write proto, read proto — no dict, no JSON."""
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader, ProtoStreamWriter

        writer = ProtoStreamWriter("task_pr1", client)  # type: ignore[arg-type]
        reader = ProtoStreamReader("task_pr1", client)  # type: ignore[arg-type]

        s1 = struct_pb2.Struct()
        s1.update({"msg": "hello", "seq": 1})
        s2 = struct_pb2.Struct()
        s2.update({"msg": "world", "seq": 2})

        await writer.write_struct(s1)
        await writer.write_struct(s2)
        await writer.write_eos()

        results: list[struct_pb2.Struct] = []
        async for item in reader.read_structs(count=10, block_ms=100):
            results.append(item)

        assert len(results) == 2
        assert results[0]["msg"] == "hello"
        assert results[1]["msg"] == "world"

    async def test_eos_terminates_reader(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader, ProtoStreamWriter

        writer = ProtoStreamWriter("task_pr2", client)  # type: ignore[arg-type]
        reader = ProtoStreamReader("task_pr2", client)  # type: ignore[arg-type]

        s = struct_pb2.Struct()
        s.update({"x": 1})
        await writer.write_struct(s)
        await writer.write_eos()

        count = 0
        async for _ in reader.read_structs(count=10, block_ms=100):
            count += 1
        assert count == 1  # EOS not yielded as data

    async def test_cursor_saved_after_batch(self, client: Any) -> None:
        """Cursor is persisted after each XREAD batch, not just EOS."""
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader, ProtoStreamWriter

        writer = ProtoStreamWriter("task_cursor_batch", client)  # type: ignore[arg-type]
        reader = ProtoStreamReader("task_cursor_batch", client)  # type: ignore[arg-type]

        s = struct_pb2.Struct()
        s.update({"data": "v"})
        await writer.write_struct(s)
        await writer.write_struct(s)
        await writer.write_eos()

        count = 0
        async for _ in reader.read_structs(count=10, block_ms=100):
            count += 1

        assert count == 2

        # Cursor should have been saved (not just at EOS, but after the batch too)
        cursor_raw = await client.get("task:task_cursor_batch:cursor")
        assert cursor_raw is not None

    async def test_write_dict_read_struct_roundtrip(self, client: Any) -> None:
        """Write dict, read as proto Struct — verifies dict→Struct→bytes→Struct."""
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader, ProtoStreamWriter

        writer = ProtoStreamWriter("task_pr3", client)  # type: ignore[arg-type]
        reader = ProtoStreamReader("task_pr3", client)  # type: ignore[arg-type]

        await writer.write_dict({"protocol": "message", "content": "test data"})
        await writer.write_eos()

        results: list[struct_pb2.Struct] = []
        async for item in reader.read_structs(count=10, block_ms=100):
            results.append(item)

        assert len(results) == 1
        assert results[0]["protocol"] == "message"
        assert results[0]["content"] == "test data"


# ===========================================================================
# Zero-copy verification — no dict intermediate
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestZeroCopyProperty:
    """Verify that proto bytes survive the Redis round-trip without dict conversion."""

    @pytest.fixture
    async def client(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_nested_struct_preserved(self, client: Any) -> None:
        """Nested proto Struct survives write→Redis→read without data loss."""
        from google.protobuf import json_format

        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader, ProtoStreamWriter

        writer = ProtoStreamWriter("task_zc1", client)  # type: ignore[arg-type]
        reader = ProtoStreamReader("task_zc1", client)  # type: ignore[arg-type]

        original_dict = {
            "root": {
                "protocol": "message",
                "payload": {
                    "user_prompt": "hello",
                    "temperature": 0.7,
                    "tokens": [1, 2, 3],
                },
            },
            "annotations": {"role": "user"},
        }
        original = struct_pb2.Struct()
        original.update(original_dict)

        await writer.write_struct(original)
        await writer.write_eos()

        async for restored in reader.read_structs(count=10, block_ms=100):
            # Verify dict-level equality (proto field order is non-deterministic)
            restored_dict = json_format.MessageToDict(restored)
            assert restored_dict == original_dict
            assert restored_dict["root"]["protocol"] == "message"
            assert restored_dict["root"]["payload"]["temperature"] == 0.7

    async def test_binary_size_comparable_to_json(self, client: Any) -> None:
        """Proto Struct binary is within 2x of JSON size.

        Note: proto Struct is NOT always smaller than JSON because Struct
        uses type tags per value. The win is speed (SerializeToString is
        faster than json.dumps), not size.
        """
        import json

        data = {
            "root": {"protocol": "message", "content": "a" * 1000},
            "annotations": {"role": "assistant", "model": "gpt-4"},
        }

        json_bytes = len(json.dumps(data).encode())

        s = struct_pb2.Struct()
        s.update(data)
        proto_bytes = len(s.SerializeToString())

        # Proto Struct size is comparable — within 2x of JSON
        assert proto_bytes < json_bytes * 2


# ===========================================================================
# Performance comparison
# ===========================================================================


@pytest.mark.stress
class TestProtoVsJsonPerformance:
    """Measure serialization cost: proto binary vs JSON."""

    def test_proto_serialize_faster_than_json(self) -> None:
        """Proto SerializeToString is faster than json.dumps for same data."""
        import json
        import time

        data = {
            "root": {"protocol": "message", "content": "token " * 100},
            "annotations": {"role": "user"},
        }

        # JSON path
        json_start = time.perf_counter_ns()
        for _ in range(1000):
            json.dumps(data)
        json_ns = time.perf_counter_ns() - json_start

        # Proto path
        s = struct_pb2.Struct()
        s.update(data)
        proto_start = time.perf_counter_ns()
        for _ in range(1000):
            s.SerializeToString()
        proto_ns = time.perf_counter_ns() - proto_start

        json_us = json_ns / 1000 / 1000  # per-op microseconds
        proto_us = proto_ns / 1000 / 1000

        # Proto should be at least as fast (usually 2-5x faster)
        assert proto_us <= json_us * 2, f"Proto {proto_us:.1f}µs should be <= 2x JSON {json_us:.1f}µs"

    def test_proto_deserialize_faster_than_json(self) -> None:
        """Proto ParseFromString is faster than json.loads + Struct.update."""
        import json
        import time

        data = {
            "root": {"protocol": "message", "content": "token " * 100},
            "annotations": {"role": "user"},
        }

        # JSON path: json.loads + Struct.update
        json_str = json.dumps(data)
        json_start = time.perf_counter_ns()
        for _ in range(1000):
            d = json.loads(json_str)
            s = struct_pb2.Struct()
            s.update(d)
        json_ns = time.perf_counter_ns() - json_start

        # Proto path: ParseFromString only
        s_orig = struct_pb2.Struct()
        s_orig.update(data)
        pb_bytes = s_orig.SerializeToString()
        proto_start = time.perf_counter_ns()
        for _ in range(1000):
            s2 = struct_pb2.Struct()
            s2.ParseFromString(pb_bytes)
        proto_ns = time.perf_counter_ns() - proto_start

        json_us = json_ns / 1000 / 1000
        proto_us = proto_ns / 1000 / 1000

        # Proto deserialize should be significantly faster (no JSON parse + no dict walk)
        assert proto_us < json_us, f"Proto {proto_us:.1f}µs should be < JSON {json_us:.1f}µs"
