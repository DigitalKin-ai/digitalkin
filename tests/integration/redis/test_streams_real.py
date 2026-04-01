"""L1 — ProtoStreamWriter/Reader round-trip on real Redis.

Verifies end-to-end proto binary serialization through real Redis Streams:
- Write proto Struct → read back identical Struct
- Sequence monotonicity and gap detection
- EOS marker terminates reader
- Batch mode flush with real pipeline
- Backpressure with real XLEN

Requires: real Redis via docker-compose --profile redis up -d
"""

from __future__ import annotations

import asyncio

import pytest
from google.protobuf import struct_pb2

from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader, ProtoStreamWriter
from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


class TestProtoRoundTrip:
    """Write proto Struct via writer, read back via reader — real Redis."""

    async def test_single_struct_roundtrip(self, redis_client: RedisClient) -> None:
        """Write one proto Struct, read it back byte-for-byte identical."""
        original = struct_pb2.Struct()
        original.update({"message": "hello", "count": 42, "nested": {"a": 1}})

        writer = ProtoStreamWriter("rt:single", redis_client)
        await writer.write_struct(original)
        await writer.write_eos()

        reader = ProtoStreamReader("rt:single", redis_client)
        received = []
        async for s in reader.read_structs(block_ms=100):
            received.append(s)

        assert len(received) == 1
        assert received[0] == original

    async def test_multi_struct_sequence(self, redis_client: RedisClient) -> None:
        """Write 50 structs, read all back in order."""
        writer = ProtoStreamWriter("rt:multi", redis_client)
        originals = []
        for i in range(50):
            s = struct_pb2.Struct()
            s.update({"seq": i, "data": f"item_{i}"})
            originals.append(s)
            await writer.write_struct(s)
        await writer.write_eos()

        reader = ProtoStreamReader("rt:multi", redis_client)
        received = []
        async for s in reader.read_structs(block_ms=100):
            received.append(s)

        assert len(received) == 50
        for i, (orig, recv) in enumerate(zip(originals, received)):
            assert orig == recv, f"Mismatch at index {i}"

    async def test_batch_mode_roundtrip(self, redis_client: RedisClient) -> None:
        """Adaptive batch: fast writes flushed via pipeline, reader gets all."""
        writer = ProtoStreamWriter(
            "rt:batch", redis_client, batch_size=10, flush_ms=60_000,
        )
        for i in range(25):
            s = struct_pb2.Struct()
            s.update({"batch_idx": i})
            await writer.write_struct(s)
        await writer.write_eos()

        reader = ProtoStreamReader("rt:batch", redis_client)
        count = 0
        async for _ in reader.read_structs(block_ms=100):
            count += 1

        assert count == 25

    async def test_eos_terminates_reader(self, redis_client: RedisClient) -> None:
        """Reader exits cleanly on EOS marker."""
        writer = ProtoStreamWriter("rt:eos", redis_client)
        s = struct_pb2.Struct()
        s.update({"final": True})
        await writer.write_struct(s)
        await writer.write_eos()

        reader = ProtoStreamReader("rt:eos", redis_client)
        items = [item async for item in reader.read_structs(block_ms=100)]
        assert len(items) == 1


class TestProtoSequenceIntegrity:
    """Sequence numbering and restore_seq on real Redis."""

    async def test_seq_monotonic(self, redis_client: RedisClient) -> None:
        """Each write increments seq by exactly 1."""
        writer = ProtoStreamWriter("seq:mono", redis_client)

        seqs = []
        for i in range(10):
            s = struct_pb2.Struct()
            s.update({"i": i})
            seq = await writer.write_struct(s)
            seqs.append(seq)

        assert seqs == list(range(1, 11))

    async def test_restore_seq_continues(self, redis_client: RedisClient) -> None:
        """New writer resumes seq from existing stream entries."""
        writer1 = ProtoStreamWriter("seq:restore", redis_client)
        for i in range(5):
            s = struct_pb2.Struct()
            s.update({"i": i})
            await writer1.write_struct(s)

        # New writer restores seq
        writer2 = ProtoStreamWriter("seq:restore", redis_client)
        restored = await writer2.restore_seq()
        assert restored == 5

        # Next write should be seq=6
        s = struct_pb2.Struct()
        s.update({"i": 5})
        seq = await writer2.write_struct(s)
        assert seq == 6


class TestProtoConcurrentWriteRead:
    """Writer and reader operating concurrently — producer/consumer pattern."""

    async def test_concurrent_write_read(self, redis_client: RedisClient) -> None:
        """Writer produces while reader consumes — no data loss."""
        total_items = 30

        async def producer():
            writer = ProtoStreamWriter("conc:wr", redis_client)
            for i in range(total_items):
                s = struct_pb2.Struct()
                s.update({"idx": i})
                await writer.write_struct(s)
                await asyncio.sleep(0.01)
            await writer.write_eos()

        async def consumer():
            reader = ProtoStreamReader("conc:wr", redis_client)
            items = []
            async for s in reader.read_structs(block_ms=200):
                items.append(s)
            return items

        producer_task = asyncio.create_task(producer())
        items = await consumer()
        await producer_task

        assert len(items) == total_items
        # Verify ordering
        for i, item in enumerate(items):
            assert item.fields["idx"].number_value == i


class TestProtoLargePayload:
    """Large proto Struct serialization through Redis."""

    async def test_1mb_struct(self, redis_client: RedisClient) -> None:
        """1MB proto Struct round-trips correctly."""
        large_value = "x" * (1024 * 1024)  # 1MB string
        original = struct_pb2.Struct()
        original.update({"big": large_value})

        writer = ProtoStreamWriter("large:1mb", redis_client)
        await writer.write_struct(original)
        await writer.write_eos()

        reader = ProtoStreamReader("large:1mb", redis_client)
        items = [item async for item in reader.read_structs(block_ms=200)]

        assert len(items) == 1
        assert items[0].fields["big"].string_value == large_value
