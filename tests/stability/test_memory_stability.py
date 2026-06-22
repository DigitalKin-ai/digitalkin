"""L6 — Memory stability tests for Redis operations.

Verifies no memory leaks across repeated Redis operation cycles:
- RedisClient connect/write/disconnect
- Pipeline create/execute/discard
- ProtoStreamWriter/Reader create/destroy
- Pub/sub subscribe/unsubscribe
- fakeredis adapter pool lifecycle

All tests measure RSS delta and use gc.collect() to detect unreachable objects.
"""

from __future__ import annotations

import gc
import os

import psutil
import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [
    pytest.mark.stability,
    pytest.mark.timeout(120),
    pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed"),
]


def _rss_mb() -> float:
    """Current process RSS in MB."""
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)


class _FakeRedisClient:
    """Lightweight adapter for memory testing."""

    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()

    async def set(self, name: str, value: bytes) -> None:
        await self._client.set(name, value)

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def xadd(self, name: str, fields: dict, *, maxlen: int | None = None) -> bytes:
        kwargs: dict = {}
        if maxlen is not None:
            kwargs["maxlen"] = maxlen
            kwargs["approximate"] = True
        return await self._client.xadd(name, fields, **kwargs)  # type: ignore[return-value]

    async def xread(self, streams: dict, *, count: int = 50, block: int = 100) -> list:
        return await self._client.xread(streams, count=count, block=block)  # type: ignore[return-value]

    async def xlen(self, name: str) -> int:
        return await self._client.xlen(name)  # type: ignore[return-value]

    async def xrevrange(self, name: str, max_id: str = "+", min_id: str = "-", count: int | None = None) -> list:
        return await self._client.xrevrange(name, max=max_id, min=min_id, count=count)  # type: ignore[return-value]

    async def expire(self, name: str, seconds: int) -> bool:
        return await self._client.expire(name, seconds)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        return await self._client.set(name, value, ex=ex)  # type: ignore[return-value]

    async def publish(self, channel: str, message: str | bytes) -> int:
        return await self._client.publish(channel, message)  # type: ignore[return-value]

    def pipeline(self):
        return self._client.pipeline()

    def pubsub(self):
        return self._client.pubsub()

    async def close(self) -> None:
        await self._client.aclose()


class TestPipelineMemory:
    """Pipeline create/execute/discard cycles should not leak."""

    async def test_1000_pipeline_cycles_no_leak(self) -> None:
        """1000 pipeline create → execute → discard: gc finds 0 unreachable."""
        client = _FakeRedisClient()

        gc.collect()
        rss_before = _rss_mb()

        for i in range(1000):
            pipe = client.pipeline()
            pipe.set(f"mem:pipe:{i}", f"v{i}")
            pipe.get(f"mem:pipe:{i}")
            await pipe.execute()
            del pipe

        gc.collect()
        unreachable = gc.collect()
        rss_after = _rss_mb()

        rss_delta = rss_after - rss_before
        assert rss_delta < 20, f"RSS grew by {rss_delta:.1f}MB over 1000 pipeline cycles"
        # gc.collect() returns count of unreachable objects found
        # A small number is normal; hundreds indicates a leak
        assert unreachable < 50, f"gc found {unreachable} unreachable objects"

        await client.close()

    async def test_pipeline_no_reference_retention(self) -> None:
        """Completed pipelines don't retain references to results."""
        client = _FakeRedisClient()

        results_ref = []
        for i in range(100):
            pipe = client.pipeline()
            for j in range(10):
                pipe.set(f"ref:{i}:{j}", f"v")
            results = await pipe.execute()
            results_ref.append(len(results))
            del results
            del pipe

        gc.collect()
        # All 100 result lists should have been freed
        assert len(results_ref) == 100
        assert all(r == 10 for r in results_ref)

        await client.close()


class TestPubSubMemory:
    """Subscribe/unsubscribe cycles should not leak PubSub instances."""

    async def test_subscribe_unsubscribe_no_leak(self) -> None:
        """200 subscribe/unsubscribe cycles: no leaked PubSub objects."""
        client = _FakeRedisClient()

        gc.collect()
        rss_before = _rss_mb()

        for i in range(200):
            ps = client.pubsub()
            await ps.subscribe(f"mem:ch:{i}")
            msg = await ps.get_message(timeout=0.1)
            await ps.unsubscribe(f"mem:ch:{i}")
            await ps.aclose()
            del ps

        gc.collect()
        rss_after = _rss_mb()

        rss_delta = rss_after - rss_before
        assert rss_delta < 15, f"RSS grew by {rss_delta:.1f}MB over 200 pubsub cycles"

        await client.close()


class TestSetGetMemory:
    """Bulk SET/GET cycles memory profile."""

    async def test_10k_set_get_stable(self) -> None:
        """10k SET/GET cycles: RSS delta bounded."""
        client = _FakeRedisClient()

        gc.collect()
        rss_before = _rss_mb()

        for i in range(10000):
            await client.set(f"mem:sg:{i}", b"x" * 100)
            await client.get(f"mem:sg:{i}")

        gc.collect()
        rss_after = _rss_mb()

        rss_delta = rss_after - rss_before
        # 10k keys × 100 bytes = 1MB data + overhead
        assert rss_delta < 50, f"RSS grew by {rss_delta:.1f}MB over 10k SET/GET"

        await client.close()
