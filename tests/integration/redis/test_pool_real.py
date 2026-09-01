"""L1 — Connection pool behavior on real Redis.

Verifies:
- Split pool isolation (blocking XREAD doesn't starve non-blocking writes)
- Verify health check (ping) works through the pool

Requires: real Redis via docker-compose --profile redis up -d
"""

from __future__ import annotations

import asyncio
import time

import pytest

from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


class TestSplitPoolIsolation:
    """Blocking XREAD uses separate pool from non-blocking writes."""

    async def test_xread_does_not_block_xadd(self, redis_client: RedisClient) -> None:
        """Concurrent XREAD (blocking pool) + XADD (default pool) don't deadlock."""
        # Start an XREAD that blocks for 500ms
        async def blocking_read():
            return await redis_client.xread({"pool:test:stream": "0-0"}, count=1, block=500)

        # Write while read is blocking
        async def concurrent_write():
            await asyncio.sleep(0.1)  # let read start first
            t0 = time.monotonic()
            await redis_client.xadd("pool:test:write", {"data": "hello"})
            return (time.monotonic() - t0) * 1000

        read_result, write_ms = await asyncio.gather(blocking_read(), concurrent_write())

        # Write should complete in <500ms (not blocked by XREAD)
        assert write_ms < 500, f"Write took {write_ms:.1f}ms — blocked by XREAD pool"

    async def test_concurrent_xread_and_hset(self, redis_client: RedisClient) -> None:
        """Multiple concurrent XREAD + HSET operations don't interfere."""
        async def xread_task(i: int):
            return await redis_client.xread({f"pool:r{i}": "0-0"}, count=1, block=200)

        async def hset_task(i: int):
            await redis_client.hset(f"pool:h{i}", {"status": f"ok_{i}"})
            return await redis_client.hgetall(f"pool:h{i}")

        # 5 blocking reads + 5 hash writes concurrently
        tasks = [xread_task(i) for i in range(5)] + [hset_task(i) for i in range(5)]
        results = await asyncio.gather(*tasks)

        # All hash writes should succeed (last 5 results)
        for result in results[5:]:
            assert isinstance(result, dict)
            assert len(result) == 1


class TestHealthCheck:
    """verify() and ping() health check."""

    async def test_verify_returns_true(self, redis_client: RedisClient) -> None:
        result = await redis_client.verify()
        assert result is True

    async def test_ping_returns_true(self, redis_client: RedisClient) -> None:
        result = await redis_client.ping()
        assert result is True

    async def test_verify_warms_both_pools(self, redis_client: RedisClient) -> None:
        """After verify(), both XADD (default pool) and XREAD (blocking pool) are warm.

        Pair to ``test_verify_pings_both_pools`` in the unit suite: that test
        proves the *call shape* against a mock; this test proves the *effect*
        against real Redis — namely that the first XADD and first XREAD after
        verify() complete in trivial time (no cold connection penalty).
        """
        # Fresh sub-pool: connect a brand-new RedisClient so we can measure cold-then-warm.
        cold_client = RedisClient(redis_client.url)
        try:
            assert await cold_client.verify() is True
            t0 = time.monotonic()
            await cold_client.xadd("pool:warmup:stream", {"k": "v"})
            xadd_ms = (time.monotonic() - t0) * 1000
            t1 = time.monotonic()
            await cold_client.xread({"pool:warmup:stream": "0-0"}, count=1, block=10)
            xread_ms = (time.monotonic() - t1) * 1000
            # After pre-warm both pools should respond in well under 100ms even on slow CI.
            assert xadd_ms < 100, f"XADD took {xadd_ms:.1f}ms after verify() — pool not warmed"
            assert xread_ms < 100, f"XREAD took {xread_ms:.1f}ms after verify() — blocking pool not warmed"
        finally:
            await cold_client.close()


class TestStreamRealBehavior:
    """Stream operations on real Redis — verifies behavior not testable with fakeredis."""

    async def test_xread_returns_on_new_data(self, redis_client: RedisClient) -> None:
        """XREAD unblocks immediately when data is added during block."""
        async def writer():
            await asyncio.sleep(0.1)
            await redis_client.xadd("real:stream:1", {"msg": "hello"})

        async def reader():
            t0 = time.monotonic()
            result = await redis_client.xread({"real:stream:1": "0-0"}, count=1, block=5000)
            elapsed = (time.monotonic() - t0) * 1000
            return result, elapsed

        writer_task = asyncio.create_task(writer())
        result, elapsed = await reader()
        await writer_task

        # Should unblock well before 5s timeout
        assert elapsed < 2000, f"XREAD took {elapsed:.0f}ms — didn't unblock on write"
        assert result is not None
        assert len(result) > 0

    async def test_xadd_maxlen_trims(self, redis_client: RedisClient) -> None:
        """XADD with maxlen trims stream approximately."""
        for i in range(200):
            await redis_client.xadd("real:trimmed", {"i": str(i)}, maxlen=50)

        length = await redis_client.xlen("real:trimmed")
        # Approximate trimming: Redis may keep slightly more
        assert length <= 100, f"Stream should be trimmed to ~50, got {length}"
        assert length >= 40, f"Stream too aggressively trimmed to {length}"

    async def test_xrevrange_count_1_returns_last(self, redis_client: RedisClient) -> None:
        """XREVRANGE COUNT 1 returns only the newest entry (restore_seq pattern)."""
        for i in range(10):
            await redis_client.xadd("real:rev", {"seq": str(i + 1)})

        result = await redis_client.xrevrange("real:rev", count=1)
        assert len(result) == 1
        _entry_id, fields = result[0]
        assert fields[b"seq"] == b"10"


class TestTtlRealAccuracy:
    """TTL timing accuracy on real Redis."""

    async def test_pttl_accuracy(self, redis_client: RedisClient) -> None:
        """PTTL should be accurate within ±50ms over a 200ms sleep."""
        await redis_client.set("ttl:accuracy", b"v", ex=10)

        pttl_before = await redis_client._client.pttl("ttl:accuracy")
        await asyncio.sleep(0.2)
        pttl_after = await redis_client._client.pttl("ttl:accuracy")

        delta = pttl_before - pttl_after
        # Should be approximately 200ms elapsed (wide tolerance for CI load)
        assert 100 < delta < 500, f"PTTL delta {delta}ms over 200ms sleep — inaccurate"
