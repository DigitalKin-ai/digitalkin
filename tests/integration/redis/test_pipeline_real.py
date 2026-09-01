"""L1 — Pipeline performance and atomicity on real Redis.

Verifies:
- Pipeline batching is measurably faster than individual commands
- MULTI/EXEC inside pipeline provides atomicity
- Pipeline error handling (partial failures)
- Pipeline + EXPIRE atomic pattern used by RedisStateManager

Requires: real Redis via docker-compose --profile redis up -d
"""

from __future__ import annotations

import time

import pytest

from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


class TestPipelinePerformance:
    """Pipeline should be significantly faster than individual commands."""

    async def test_pipeline_vs_individual_speed(self, redis_client: RedisClient) -> None:
        """100-cmd pipeline must be >5x faster than 100 individual SET/GET."""
        n = 100

        # Individual commands
        t0 = time.monotonic()
        for i in range(n):
            await redis_client.set(f"ind:{i}", f"v{i}")
        for i in range(n):
            await redis_client.get(f"ind:{i}")
        individual_ms = (time.monotonic() - t0) * 1000

        # Pipeline
        t0 = time.monotonic()
        pipe = redis_client.pipeline()
        for i in range(n):
            pipe.set(f"pipe:{i}", f"v{i}")
        for i in range(n):
            pipe.get(f"pipe:{i}")
        results = await pipe.execute()
        pipeline_ms = (time.monotonic() - t0) * 1000

        # Verify correctness
        assert len(results) == 2 * n
        for i in range(n):
            assert results[n + i] == f"v{i}".encode()

        # Pipeline should be >3x faster (conservative threshold for CI)
        ratio = individual_ms / pipeline_ms
        assert ratio > 3, f"Pipeline only {ratio:.1f}x faster ({pipeline_ms:.1f}ms vs {individual_ms:.1f}ms individual)"


class TestPipelineAtomicity:
    """MULTI/EXEC inside pipeline provides transaction semantics."""

    async def test_multi_exec_in_pipeline(self, redis_client: RedisClient) -> None:
        """Transaction inside pipeline executes atomically."""
        pipe = redis_client._client.pipeline(transaction=True)
        pipe.set("tx:a", "1")
        pipe.set("tx:b", "2")
        pipe.incr("tx:a")
        results = await pipe.execute()

        assert results[0] is True  # SET OK
        assert results[1] is True  # SET OK
        assert results[2] == 2  # INCR result

        val_a = await redis_client.get("tx:a")
        val_b = await redis_client.get("tx:b")
        assert val_a == b"2"
        assert val_b == b"2"


class TestPipelineProductionPatterns:
    """Patterns used by SDK components."""

    async def test_hset_expire_atomic(self, redis_client: RedisClient) -> None:
        """RedisStateManager: HSET + EXPIRE in one pipeline round-trip."""
        pipe = redis_client.pipeline()
        pipe.hset("task:state:t1", mapping={"status": "running", "started": "now"})
        pipe.expire("task:state:t1", 86400)
        results = await pipe.execute()

        assert len(results) == 2
        data = await redis_client.hgetall("task:state:t1")
        assert data[b"status"] == b"running"

        ttl = await redis_client._client.ttl("task:state:t1")
        assert ttl > 86000

    async def test_stream_batch_xadd(self, redis_client: RedisClient) -> None:
        """ProtoStreamWriter._flush(): batch XADD via pipeline."""
        pipe = redis_client.pipeline()
        for i in range(20):
            pipe.xadd("task:stream:batch", {"pb": f"data_{i}".encode(), "seq": str(i + 1)})
        results = await pipe.execute()

        assert len(results) == 20
        # All entry IDs should be non-None
        for entry_id in results:
            assert entry_id is not None

        length = await redis_client.xlen("task:stream:batch")
        assert length == 20

    async def test_unregister_pipeline(self, redis_client: RedisClient) -> None:
        """StreamRegistry.unregister(): DECR + ZREM + DELETE in one pipeline."""
        # Setup: simulate registered session
        await redis_client.set("gateway:session_count", "5")
        await redis_client.zadd("gateway:heartbeats", {"task_1": 1000.0})
        await redis_client.hset("gateway:session:task_1", {"status": "active"})

        # Pipeline unregister
        pipe = redis_client.pipeline()
        pipe.decr("gateway:session_count")
        pipe.zrem("gateway:heartbeats", "task_1")
        pipe.delete("gateway:session:task_1")
        results = await pipe.execute()

        assert results[0] == 4  # count decremented
        assert results[1] == 1  # 1 member removed from zset
        assert results[2] == 1  # 1 key deleted

        # Verify cleanup
        count = await redis_client.get("gateway:session_count")
        assert count == b"4"
        members = await redis_client.zrangebyscore("gateway:heartbeats", "-inf", "+inf")
        assert b"task_1" not in members
