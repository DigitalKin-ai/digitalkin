"""L1 — Redis manager classes against REAL Redis.

Exercises RedisIdempotency (paired with tests/core/redis/test_redis_idempotency.py) through their public APIs on the
docker Redis — the true end-to-end check of the Lua claim flow.
"""

from __future__ import annotations

import pytest

from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotency
from digitalkin.models.core.redis import ClaimResult

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


class TestRedisIdempotencyReal:
    """Atomic claim against real Redis: CLAIMED → RECLAIMED → TAKEN → release."""

    async def test_claim_lifecycle(self, redis_client) -> None:
        idem = RedisIdempotency(redis_client)
        assert await idem.claim("task_a", "inst_1") is ClaimResult.CLAIMED
        assert await idem.claim("task_a", "inst_1") is ClaimResult.RECLAIMED
        assert await idem.claim("task_a", "inst_2") is ClaimResult.TAKEN
        await idem.release("task_a")
        assert await idem.claim("task_a", "inst_2") is ClaimResult.CLAIMED


class TestStreamMaxlenReal:
    """The output stream is bounded by xadd(maxlen=...) — no unbounded growth."""

    async def test_xadd_maxlen_bounds_stream(self, redis_client) -> None:
        key = "task:bounded:stream"
        for seq in range(5000):
            await redis_client.xadd(key, {"pb": b"x", "seq": str(seq)}, maxlen=1000)
        # Approximate trimming keeps it near the cap, never the full 5000.
        assert await redis_client.xlen(key) < 2000
