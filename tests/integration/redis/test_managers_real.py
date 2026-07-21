"""L1 — Redis manager classes against REAL Redis.

Exercises RedisStateManager (paired with
tests/core/redis/test_redis_deterministic.py) and RedisIdempotency (paired with
tests/core/redis/test_redis_idempotency.py) through their public APIs on the
docker Redis — the true end-to-end check of the Lua claim flow.
"""

from __future__ import annotations

import pytest

from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotency
from digitalkin.core.task_manager.redis.redis_state import RedisStateManager
from digitalkin.models.core.redis import ClaimResult

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


class TestRedisStateManagerReal:
    async def test_set_and_get_status(self, redis_client) -> None:
        mgr = RedisStateManager(redis_client)
        await mgr.set_status("task_1", "running", started_at="2025-01-01T00:00:00Z")
        result = await mgr.get_status("task_1")
        assert result["status"] == "running"
        assert result["started_at"] == "2025-01-01T00:00:00Z"

    async def test_status_transitions_overwrite(self, redis_client) -> None:
        mgr = RedisStateManager(redis_client)
        await mgr.set_status("task_2", "pending")
        await mgr.set_status("task_2", "running")
        await mgr.set_status("task_2", "completed")
        assert (await mgr.get_status("task_2"))["status"] == "completed"

    async def test_get_nonexistent_returns_empty(self, redis_client) -> None:
        assert await RedisStateManager(redis_client).get_status("nonexistent") == {}

    async def test_record_exception_persists(self, redis_client) -> None:
        mgr = RedisStateManager(redis_client)
        await mgr.set_status("task_3", "failed")
        await mgr.record_exception("task_3", "boom", "traceback here")
        result = await mgr.get_status("task_3")
        assert result["error_message"] == "boom"
        assert result["exception_traceback"] == "traceback here"

    async def test_register_task_sets_pending(self, redis_client) -> None:
        mgr = RedisStateManager(redis_client)
        await mgr.register_task("task_4", "missions:m1", "setups:s1", "setup_versions:sv1")
        result = await mgr.get_status("task_4")
        assert result["status"] == "pending"
        assert result["mission_id"] == "missions:m1"


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
