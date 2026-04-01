"""L0 — Lua script atomicity tests for SDK-specific scripts.

Tests the two Lua scripts used in production:
1. _LUA_REGISTER (stream_registry.py) — atomic capacity check + heartbeat ZADD
2. _CLAIM_SCRIPT (redis_idempotency.py) — atomic task claim with CLAIMED/RECLAIMED/TAKEN

All tests use fakeredis[lua] for hermetic execution.
"""

from __future__ import annotations

import asyncio

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [
    pytest.mark.timeout(15),
    pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed"),
]


class _FakeRedisClient:
    """Minimal adapter for Lua script testing."""

    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()

    async def eval(self, script: str, keys: list[str], args: list[str]) -> int | str | bytes | None:
        return await self._client.eval(script, len(keys), *keys, *args)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def set(self, name: str, value: str | bytes) -> bool:
        return await self._client.set(name, value)  # type: ignore[return-value]

    async def hset(self, name: str, mapping: dict) -> int:
        return await self._client.hset(name, mapping=mapping)  # type: ignore[return-value]

    async def hgetall(self, name: str) -> dict:
        return await self._client.hgetall(name)  # type: ignore[return-value]

    async def zadd(self, name: str, mapping: dict[str, float]) -> int:
        return await self._client.zadd(name, mapping)  # type: ignore[return-value]

    async def zrangebyscore(self, name: str, min_score: str, max_score: str) -> list:
        return await self._client.zrangebyscore(name, min_score, max_score)  # type: ignore[return-value]

    async def close(self) -> None:
        await self._client.aclose()


@pytest.fixture
async def client():
    c = _FakeRedisClient()
    yield c
    await c.close()


# ══════════════════════════════════════════════════════════════════════════════
# _LUA_REGISTER — StreamRegistry capacity check
# ══════════════════════════════════════════════════════════════════════════════

# Production script from stream_registry.py
_LUA_REGISTER = """
local count_key = KEYS[1]
local hb_key = KEYS[2]
local max = tonumber(ARGV[1])
local task_id = ARGV[2]
local now = tonumber(ARGV[3])
local current = tonumber(redis.call('GET', count_key) or '0')
if current >= max then
    return 0
end
redis.call('INCR', count_key)
redis.call('EXPIRE', count_key, 3600)
redis.call('ZADD', hb_key, now, task_id)
return 1
"""


class TestLuaRegister:
    """Atomic capacity check + heartbeat ZADD."""

    async def test_register_below_capacity_succeeds(self, client: _FakeRedisClient) -> None:
        result = await client.eval(_LUA_REGISTER, ["count", "heartbeats"], ["10", "task_1", "1000"])
        assert result == 1

    async def test_register_at_capacity_fails(self, client: _FakeRedisClient) -> None:
        await client.set("count", "10")
        result = await client.eval(_LUA_REGISTER, ["count", "heartbeats"], ["10", "task_x", "1000"])
        assert result == 0

    async def test_register_increments_counter(self, client: _FakeRedisClient) -> None:
        await client.eval(_LUA_REGISTER, ["count", "heartbeats"], ["100", "t1", "1000"])
        await client.eval(_LUA_REGISTER, ["count", "heartbeats"], ["100", "t2", "2000"])
        val = await client.get("count")
        assert val == b"2"

    async def test_register_adds_heartbeat(self, client: _FakeRedisClient) -> None:
        await client.eval(_LUA_REGISTER, ["count", "heartbeats"], ["100", "task_abc", "1500"])
        members = await client.zrangebyscore("heartbeats", "-inf", "+inf")
        assert b"task_abc" in members

    async def test_register_atomic_no_partial_state(self, client: _FakeRedisClient) -> None:
        """When capacity is exceeded, neither counter nor heartbeat should change."""
        await client.set("count", "5")
        await client.eval(_LUA_REGISTER, ["count", "heartbeats"], ["5", "overflow", "9999"])
        val = await client.get("count")
        assert val == b"5"  # not incremented
        members = await client.zrangebyscore("heartbeats", "-inf", "+inf")
        assert b"overflow" not in members  # not added

    async def test_register_concurrent_fills_to_max(self, client: _FakeRedisClient) -> None:
        """Sequential registrations stop at exactly max capacity."""
        max_cap = 5
        results = []
        for i in range(max_cap + 3):
            r = await client.eval(_LUA_REGISTER, ["count", "heartbeats"], [str(max_cap), f"t{i}", str(i)])
            results.append(r)
        assert results.count(1) == max_cap
        assert results.count(0) == 3


# ══════════════════════════════════════════════════════════════════════════════
# _CLAIM_SCRIPT — IdempotencyGuard atomic claim
# ══════════════════════════════════════════════════════════════════════════════

# Production script from redis_idempotency.py
_CLAIM_SCRIPT = """
local key = KEYS[1]
local instance_id = ARGV[1]
local ttl = tonumber(ARGV[2])
local current = redis.call('GET', key)
if current == false then
    redis.call('SET', key, instance_id, 'EX', ttl)
    return 'CLAIMED'
elseif current == instance_id then
    redis.call('EXPIRE', key, ttl)
    return 'RECLAIMED'
else
    return 'TAKEN'
end
"""


class TestLuaClaim:
    """Atomic idempotency claim: CLAIMED / RECLAIMED / TAKEN."""

    async def test_claim_new_task_returns_claimed(self, client: _FakeRedisClient) -> None:
        result = await client.eval(_CLAIM_SCRIPT, ["idem:task1"], ["instance_a", "3600"])
        assert result == b"CLAIMED"

    async def test_claim_same_instance_returns_reclaimed(self, client: _FakeRedisClient) -> None:
        await client.eval(_CLAIM_SCRIPT, ["idem:task2"], ["instance_a", "3600"])
        result = await client.eval(_CLAIM_SCRIPT, ["idem:task2"], ["instance_a", "3600"])
        assert result == b"RECLAIMED"

    async def test_claim_different_instance_returns_taken(self, client: _FakeRedisClient) -> None:
        await client.eval(_CLAIM_SCRIPT, ["idem:task3"], ["instance_a", "3600"])
        result = await client.eval(_CLAIM_SCRIPT, ["idem:task3"], ["instance_b", "3600"])
        assert result == b"TAKEN"

    async def test_claim_sets_key_value(self, client: _FakeRedisClient) -> None:
        await client.eval(_CLAIM_SCRIPT, ["idem:task4"], ["inst_x", "3600"])
        val = await client.get("idem:task4")
        assert val == b"inst_x"

    async def test_reclaim_resets_ttl(self, client: _FakeRedisClient) -> None:
        await client.eval(_CLAIM_SCRIPT, ["idem:task5"], ["inst_x", "100"])
        await client.eval(_CLAIM_SCRIPT, ["idem:task5"], ["inst_x", "7200"])
        ttl = await client._client.ttl("idem:task5")
        assert ttl > 100  # TTL was reset to 7200

    async def test_claim_sequence_three_instances(self, client: _FakeRedisClient) -> None:
        """First claims, second is taken, first reclaims."""
        r1 = await client.eval(_CLAIM_SCRIPT, ["idem:seq"], ["A", "3600"])
        assert r1 == b"CLAIMED"
        r2 = await client.eval(_CLAIM_SCRIPT, ["idem:seq"], ["B", "3600"])
        assert r2 == b"TAKEN"
        r3 = await client.eval(_CLAIM_SCRIPT, ["idem:seq"], ["A", "3600"])
        assert r3 == b"RECLAIMED"
