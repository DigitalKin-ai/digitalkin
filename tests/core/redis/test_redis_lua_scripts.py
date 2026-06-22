"""L0 — Lua script atomicity test for the idempotency claim.

Tests the ``_CLAIM_SCRIPT`` (redis_idempotency.py) — atomic task claim with
CLAIMED/RECLAIMED/TAKEN. Uses fakeredis[lua] for hermetic execution.
"""

from __future__ import annotations

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


# Production script mirroring redis_idempotency.RedisIdempotency.claim.
# Returns the ClaimResult integer value: TAKEN=0, CLAIMED=1, RECLAIMED=2.
_CLAIM_SCRIPT = """
local key = KEYS[1]
local instance_id = ARGV[1]
local ttl = tonumber(ARGV[2])
local current = redis.call('GET', key)
if current == false then
    redis.call('SET', key, instance_id, 'EX', ttl)
    return 1
elseif current == instance_id then
    redis.call('EXPIRE', key, ttl)
    return 2
else
    return 0
end
"""


class TestLuaClaim:
    """Atomic idempotency claim: CLAIMED(1) / RECLAIMED(2) / TAKEN(0)."""

    async def test_claim_new_task_returns_claimed(self, client: _FakeRedisClient) -> None:
        result = await client.eval(_CLAIM_SCRIPT, ["idem:task1"], ["instance_a", "3600"])
        assert result == 1

    async def test_claim_same_instance_returns_reclaimed(self, client: _FakeRedisClient) -> None:
        await client.eval(_CLAIM_SCRIPT, ["idem:task2"], ["instance_a", "3600"])
        result = await client.eval(_CLAIM_SCRIPT, ["idem:task2"], ["instance_a", "3600"])
        assert result == 2

    async def test_claim_different_instance_returns_taken(self, client: _FakeRedisClient) -> None:
        await client.eval(_CLAIM_SCRIPT, ["idem:task3"], ["instance_a", "3600"])
        result = await client.eval(_CLAIM_SCRIPT, ["idem:task3"], ["instance_b", "3600"])
        assert result == 0

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
        assert r1 == 1
        r2 = await client.eval(_CLAIM_SCRIPT, ["idem:seq"], ["B", "3600"])
        assert r2 == 0
        r3 = await client.eval(_CLAIM_SCRIPT, ["idem:seq"], ["A", "3600"])
        assert r3 == 2
