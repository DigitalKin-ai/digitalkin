"""L1 — Lua script atomicity against REAL Redis (paired with tests/core/redis/test_redis_lua_scripts.py).

fakeredis[lua] diverges most from real Redis on Lua semantics (``false`` for
missing GET, ``SET ... 'EX'`` options, ``tonumber`` coercion). This pair runs
the production idempotency claim script and the registry capacity pattern
against the docker Redis to lock that behaviour down.
"""

from __future__ import annotations

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]

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


class TestLuaRegisterReal:
    """Atomic capacity check + heartbeat ZADD on real Redis."""

    async def test_register_below_capacity_succeeds(self, redis_client) -> None:
        assert await redis_client.eval(_LUA_REGISTER, ["count", "heartbeats"], ["10", "task_1", "1000"]) == 1

    async def test_register_at_capacity_fails(self, redis_client) -> None:
        await redis_client.set("count", "10")
        assert await redis_client.eval(_LUA_REGISTER, ["count", "heartbeats"], ["10", "task_x", "1000"]) == 0

    async def test_register_atomic_no_partial_state(self, redis_client) -> None:
        await redis_client.set("count", "5")
        await redis_client.eval(_LUA_REGISTER, ["count", "heartbeats"], ["5", "overflow", "9999"])
        assert await redis_client.get("count") == b"5"
        assert b"overflow" not in await redis_client.zrangebyscore("heartbeats", "-inf", "+inf")

    async def test_register_fills_to_exactly_max(self, redis_client) -> None:
        max_cap = 5
        results = [
            await redis_client.eval(_LUA_REGISTER, ["count", "heartbeats"], [str(max_cap), f"t{i}", str(i)])
            for i in range(max_cap + 3)
        ]
        assert results.count(1) == max_cap
        assert results.count(0) == 3
