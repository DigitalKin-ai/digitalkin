"""L5 — Canary shadow-write tests for ShadowRedisClient.

Verifies:
- Dual-write propagation (both clients receive writes)
- Read from stable only (canary errors hidden from caller)
- Canary error logged, not raised
- Circuit breaker opens on canary error spike
- Circuit re-enables after window reset

Uses fakeredis for both stable and canary — no real Redis needed.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [
    pytest.mark.timeout(15),
    pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed"),
]


class _FakeClient:
    """Minimal RedisClient adapter for canary testing."""

    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()

    async def set(self, name, value, *, ex=None):
        return await self._client.set(name, value, ex=ex)

    async def get(self, name):
        return await self._client.get(name)

    async def hset(self, name, mapping):
        return await self._client.hset(name, mapping=mapping)

    async def hgetall(self, name):
        return await self._client.hgetall(name)

    async def xadd(self, name, fields, *, maxlen=None):
        return await self._client.xadd(name, fields)

    async def delete(self, *names):
        return await self._client.delete(*names)

    async def expire(self, name, seconds):
        return await self._client.expire(name, seconds)

    async def close(self):
        await self._client.aclose()


@pytest.fixture
async def shadow():
    from digitalkin.core.task_manager.redis.shadow import ShadowRedisClient

    stable = _FakeClient()
    canary = _FakeClient()
    client = ShadowRedisClient(stable, canary, error_threshold_ratio=3.0, window_seconds=60.0)
    yield client, stable, canary
    await client.close()


class TestDualWrite:
    """Both stable and canary receive writes."""

    async def test_set_propagates_to_both(self, shadow) -> None:
        client, stable, canary = shadow

        await client.set("k", b"v")

        assert await stable.get("k") == b"v"
        assert await canary.get("k") == b"v"

    async def test_hset_propagates_to_both(self, shadow) -> None:
        client, stable, canary = shadow

        await client.hset("h", {"field": "val"})

        stable_data = await stable.hgetall("h")
        canary_data = await canary.hgetall("h")
        assert stable_data[b"field"] == b"val"
        assert canary_data[b"field"] == b"val"

    async def test_delete_propagates_to_both(self, shadow) -> None:
        client, stable, canary = shadow

        await client.set("d", b"v")
        await client.delete("d")

        assert await stable.get("d") is None
        assert await canary.get("d") is None

    async def test_expire_propagates_to_both(self, shadow) -> None:
        client, stable, canary = shadow

        await client.set("e", b"v")
        await client.expire("e", 3600)

        stable_ttl = await stable._client.ttl("e")
        canary_ttl = await canary._client.ttl("e")
        assert stable_ttl > 0
        assert canary_ttl > 0


class TestReadFromStableOnly:
    """Reads always come from stable, never from canary."""

    async def test_get_reads_stable(self, shadow) -> None:
        client, stable, canary = shadow

        await stable._client.set("read_test", b"stable_val")
        await canary._client.set("read_test", b"canary_val")

        result = await client.get("read_test")
        assert result == b"stable_val"

    async def test_hgetall_reads_stable(self, shadow) -> None:
        client, stable, canary = shadow

        await stable._client.hset("hread", mapping={"src": "stable"})
        await canary._client.hset("hread", mapping={"src": "canary"})

        result = await client.hgetall("hread")
        assert result[b"src"] == b"stable"


class TestCanaryErrorIsolation:
    """Canary errors are logged, never propagated to caller."""

    async def test_canary_error_hidden(self) -> None:
        """Canary failure doesn't affect caller."""
        from digitalkin.core.task_manager.redis.shadow import ShadowRedisClient

        stable = _FakeClient()
        canary = AsyncMock()
        canary.set = AsyncMock(side_effect=ConnectionError("canary down"))

        client = ShadowRedisClient(stable, canary)

        # Should not raise despite canary failure
        result = await client.set("k", b"v")
        assert result is True

        # Stable should have the value
        assert await stable.get("k") == b"v"

        await stable.close()

    async def test_stable_error_propagated(self) -> None:
        """Stable failure IS propagated to caller."""
        from digitalkin.core.task_manager.redis.shadow import ShadowRedisClient

        stable = AsyncMock()
        stable.set = AsyncMock(side_effect=ConnectionError("stable down"))
        canary = _FakeClient()

        client = ShadowRedisClient(stable, canary)

        with pytest.raises(ConnectionError, match="stable down"):
            await client.set("k", b"v")

        await canary.close()


class TestCircuitBreaker:
    """Canary disabled when error rate exceeds threshold."""

    async def test_circuit_opens_on_error_spike(self) -> None:
        """Circuit opens when canary_errors > ratio * stable_errors."""
        from digitalkin.core.task_manager.redis.shadow import ShadowRedisClient

        stable = _FakeClient()
        canary = AsyncMock()
        canary.set = AsyncMock(side_effect=ConnectionError("fail"))
        canary.close = AsyncMock()

        # ratio=3.0: canary disabled after 4 errors (> 3 * 1 stable_error baseline)
        client = ShadowRedisClient(stable, canary, error_threshold_ratio=3.0, window_seconds=60.0)

        for i in range(5):
            await client.set(f"k{i}", b"v")

        assert not client.canary_enabled, "Circuit should be open after 5 canary errors"

        # Further writes skip canary entirely
        canary.set.reset_mock()
        await client.set("after_circuit", b"v")
        canary.set.assert_not_called()

        await stable.close()

    async def test_circuit_resets_after_window(self) -> None:
        """Circuit re-enables after window expires."""
        from digitalkin.core.task_manager.redis.shadow import ShadowRedisClient

        stable = _FakeClient()
        canary = AsyncMock()
        canary.set = AsyncMock(side_effect=ConnectionError("fail"))
        canary.close = AsyncMock()

        client = ShadowRedisClient(stable, canary, error_threshold_ratio=2.0, window_seconds=0.05)

        # Trip the circuit
        for i in range(5):
            await client.set(f"k{i}", b"v")

        assert not client.canary_enabled

        # Wait for window to expire (wide margin)
        await asyncio.sleep(0.2)

        # Next call should re-enable canary (window reset)
        canary.set = AsyncMock(return_value=True)
        await client.set("after_reset", b"v")

        assert client.canary_enabled

        await stable.close()
