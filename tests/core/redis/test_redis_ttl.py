"""L0 — TTL lifecycle tests for Redis key expiration.

Tests EXPIRE/PERSIST/TTL patterns used by:
- RedisStateManager (task_ttl=24h)
- RedisIdempotency (idem_ttl=1h)
- proto stream output (stream_ttl=60s after EOS)

All tests use fakeredis, no real Redis needed.
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
    """Adapter for TTL testing with raw TTL access."""

    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        return await self._client.set(name, value, ex=ex)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def hset(self, name: str, mapping: dict) -> int:
        return await self._client.hset(name, mapping=mapping)  # type: ignore[return-value]

    async def expire(self, name: str, seconds: int) -> bool:
        return await self._client.expire(name, seconds)  # type: ignore[return-value]

    async def delete(self, *names: str) -> int:
        return await self._client.delete(*names)  # type: ignore[return-value]

    async def ttl(self, name: str) -> int:
        return await self._client.ttl(name)  # type: ignore[return-value]

    async def pttl(self, name: str) -> int:
        return await self._client.pttl(name)  # type: ignore[return-value]

    async def persist(self, name: str) -> bool:
        return await self._client.persist(name)  # type: ignore[return-value]

    def pipeline(self):
        return self._client.pipeline()

    async def xadd(self, name: str, fields: dict) -> bytes:
        return await self._client.xadd(name, fields)  # type: ignore[return-value]

    async def close(self) -> None:
        await self._client.aclose()


@pytest.fixture
async def client():
    c = _FakeRedisClient()
    yield c
    await c.close()


class TestExpireBasic:
    """EXPIRE/TTL/PERSIST round-trips."""

    async def test_expire_sets_ttl(self, client: _FakeRedisClient) -> None:
        await client.set("k", b"v")
        await client.expire("k", 3600)
        ttl = await client.ttl("k")
        assert 3500 < ttl <= 3600

    async def test_ttl_no_expiry_returns_negative(self, client: _FakeRedisClient) -> None:
        await client.set("k", b"v")
        ttl = await client.ttl("k")
        assert ttl == -1  # no TTL set

    async def test_ttl_nonexistent_key(self, client: _FakeRedisClient) -> None:
        ttl = await client.ttl("nonexistent")
        assert ttl == -2  # key does not exist

    async def test_persist_removes_ttl(self, client: _FakeRedisClient) -> None:
        await client.set("k", b"v", ex=100)
        ttl_before = await client.ttl("k")
        assert ttl_before > 0
        await client.persist("k")
        ttl_after = await client.ttl("k")
        assert ttl_after == -1

    async def test_set_with_ex_sets_ttl(self, client: _FakeRedisClient) -> None:
        await client.set("k", b"v", ex=60)
        ttl = await client.ttl("k")
        assert 55 < ttl <= 60

    async def test_pttl_millisecond_precision(self, client: _FakeRedisClient) -> None:
        await client.set("k", b"v", ex=10)
        pttl = await client.pttl("k")
        assert 9000 < pttl <= 10000


class TestPipelineTtl:
    """Atomic HSET + EXPIRE via pipeline — production pattern."""

    async def test_hset_expire_pipeline(self, client: _FakeRedisClient) -> None:
        """RedisStateManager pattern: set fields and TTL atomically."""
        pipe = client.pipeline()
        pipe.hset("task:abc", mapping={"status": "running", "started_at": "2025-01-01"})
        pipe.expire("task:abc", 86400)
        results = await pipe.execute()
        assert len(results) == 2

        ttl = await client.ttl("task:abc")
        assert ttl > 0

    async def test_stream_expire_after_eos(self, client: _FakeRedisClient) -> None:
        """ProtoStreamWriter.write_eos() sets stream TTL after EOS marker."""
        await client.xadd("task:stream:1", {"eos": "true"})
        await client.expire("task:stream:1", 60)
        ttl = await client.ttl("task:stream:1")
        assert 55 < ttl <= 60


class TestTtlProductionValues:
    """Verify SDK-specific TTL constants can be applied."""

    async def test_task_ttl_24h(self, client: _FakeRedisClient) -> None:
        await client.hset("task:t1", {"status": "pending"})
        await client.expire("task:t1", 86400)
        ttl = await client.ttl("task:t1")
        assert ttl > 86000

    async def test_claim_ttl_1h(self, client: _FakeRedisClient) -> None:
        await client.set("idem:task1", b"instance_a", ex=3600)
        ttl = await client.ttl("idem:task1")
        assert ttl > 3500

    async def test_stream_ttl_60s(self, client: _FakeRedisClient) -> None:
        await client.xadd("task:s1:stream", {"data": b"x"})
        await client.expire("task:s1:stream", 60)
        ttl = await client.ttl("task:s1:stream")
        assert 55 < ttl <= 60

class TestExpireOnDelete:
    """Keys with TTL are properly cleaned on DELETE."""

    async def test_delete_removes_ttl_key(self, client: _FakeRedisClient) -> None:
        await client.set("k", b"v", ex=3600)
        await client.delete("k")
        ttl = await client.ttl("k")
        assert ttl == -2  # key gone

    async def test_expire_then_overwrite_resets(self, client: _FakeRedisClient) -> None:
        await client.set("k", b"v1", ex=100)
        await client.set("k", b"v2")  # no ex → TTL removed
        ttl = await client.ttl("k")
        assert ttl == -1  # no TTL
