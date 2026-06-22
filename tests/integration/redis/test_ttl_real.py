"""L1 — TTL/EXPIRE lifecycle against REAL Redis (paired with tests/core/redis/test_redis_ttl.py).

Locks down real-Redis EXPIRE/TTL/PERSIST/SET-EX semantics (TTL -1 vs -2,
PERSIST clearing TTL, overwrite-without-EX clearing TTL) for the production
TTL constants used by state/checkpoint/idempotency/stream managers.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


class TestExpireBasicReal:
    async def test_expire_sets_ttl(self, redis_client) -> None:
        await redis_client.set("k", b"v")
        await redis_client.expire("k", 3600)
        assert 3500 < await redis_client._client.ttl("k") <= 3600

    async def test_ttl_no_expiry_returns_negative_one(self, redis_client) -> None:
        await redis_client.set("k", b"v")
        assert await redis_client._client.ttl("k") == -1

    async def test_ttl_nonexistent_key_returns_negative_two(self, redis_client) -> None:
        assert await redis_client._client.ttl("nonexistent") == -2

    async def test_persist_removes_ttl(self, redis_client) -> None:
        await redis_client.set("k", b"v", ex=100)
        assert await redis_client._client.ttl("k") > 0
        await redis_client._client.persist("k")
        assert await redis_client._client.ttl("k") == -1

    async def test_set_with_ex_sets_ttl(self, redis_client) -> None:
        await redis_client.set("k", b"v", ex=60)
        assert 55 < await redis_client._client.ttl("k") <= 60

    async def test_pttl_millisecond_precision(self, redis_client) -> None:
        await redis_client.set("k", b"v", ex=10)
        assert 9000 < await redis_client._client.pttl("k") <= 10000


class TestPipelineTtlReal:
    async def test_hset_expire_pipeline(self, redis_client) -> None:
        pipe = redis_client.pipeline()
        pipe.hset("task:abc", mapping={"status": "running", "started_at": "2025-01-01"})
        pipe.expire("task:abc", 86400)
        results = await pipe.execute()
        assert len(results) == 2
        assert await redis_client._client.ttl("task:abc") > 0

    async def test_stream_expire_after_eos(self, redis_client) -> None:
        await redis_client.xadd("task:stream:1", {"eos": b"true"})
        await redis_client.expire("task:stream:1", 60)
        assert 55 < await redis_client._client.ttl("task:stream:1") <= 60


class TestTtlProductionValuesReal:
    async def test_task_ttl_24h(self, redis_client) -> None:
        await redis_client.hset("task:t1", {"status": "pending"})
        await redis_client.expire("task:t1", 86400)
        assert await redis_client._client.ttl("task:t1") > 86000

    async def test_claim_ttl_1h(self, redis_client) -> None:
        await redis_client.set("idem:task1", b"instance_a", ex=3600)
        assert await redis_client._client.ttl("idem:task1") > 3500


class TestExpireOnDeleteReal:
    async def test_delete_removes_ttl_key(self, redis_client) -> None:
        await redis_client.set("k", b"v", ex=3600)
        await redis_client.delete("k")
        assert await redis_client._client.ttl("k") == -2

    async def test_overwrite_without_ex_clears_ttl(self, redis_client) -> None:
        await redis_client.set("k", b"v1", ex=100)
        await redis_client.set("k", b"v2")
        assert await redis_client._client.ttl("k") == -1
