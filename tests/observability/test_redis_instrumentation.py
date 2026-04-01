"""Tests for InstrumentedRedisClient observability wrapper.

Verifies:
- Every command is counted (command_count increments)
- Errors are tracked (error_count increments)
- Key values are NOT leaked in logs (structural pattern only)
- Commands pass through correctly (results match underlying client)
- Failing commands are re-raised after logging
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

from digitalkin.core.task_manager.redis.instrumented import InstrumentedRedisClient

pytestmark = [
    pytest.mark.timeout(15),
    pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed"),
]


class _FakeInner:
    """Inner client adapter for instrumentation testing."""

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

    async def xlen(self, name):
        return await self._client.xlen(name)

    async def xread(self, streams, *, count=50, block=100):
        return await self._client.xread(streams, count=count, block=block)

    async def xrevrange(self, name, max_id="+", min_id="-", count=None):
        return await self._client.xrevrange(name, max=max_id, min=min_id, count=count)

    async def delete(self, *names):
        return await self._client.delete(*names)

    async def expire(self, name, seconds):
        return await self._client.expire(name, seconds)

    async def ping(self):
        return await self._client.ping()

    async def publish(self, channel, message):
        return await self._client.publish(channel, message)

    async def zadd(self, name, mapping):
        return await self._client.zadd(name, mapping)

    async def zrangebyscore(self, name, min_score="-inf", max_score="+inf"):
        return await self._client.zrangebyscore(name, min_score, max_score)

    async def zrem(self, name, *members):
        return await self._client.zrem(name, *members)

    async def decr(self, name):
        return await self._client.decr(name)

    async def sadd(self, name, *values):
        return await self._client.sadd(name, *values)

    async def srem(self, name, *values):
        return await self._client.srem(name, *values)

    async def smembers(self, name):
        return await self._client.smembers(name)

    async def eval(self, script, keys, args):
        return await self._client.eval(script, len(keys), *keys, *args)

    def pipeline(self):
        return self._client.pipeline()

    def pubsub(self):
        return self._client.pubsub()

    async def close(self):
        await self._client.aclose()


@pytest.fixture
async def instrumented():
    inner = _FakeInner()
    client = InstrumentedRedisClient(inner)
    yield client
    await client.close()


class TestCommandCounting:
    """command_count increments on every operation."""

    async def test_set_increments_count(self, instrumented: InstrumentedRedisClient) -> None:
        assert instrumented.command_count == 0
        await instrumented.set("k", b"v")
        assert instrumented.command_count == 1

    async def test_multiple_commands_counted(self, instrumented: InstrumentedRedisClient) -> None:
        await instrumented.set("k1", b"v1")
        await instrumented.get("k1")
        await instrumented.hset("h", {"f": "v"})
        await instrumented.hgetall("h")
        await instrumented.ping()
        assert instrumented.command_count == 5

    async def test_stream_commands_counted(self, instrumented: InstrumentedRedisClient) -> None:
        await instrumented.xadd("s", {"d": b"x"})
        await instrumented.xlen("s")
        assert instrumented.command_count == 2


class TestErrorTracking:
    """error_count increments on command failure."""

    async def test_error_on_wrong_type(self) -> None:
        """Calling string op on a hash key raises and increments error_count."""
        inner = _FakeInner()
        client = InstrumentedRedisClient(inner)

        await client.hset("h", {"f": "v"})  # create as hash
        # GET on a hash key should raise WRONGTYPE
        try:
            await client.get("h")
        except Exception:
            pass

        assert client.error_count >= 1
        await client.close()

    async def test_error_reraises(self) -> None:
        """Failed commands re-raise the original exception."""
        inner = AsyncMock()
        inner.set = AsyncMock(side_effect=ConnectionError("down"))
        client = InstrumentedRedisClient(inner)

        with pytest.raises(ConnectionError, match="down"):
            await client.set("k", b"v")

        assert client.error_count == 1


class TestPassthrough:
    """Instrumented commands return the same results as the inner client."""

    async def test_set_get_passthrough(self, instrumented: InstrumentedRedisClient) -> None:
        await instrumented.set("pt:k", b"hello")
        result = await instrumented.get("pt:k")
        assert result == b"hello"

    async def test_hash_passthrough(self, instrumented: InstrumentedRedisClient) -> None:
        await instrumented.hset("pt:h", {"a": "1", "b": "2"})
        result = await instrumented.hgetall("pt:h")
        assert result[b"a"] == b"1"
        assert result[b"b"] == b"2"

    async def test_stream_passthrough(self, instrumented: InstrumentedRedisClient) -> None:
        entry_id = await instrumented.xadd("pt:s", {"msg": b"test"})
        assert entry_id is not None
        length = await instrumented.xlen("pt:s")
        assert length == 1

    async def test_sorted_set_passthrough(self, instrumented: InstrumentedRedisClient) -> None:
        await instrumented.zadd("pt:z", {"a": 1.0, "b": 2.0})
        result = await instrumented.zrangebyscore("pt:z", "-inf", "+inf")
        assert len(result) == 2

    async def test_set_ops_passthrough(self, instrumented: InstrumentedRedisClient) -> None:
        await instrumented.sadd("pt:set", "x", "y")
        members = await instrumented.smembers("pt:set")
        assert members == {b"x", b"y"}

    async def test_delete_passthrough(self, instrumented: InstrumentedRedisClient) -> None:
        await instrumented.set("pt:del", b"v")
        deleted = await instrumented.delete("pt:del")
        assert deleted == 1

    async def test_eval_passthrough(self, instrumented: InstrumentedRedisClient) -> None:
        result = await instrumented.eval("return 42", [], [])
        assert result == 42


class TestKeyPatternRedaction:
    """Key values are redacted — only structural patterns appear."""

    def test_simple_key(self) -> None:
        assert InstrumentedRedisClient._key_pattern("simple") == "simple"

    def test_two_part_key(self) -> None:
        assert InstrumentedRedisClient._key_pattern("task:abc123") == "task:*"

    def test_three_part_key(self) -> None:
        pattern = InstrumentedRedisClient._key_pattern("task:abc123:stream")
        assert pattern == "task:*:stream"

    def test_four_part_key(self) -> None:
        pattern = InstrumentedRedisClient._key_pattern("gateway:session:task_xyz:status")
        assert pattern == "gateway:*:*:status"

    def test_no_actual_id_leaked(self) -> None:
        """Specific task IDs never appear in the pattern."""
        pattern = InstrumentedRedisClient._key_pattern("task:secret-task-id-12345:stream")
        assert "secret-task-id-12345" not in pattern
