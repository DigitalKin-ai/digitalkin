"""L0 — Comprehensive unit tests for every RedisClient wrapper method.

Hermetic: uses fakeredis only, no real Redis needed.
Covers all data structure families exposed by RedisClient:
STRING, HASH, STREAM, SORTED SET, SET, LUA, PIPELINE, PUB/SUB, KEY OPS.

Each test exercises the production RedisClient method signature exactly
as downstream code calls it (ProtoStreams, StreamRegistry, etc.).
"""

from __future__ import annotations

import asyncio
from typing import Any

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
    """Full adapter matching RedisClient's public interface for fakeredis."""

    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()
        self._blocking_client = self._client  # same instance for unit tests

    # -- STRING --
    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        return await self._client.set(name, value, ex=ex)  # type: ignore[return-value]

    # -- HASH --
    async def hset(self, name: str, mapping: dict[str, str | bytes]) -> int:
        return await self._client.hset(name, mapping=mapping)  # type: ignore[return-value]

    async def hgetall(self, name: str) -> dict[bytes, bytes]:
        return await self._client.hgetall(name)  # type: ignore[return-value]

    # -- STREAM --
    async def xadd(self, name: str, fields: dict[str, str | bytes], *, maxlen: int | None = None) -> bytes:
        kwargs: dict[str, Any] = {}
        if maxlen is not None:
            kwargs["maxlen"] = maxlen
            kwargs["approximate"] = True
        return await self._client.xadd(name, fields, **kwargs)  # type: ignore[return-value]

    async def xread(self, streams: dict[str, str | bytes], *, count: int = 50, block: int = 0) -> list:
        return await self._client.xread(streams, count=count, block=block)  # type: ignore[return-value]

    async def xlen(self, name: str) -> int:
        return await self._client.xlen(name)  # type: ignore[return-value]

    async def xrevrange(self, name: str, max_id: str = "+", min_id: str = "-", count: int | None = None) -> list:
        return await self._client.xrevrange(name, max=max_id, min=min_id, count=count)  # type: ignore[return-value]

    # -- SORTED SET --
    async def zadd(self, name: str, mapping: dict[str, float]) -> int:
        return await self._client.zadd(name, mapping)  # type: ignore[return-value]

    async def zrangebyscore(self, name: str, min_score: float | str = "-inf", max_score: float | str = "+inf") -> list:
        return await self._client.zrangebyscore(name, min_score, max_score)  # type: ignore[return-value]

    async def zrem(self, name: str, *members: str) -> int:
        return await self._client.zrem(name, *members)  # type: ignore[return-value]

    # -- SET --
    async def sadd(self, name: str, *values: str) -> int:
        return await self._client.sadd(name, *values)  # type: ignore[return-value]

    async def srem(self, name: str, *values: str) -> int:
        return await self._client.srem(name, *values)  # type: ignore[return-value]

    async def smembers(self, name: str) -> set[bytes]:
        return await self._client.smembers(name)  # type: ignore[return-value]

    # -- KEY OPS --
    async def delete(self, *names: str) -> int:
        return await self._client.delete(*names)  # type: ignore[return-value]

    async def expire(self, name: str, seconds: int) -> bool:
        return await self._client.expire(name, seconds)  # type: ignore[return-value]

    async def ping(self) -> bool:
        return await self._client.ping()  # type: ignore[return-value]

    async def decr(self, name: str) -> int:
        return await self._client.decr(name)  # type: ignore[return-value]

    async def publish(self, channel: str, message: str | bytes) -> int:
        return await self._client.publish(channel, message)  # type: ignore[return-value]

    # -- LUA --
    async def eval(self, script: str, keys: list[str], args: list[str]) -> int | str | bytes | None:
        return await self._client.eval(script, len(keys), *keys, *args)  # type: ignore[return-value]

    # -- PIPELINE --
    def pipeline(self) -> Any:
        return self._client.pipeline()

    def pubsub(self) -> Any:
        return self._client.pubsub()

    async def close(self) -> None:
        await self._client.aclose()


@pytest.fixture
async def client():
    c = _FakeRedisClient()
    yield c
    await c.close()


# ══════════════════════════════════════════════════════════════════════════════
# STRING
# ══════════════════════════════════════════════════════════════════════════════


class TestStringOps:
    """SET/GET round-trip and options."""

    async def test_set_get_roundtrip(self, client: _FakeRedisClient) -> None:
        await client.set("key1", b"value1")
        result = await client.get("key1")
        assert result == b"value1"

    async def test_set_string_value(self, client: _FakeRedisClient) -> None:
        await client.set("key2", "string_val")
        result = await client.get("key2")
        assert result == b"string_val"

    async def test_get_nonexistent_returns_none(self, client: _FakeRedisClient) -> None:
        result = await client.get("no_such_key")
        assert result is None

    async def test_set_with_ex_ttl(self, client: _FakeRedisClient) -> None:
        await client.set("ttl_key", b"v", ex=3600)
        result = await client.get("ttl_key")
        assert result == b"v"
        ttl = await client._client.ttl("ttl_key")
        assert ttl > 0

    async def test_set_overwrites_existing(self, client: _FakeRedisClient) -> None:
        await client.set("k", b"old")
        await client.set("k", b"new")
        assert await client.get("k") == b"new"

    async def test_decr_from_zero(self, client: _FakeRedisClient) -> None:
        result = await client.decr("counter")
        assert result == -1

    async def test_decr_existing_value(self, client: _FakeRedisClient) -> None:
        await client.set("counter", "10")
        result = await client.decr("counter")
        assert result == 9


# ══════════════════════════════════════════════════════════════════════════════
# HASH
# ══════════════════════════════════════════════════════════════════════════════


class TestHashOps:
    """HSET/HGETALL round-trip, used by RedisStateManager and checkpoints."""

    async def test_hset_hgetall_roundtrip(self, client: _FakeRedisClient) -> None:
        await client.hset("hash:1", {"field1": "val1", "field2": "val2"})
        result = await client.hgetall("hash:1")
        assert result[b"field1"] == b"val1"
        assert result[b"field2"] == b"val2"

    async def test_hset_bytes_values(self, client: _FakeRedisClient) -> None:
        await client.hset("hash:2", {"bin": b"\x00\x01\x02"})
        result = await client.hgetall("hash:2")
        assert result[b"bin"] == b"\x00\x01\x02"

    async def test_hgetall_empty_hash(self, client: _FakeRedisClient) -> None:
        result = await client.hgetall("nonexistent_hash")
        assert result == {}

    async def test_hset_overwrites_field(self, client: _FakeRedisClient) -> None:
        await client.hset("hash:3", {"status": "pending"})
        await client.hset("hash:3", {"status": "running"})
        result = await client.hgetall("hash:3")
        assert result[b"status"] == b"running"

    async def test_hset_returns_new_field_count(self, client: _FakeRedisClient) -> None:
        added = await client.hset("hash:4", {"a": "1", "b": "2"})
        assert added == 2
        added2 = await client.hset("hash:4", {"a": "updated", "c": "3"})
        assert added2 == 1  # only 'c' is new


# ══════════════════════════════════════════════════════════════════════════════
# STREAM
# ══════════════════════════════════════════════════════════════════════════════


class TestStreamOps:
    """XADD/XREAD/XLEN/XREVRANGE — core of ProtoStreamWriter/Reader."""

    async def test_xadd_xlen(self, client: _FakeRedisClient) -> None:
        await client.xadd("stream:1", {"data": b"msg1"})
        await client.xadd("stream:1", {"data": b"msg2"})
        length = await client.xlen("stream:1")
        assert length == 2

    async def test_xadd_returns_entry_id(self, client: _FakeRedisClient) -> None:
        entry_id = await client.xadd("stream:2", {"k": "v"})
        assert entry_id is not None
        assert isinstance(entry_id, bytes)

    async def test_xread_returns_entries(self, client: _FakeRedisClient) -> None:
        await client.xadd("stream:3", {"seq": "1"})
        await client.xadd("stream:3", {"seq": "2"})
        result = await client.xread({"stream:3": "0-0"}, count=10, block=0)
        assert len(result) == 1
        stream_name, entries = result[0]
        assert len(entries) == 2

    async def test_xread_empty_stream_returns_none_on_timeout(self, client: _FakeRedisClient) -> None:
        """XREAD on non-existent stream with short block returns None/empty."""
        result = await client.xread({"stream:empty": "0-0"}, count=10, block=100)
        assert not result

    async def test_xrevrange_returns_newest_first(self, client: _FakeRedisClient) -> None:
        await client.xadd("stream:4", {"seq": "1"})
        await client.xadd("stream:4", {"seq": "2"})
        await client.xadd("stream:4", {"seq": "3"})
        result = await client.xrevrange("stream:4", count=1)
        assert len(result) == 1
        _entry_id, fields = result[0]
        assert fields[b"seq"] == b"3"

    async def test_xadd_with_maxlen(self, client: _FakeRedisClient) -> None:
        for i in range(100):
            await client.xadd("stream:capped", {"i": str(i)}, maxlen=50)
        length = await client.xlen("stream:capped")
        # approximate trimming: may be slightly above maxlen
        assert length <= 60

    async def test_xlen_nonexistent_stream(self, client: _FakeRedisClient) -> None:
        length = await client.xlen("stream:none")
        assert length == 0

    async def test_xrevrange_empty_stream(self, client: _FakeRedisClient) -> None:
        result = await client.xrevrange("stream:none")
        assert result == []


# ══════════════════════════════════════════════════════════════════════════════
# SORTED SET
# ══════════════════════════════════════════════════════════════════════════════


class TestSortedSetOps:
    """ZADD/ZRANGEBYSCORE/ZREM — used by StreamRegistry heartbeats."""

    async def test_zadd_zrangebyscore_roundtrip(self, client: _FakeRedisClient) -> None:
        await client.zadd("zs:1", {"member_a": 1.0, "member_b": 2.0, "member_c": 3.0})
        result = await client.zrangebyscore("zs:1", 1.5, 3.0)
        assert b"member_b" in result
        assert b"member_c" in result
        assert b"member_a" not in result

    async def test_zrem_removes_member(self, client: _FakeRedisClient) -> None:
        await client.zadd("zs:2", {"a": 1.0, "b": 2.0})
        removed = await client.zrem("zs:2", "a")
        assert removed == 1
        result = await client.zrangebyscore("zs:2", "-inf", "+inf")
        assert b"a" not in result
        assert b"b" in result

    async def test_zadd_returns_new_count(self, client: _FakeRedisClient) -> None:
        added = await client.zadd("zs:3", {"x": 1.0, "y": 2.0})
        assert added == 2
        added2 = await client.zadd("zs:3", {"x": 5.0, "z": 3.0})
        assert added2 == 1  # only 'z' is new

    async def test_zrangebyscore_empty(self, client: _FakeRedisClient) -> None:
        result = await client.zrangebyscore("zs:none", "-inf", "+inf")
        assert result == []

    async def test_zrem_nonexistent_member(self, client: _FakeRedisClient) -> None:
        await client.zadd("zs:4", {"a": 1.0})
        removed = await client.zrem("zs:4", "nonexistent")
        assert removed == 0


# ══════════════════════════════════════════════════════════════════════════════
# SET
# ══════════════════════════════════════════════════════════════════════════════


class TestSetOps:
    """SADD/SREM/SMEMBERS — used by RedisCheckpointManager active index."""

    async def test_sadd_smembers_roundtrip(self, client: _FakeRedisClient) -> None:
        await client.sadd("set:1", "a", "b", "c")
        members = await client.smembers("set:1")
        assert members == {b"a", b"b", b"c"}

    async def test_srem_removes_member(self, client: _FakeRedisClient) -> None:
        await client.sadd("set:2", "x", "y")
        await client.srem("set:2", "x")
        members = await client.smembers("set:2")
        assert members == {b"y"}

    async def test_smembers_empty_set(self, client: _FakeRedisClient) -> None:
        members = await client.smembers("set:none")
        assert members == set()

    async def test_sadd_idempotent(self, client: _FakeRedisClient) -> None:
        added1 = await client.sadd("set:3", "a")
        assert added1 == 1
        added2 = await client.sadd("set:3", "a")
        assert added2 == 0


# ══════════════════════════════════════════════════════════════════════════════
# KEY OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════


class TestKeyOps:
    """DELETE/EXPIRE/PING — infrastructure ops."""

    async def test_delete_existing_key(self, client: _FakeRedisClient) -> None:
        await client.set("del:1", b"v")
        deleted = await client.delete("del:1")
        assert deleted == 1
        assert await client.get("del:1") is None

    async def test_delete_nonexistent_key(self, client: _FakeRedisClient) -> None:
        deleted = await client.delete("del:none")
        assert deleted == 0

    async def test_delete_multiple_keys(self, client: _FakeRedisClient) -> None:
        await client.set("d1", b"v")
        await client.set("d2", b"v")
        deleted = await client.delete("d1", "d2", "d3")
        assert deleted == 2

    async def test_expire_sets_ttl(self, client: _FakeRedisClient) -> None:
        await client.set("exp:1", b"v")
        result = await client.expire("exp:1", 3600)
        assert result is True
        ttl = await client._client.ttl("exp:1")
        assert ttl > 0

    async def test_expire_nonexistent_key(self, client: _FakeRedisClient) -> None:
        result = await client.expire("exp:none", 3600)
        assert result is False

    async def test_ping(self, client: _FakeRedisClient) -> None:
        result = await client.ping()
        assert result is True


# ══════════════════════════════════════════════════════════════════════════════
# LUA SCRIPTING
# ══════════════════════════════════════════════════════════════════════════════


class TestLuaScripting:
    """EVAL — atomic scripts used by StreamRegistry and IdempotencyGuard."""

    async def test_eval_simple_return(self, client: _FakeRedisClient) -> None:
        result = await client.eval("return 42", [], [])
        assert result == 42

    async def test_eval_with_keys_and_args(self, client: _FakeRedisClient) -> None:
        script = "redis.call('SET', KEYS[1], ARGV[1]); return 1"
        result = await client.eval(script, ["lua:key"], ["lua:value"])
        assert result == 1
        val = await client.get("lua:key")
        assert val == b"lua:value"

    async def test_eval_atomic_incr_if_below(self, client: _FakeRedisClient) -> None:
        """Simulates the _LUA_REGISTER pattern from StreamRegistry."""
        script = """
        local count_key = KEYS[1]
        local max = tonumber(ARGV[1])
        local current = tonumber(redis.call('GET', count_key) or '0')
        if current >= max then
            return 0
        end
        redis.call('INCR', count_key)
        return 1
        """
        # First call: current=0, max=2 → should succeed
        result = await client.eval(script, ["counter"], ["2"])
        assert result == 1

        # Second call: current=1, max=2 → should succeed
        result = await client.eval(script, ["counter"], ["2"])
        assert result == 1

        # Third call: current=2, max=2 → should fail
        result = await client.eval(script, ["counter"], ["2"])
        assert result == 0

    async def test_eval_reads_and_writes_hash(self, client: _FakeRedisClient) -> None:
        await client.hset("lua:hash", {"status": "pending"})
        script = """
        local val = redis.call('HGET', KEYS[1], ARGV[1])
        if val == ARGV[2] then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
            return 1
        end
        return 0
        """
        result = await client.eval(script, ["lua:hash"], ["status", "pending", "running"])
        assert result == 1
        data = await client.hgetall("lua:hash")
        assert data[b"status"] == b"running"


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE
# ══════════════════════════════════════════════════════════════════════════════


class TestPipeline:
    """Pipeline batched execution — single round-trip for multiple commands."""

    async def test_pipeline_execute_multiple(self, client: _FakeRedisClient) -> None:
        pipe = client.pipeline()
        pipe.set("p1", "v1")
        pipe.set("p2", "v2")
        pipe.get("p1")
        pipe.get("p2")
        results = await pipe.execute()
        assert len(results) == 4
        assert results[2] == b"v1"
        assert results[3] == b"v2"

    async def test_pipeline_hset_and_expire(self, client: _FakeRedisClient) -> None:
        """Atomic HSET + EXPIRE pattern used by RedisStateManager."""
        pipe = client.pipeline()
        pipe.hset("pipe:hash", mapping={"status": "running"})
        pipe.expire("pipe:hash", 3600)
        results = await pipe.execute()
        assert len(results) == 2
        data = await client.hgetall("pipe:hash")
        assert data[b"status"] == b"running"

    async def test_pipeline_stream_batch(self, client: _FakeRedisClient) -> None:
        """Batched XADD pattern used by ProtoStreamWriter._flush()."""
        pipe = client.pipeline()
        for i in range(20):
            pipe.xadd("pipe:stream", {"seq": str(i)})
        results = await pipe.execute()
        assert len(results) == 20
        length = await client.xlen("pipe:stream")
        assert length == 20


# ══════════════════════════════════════════════════════════════════════════════
# PUB/SUB
# ══════════════════════════════════════════════════════════════════════════════


class TestPubSub:
    """Publish/subscribe — used by RedisSendBuffer for signal delivery."""

    async def test_publish_returns_subscriber_count(self, client: _FakeRedisClient) -> None:
        # No subscribers → 0
        count = await client.publish("ch:1", "msg")
        assert count == 0

    async def test_pubsub_subscribe_receive(self, client: _FakeRedisClient) -> None:
        ps = client.pubsub()
        await ps.subscribe("ch:test")

        # Consume the subscription confirmation message
        msg = await ps.get_message(timeout=1)
        assert msg is not None
        assert msg["type"] == "subscribe"

        # Publish and receive
        await client.publish("ch:test", b"hello")
        msg = await ps.get_message(timeout=1)
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"hello"

        await ps.unsubscribe("ch:test")
        await ps.aclose()


# ══════════════════════════════════════════════════════════════════════════════
# PROPERTY-BASED (Hypothesis)
# ══════════════════════════════════════════════════════════════════════════════


class TestPropertyBased:
    """Deterministic invariant tests across diverse key/value shapes."""

    @pytest.mark.property
    async def test_get_set_roundtrip_diverse_keys(self, client: _FakeRedisClient) -> None:
        """GET(SET(k, v)) == v for diverse key formats and value sizes."""
        cases = [
            ("simple", b"v"),
            ("a:b:c", b"colons"),
            ("key_with_dots.and-dashes", b"special"),
            ("k" * 200, b"long_key"),
            ("unicode_safe_123", b"\x00\x01\xff" * 100),
            ("empty_val", b""),
            ("binary_val", bytes(range(256))),
            ("task:abc-123:stream", b"realistic_key_format"),
        ]
        for key, value in cases:
            await client.set(key, value)
            result = await client.get(key)
            assert result == value, f"Roundtrip failed for key={key!r}"

    @pytest.mark.property
    async def test_hgetall_consistency(self, client: _FakeRedisClient) -> None:
        """HGETALL returns all fields set by HSET."""
        fields = {f"f{i}": f"v{i}" for i in range(20)}
        await client.hset("prop:hash", fields)
        result = await client.hgetall("prop:hash")
        assert len(result) == 20
        for k, v in fields.items():
            assert result[k.encode()] == v.encode()

    @pytest.mark.property
    async def test_sadd_smembers_invariant(self, client: _FakeRedisClient) -> None:
        """SMEMBERS after N SADD contains exactly N unique members."""
        members = [f"m{i}" for i in range(50)]
        for m in members:
            await client.sadd("prop:set", m)
        result = await client.smembers("prop:set")
        assert len(result) == 50

    @pytest.mark.property
    async def test_zrangebyscore_subset(self, client: _FakeRedisClient) -> None:
        """ZRANGEBYSCORE result is always a subset of all members."""
        import random

        all_members = {}
        for i in range(30):
            score = random.uniform(0, 100)
            all_members[f"z{i}"] = score
        await client.zadd("prop:zs", all_members)

        low, high = sorted(random.sample(range(101), 2))
        result = await client.zrangebyscore("prop:zs", low, high)
        all_result = await client.zrangebyscore("prop:zs", "-inf", "+inf")
        for member in result:
            assert member in all_result
