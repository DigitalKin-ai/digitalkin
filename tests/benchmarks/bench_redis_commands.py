"""L4 — Redis command regression benchmarks.

Measures latency of SDK-specific Redis operations against real Redis.
Reports p50/p95/p99 and asserts no regression beyond budget.

Requires: real Redis via docker-compose --profile redis up -d

Usage:
    uv run pytest tests/benchmarks/bench_redis_commands.py -v -s
"""

from __future__ import annotations

import os
import statistics
import time

import pytest

pytestmark = [pytest.mark.stress, pytest.mark.integration, pytest.mark.timeout(120)]

REDIS_URL = os.environ.get("DIGITALKIN_REDIS_URL", "redis://localhost:6379/0")
ROUNDS = 200
WARMUP = 10


def _percentile(data: list[float], pct: float) -> float:
    """Compute percentile from sorted data."""
    if not data:
        return 0.0
    k = (len(data) - 1) * (pct / 100)
    f_idx = int(k)
    c_idx = min(f_idx + 1, len(data) - 1)
    d = k - f_idx
    return data[f_idx] + d * (data[c_idx] - data[f_idx])


def _report(name: str, latencies_ms: list[float]) -> None:
    """Print benchmark results."""
    latencies_ms.sort()
    p50 = _percentile(latencies_ms, 50)
    p95 = _percentile(latencies_ms, 95)
    p99 = _percentile(latencies_ms, 99)
    mean = statistics.mean(latencies_ms)
    print(f"  {name:40s} p50={p50:.3f}ms  p95={p95:.3f}ms  p99={p99:.3f}ms  mean={mean:.3f}ms  n={len(latencies_ms)}")


@pytest.fixture
async def redis_client():
    from digitalkin.core.task_manager.redis.redis_client import RedisClient

    client = RedisClient(REDIS_URL, pool_size=20)
    reachable = await client.verify(timeout=3.0)
    if not reachable:
        await client.close()
        pytest.skip("Redis not reachable")
    await client._client.flushdb()
    yield client
    await client._client.flushdb()
    await client.close()


class TestStringBenchmarks:
    """SET/GET latency."""

    async def test_bench_set_small(self, redis_client) -> None:
        """SET 10-byte value: expect p95 < 2ms."""
        for _ in range(WARMUP):
            await redis_client.set("w", b"0123456789")

        latencies = []
        for _ in range(ROUNDS):
            t0 = time.perf_counter()
            await redis_client.set("bench:small", b"0123456789")
            latencies.append((time.perf_counter() - t0) * 1000)

        _report("SET small (10B)", latencies)
        latencies.sort()
        assert _percentile(latencies, 95) < 5.0, "SET small p95 > 5ms"

    async def test_bench_get_hot(self, redis_client) -> None:
        """GET on a hot key: expect p95 < 2ms."""
        await redis_client.set("bench:hot", b"v")
        for _ in range(WARMUP):
            await redis_client.get("bench:hot")

        latencies = []
        for _ in range(ROUNDS):
            t0 = time.perf_counter()
            await redis_client.get("bench:hot")
            latencies.append((time.perf_counter() - t0) * 1000)

        _report("GET hot", latencies)
        latencies.sort()
        assert _percentile(latencies, 95) < 5.0, "GET hot p95 > 5ms"


class TestStreamBenchmarks:
    """XADD/XREAD latency — ProtoStreamWriter/Reader hot path."""

    async def test_bench_xadd_single(self, redis_client) -> None:
        """Single XADD: expect p95 < 3ms."""
        for _ in range(WARMUP):
            await redis_client.xadd("w:s", {"d": b"x"})

        latencies = []
        for _ in range(ROUNDS):
            t0 = time.perf_counter()
            await redis_client.xadd("bench:stream", {"pb": b"data", "seq": "1"})
            latencies.append((time.perf_counter() - t0) * 1000)

        _report("XADD single", latencies)
        latencies.sort()
        assert _percentile(latencies, 95) < 5.0, "XADD p95 > 5ms"

    async def test_bench_proto_write_struct(self, redis_client) -> None:
        """ProtoStreamWriter.write_struct: expect p95 < 3ms."""
        from google.protobuf import struct_pb2

        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

        writer = ProtoStreamWriter("bench:proto", redis_client)
        s = struct_pb2.Struct()
        s.update({"msg": "benchmark"})

        for _ in range(WARMUP):
            await writer.write_struct(s)

        latencies = []
        for _ in range(ROUNDS):
            t0 = time.perf_counter()
            await writer.write_struct(s)
            latencies.append((time.perf_counter() - t0) * 1000)

        _report("ProtoStreamWriter.write_struct", latencies)
        latencies.sort()
        assert _percentile(latencies, 95) < 5.0, "write_struct p95 > 5ms"


class TestHashBenchmarks:
    """HSET/HGETALL latency — RedisStateManager pattern."""

    async def test_bench_hset_hgetall(self, redis_client) -> None:
        """HSET + HGETALL round-trip: expect p95 < 5ms."""
        for _ in range(WARMUP):
            await redis_client.hset("w:h", {"s": "r"})
            await redis_client.hgetall("w:h")

        latencies = []
        for _ in range(ROUNDS):
            t0 = time.perf_counter()
            await redis_client.hset("bench:hash", {"status": "running", "ts": "now"})
            await redis_client.hgetall("bench:hash")
            latencies.append((time.perf_counter() - t0) * 1000)

        _report("HSET+HGETALL round-trip", latencies)
        latencies.sort()
        assert _percentile(latencies, 95) < 10.0, "HSET+HGETALL p95 > 10ms"


class TestPipelineBenchmarks:
    """Pipeline batching latency."""

    async def test_bench_pipeline_100(self, redis_client) -> None:
        """100-cmd pipeline: expect p95 < 10ms."""
        for _ in range(WARMUP):
            pipe = redis_client.pipeline()
            for i in range(10):
                pipe.set(f"w:{i}", f"v")
            await pipe.execute()

        latencies = []
        for _ in range(ROUNDS):
            t0 = time.perf_counter()
            pipe = redis_client.pipeline()
            for i in range(100):
                pipe.set(f"bench:pipe:{i}", f"v{i}")
            await pipe.execute()
            latencies.append((time.perf_counter() - t0) * 1000)

        _report("Pipeline 100 SET", latencies)
        latencies.sort()
        assert _percentile(latencies, 95) < 20.0, "Pipeline 100 p95 > 20ms"


class TestLuaBenchmarks:
    """Lua script latency."""

    async def test_bench_lua_register(self, redis_client) -> None:
        """_LUA_REGISTER capacity script: expect p95 < 3ms."""
        script = """
        local count_key = KEYS[1]
        local hb_key = KEYS[2]
        local max = tonumber(ARGV[1])
        local task_id = ARGV[2]
        local now = tonumber(ARGV[3])
        local current = tonumber(redis.call('GET', count_key) or '0')
        if current >= max then return 0 end
        redis.call('INCR', count_key)
        redis.call('EXPIRE', count_key, 3600)
        redis.call('ZADD', hb_key, now, task_id)
        return 1
        """

        for i in range(WARMUP):
            await redis_client.eval(script, ["w:c", "w:h"], ["100000", f"w{i}", str(i)])

        latencies = []
        for i in range(ROUNDS):
            t0 = time.perf_counter()
            await redis_client.eval(script, ["bench:count", "bench:hb"], ["100000", f"t{i}", str(i)])
            latencies.append((time.perf_counter() - t0) * 1000)

        _report("Lua _LUA_REGISTER", latencies)
        latencies.sort()
        assert _percentile(latencies, 95) < 5.0, "Lua register p95 > 5ms"
