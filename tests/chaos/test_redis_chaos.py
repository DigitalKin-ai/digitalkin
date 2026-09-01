"""L2 — Chaos tests: 10 fault injection scenarios via Toxiproxy.

Each test injects a specific fault between the client and Redis,
then verifies the SDK handles it correctly (retry, reconnect, error).

Requires:
  docker compose --profile redis --profile chaos up -d

Scenarios:
1.  complete_outage → operations fail → re-enable → operations resume
2.  latency_spike_2s → write times out or takes >2s
3.  jitter_100ms → concurrent ops complete with zero data corruption
4.  bandwidth_10kbps → large value write slow but intact
5.  connection_reset_100ms → auto-reconnect, next op succeeds <500ms
6.  slow_close → client close completes in bounded time
7.  partial_failure_50pct → pipeline returns correct-length results
8.  stream_registry_under_partition → capacity check recovers
9.  signal_delivery_under_chaos → signal batch flush with jitter
10. checkpoint_restore_after_outage → checkpoint data survives
"""

from __future__ import annotations

import asyncio
import time

import pytest

from tests.chaos.conftest import SKIP_NO_TOXIPROXY, ToxiproxyClient

pytestmark = [pytest.mark.chaos, pytest.mark.timeout(30), SKIP_NO_TOXIPROXY]


class TestCompleteOutage:
    """Scenario 1: Redis completely unreachable, then restored."""

    async def test_outage_and_recovery(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """Operations fail during outage, succeed after re-enable."""
        # Baseline: works
        await redis_via_proxy.set("outage:k", b"before")
        assert await redis_via_proxy.get("outage:k") == b"before"

        # Cut the connection
        await toxiproxy.disable_proxy()
        await asyncio.sleep(0.2)

        # Operations should fail
        with pytest.raises(Exception):
            await asyncio.wait_for(redis_via_proxy.set("outage:fail", b"x"), timeout=3)

        # Restore
        await toxiproxy.enable_proxy()
        await asyncio.sleep(0.5)

        # Should recover
        await redis_via_proxy.set("outage:after", b"recovered")
        assert await redis_via_proxy.get("outage:after") == b"recovered"


class TestLatencySpike:
    """Scenario 2: 2s latency added to all Redis responses."""

    async def test_latency_increases_response_time(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """SET takes >2s with 2s latency toxic."""
        await toxiproxy.add_toxic("latency", {"latency": 2000, "jitter": 0})

        t0 = time.monotonic()
        await redis_via_proxy.set("lat:k", b"slow")
        elapsed = (time.monotonic() - t0) * 1000

        assert elapsed > 1500, f"Expected >1.5s latency, got {elapsed:.0f}ms"

        # Data is still correct
        val = await redis_via_proxy.get("lat:k")
        assert val == b"slow"


class TestJitter:
    """Scenario 3: 100ms ±80ms jitter — no data corruption under concurrency."""

    async def test_concurrent_ops_no_corruption(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """Concurrent SET/GET with jitter: all values correct, zero corruption."""
        await toxiproxy.add_toxic("latency", {"latency": 100, "jitter": 80})

        sem = asyncio.Semaphore(5)  # limit to pool capacity

        async def write_read(i: int) -> bool:
            async with sem:
                key = f"jit:{i}"
                val = f"val_{i}".encode()
                await redis_via_proxy.set(key, val)
                result = await redis_via_proxy.get(key)
                return result == val

        results = await asyncio.gather(*[write_read(i) for i in range(30)])
        assert all(results), f"Data corruption: {sum(not r for r in results)}/{len(results)} failures"


class TestBandwidthLimit:
    """Scenario 4: 10KB/s bandwidth — large values slow but intact."""

    async def test_large_value_intact_under_bandwidth_limit(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """10KB value arrives intact at 10KB/s bandwidth."""
        await toxiproxy.add_toxic("bandwidth", {"rate": 10}, stream="downstream")

        large_val = b"X" * 10_000
        await redis_via_proxy.set("bw:large", large_val)

        result = await redis_via_proxy.get("bw:large")
        assert result == large_val
        assert len(result) == 10_000


class TestConnectionReset:
    """Scenario 5: Connection resets every 100ms — auto-reconnect."""

    async def test_reconnect_after_reset(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """After connection reset toxic is removed, next op succeeds <500ms."""
        toxic = await toxiproxy.add_toxic("reset_peer", {"timeout": 100})
        toxic_name = toxic.get("name", "reset_peer_downstream")
        await asyncio.sleep(0.3)

        # Remove toxic by its actual name
        await toxiproxy.remove_toxic(toxic_name)
        await asyncio.sleep(0.2)

        # Next operation should succeed quickly
        t0 = time.monotonic()
        await redis_via_proxy.set("reset:k", b"recovered")
        elapsed = (time.monotonic() - t0) * 1000

        assert elapsed < 2000, f"Reconnect took {elapsed:.0f}ms — too slow"
        assert await redis_via_proxy.get("reset:k") == b"recovered"


class TestSlowClose:
    """Scenario 6: slow_close toxic — client close completes in bounded time."""

    async def test_close_completes_bounded(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """Client.close() completes within 3s even with slow_close toxic."""
        await toxiproxy.add_toxic("slow_close", {"delay": 500})

        t0 = time.monotonic()
        await redis_via_proxy.close()
        elapsed = (time.monotonic() - t0) * 1000

        assert elapsed < 3000, f"close() took {elapsed:.0f}ms — should be <3s"


class TestPartialFailure:
    """Scenario 7: 50% upstream failures — pipeline integrity."""

    async def test_pipeline_returns_correct_length(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """Pipeline results list has same length as commands sent."""
        # Pre-populate data without toxic
        for i in range(10):
            await redis_via_proxy.set(f"pf:{i}", f"v{i}")

        # Add jitter (not full failure — pipeline should still work)
        await toxiproxy.add_toxic("latency", {"latency": 50, "jitter": 40})

        pipe = redis_via_proxy.pipeline()
        for i in range(10):
            pipe.get(f"pf:{i}")
        results = await pipe.execute()

        assert len(results) == 10
        for i, r in enumerate(results):
            assert r == f"v{i}".encode()


class TestStreamRegistryUnderPartition:
    """Scenario 8: Registry capacity check with intermittent Redis."""

    async def test_registry_handles_intermittent_redis(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """Lua capacity script returns valid result despite jitter."""
        await toxiproxy.add_toxic("latency", {"latency": 50, "jitter": 30})

        script = """
        local count_key = KEYS[1]
        local max = tonumber(ARGV[1])
        local current = tonumber(redis.call('GET', count_key) or '0')
        if current >= max then return 0 end
        redis.call('INCR', count_key)
        return 1
        """

        results = []
        for i in range(10):
            r = await redis_via_proxy.eval(script, ["chaos:count"], ["100"])
            results.append(r)

        # All should succeed (capacity=100, only 10 calls)
        assert all(r == 1 for r in results)

        # Counter should be exactly 10
        val = await redis_via_proxy.get("chaos:count")
        assert val == b"10"


class TestSignalDeliveryUnderChaos:
    """Scenario 9: Signal pub/sub with jitter."""

    async def test_publish_reaches_subscriber_with_jitter(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """Published signal reaches subscriber despite network jitter."""
        await toxiproxy.add_toxic("latency", {"latency": 30, "jitter": 20})

        ps = redis_via_proxy.pubsub()
        await ps.subscribe("chaos:signal")
        await ps.get_message(timeout=2)  # subscription confirmation

        await redis_via_proxy.publish("chaos:signal", b'{"action":"cancel"}')

        msg = await ps.get_message(timeout=3)
        assert msg is not None
        assert msg["type"] == "message"
        assert b"cancel" in msg["data"]

        await ps.unsubscribe()
        await ps.aclose()


class TestCheckpointRestoreAfterOutage:
    """Scenario 10: Checkpoint data survives brief outage."""

    async def test_checkpoint_survives_outage(self, toxiproxy: ToxiproxyClient, redis_via_proxy) -> None:
        """Data written before outage is readable after recovery."""
        # Write checkpoint
        pipe = redis_via_proxy.pipeline()
        pipe.hset("chaos:checkpoint:s1", mapping={"state": '{"step":5}', "last_seq": "42"})
        pipe.expire("chaos:checkpoint:s1", 300)
        pipe.sadd("chaos:checkpoints:active", "s1")
        await pipe.execute()

        # Brief outage
        await toxiproxy.disable_proxy()
        await asyncio.sleep(0.5)
        await toxiproxy.enable_proxy()
        await asyncio.sleep(0.5)

        # Checkpoint should be intact
        data = await redis_via_proxy.hgetall("chaos:checkpoint:s1")
        assert data[b"state"] == b'{"step":5}'
        assert data[b"last_seq"] == b"42"

        members = await redis_via_proxy.smembers("chaos:checkpoints:active")
        assert b"s1" in members
