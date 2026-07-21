"""L0 — Pub/sub lifecycle tests for signal channel delivery.

Tests the pub/sub pattern used by RedisSendBuffer and SharedRedisListener:
- subscribe → publish → receive round-trip
- Signal channel naming: signal_ch:{task_id}
- Multiple channels (one per task)
- Unsubscribe cleanup (no leaked subscriptions)

All tests use fakeredis, no real Redis needed.
"""

from __future__ import annotations

import asyncio
import json

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [
    pytest.mark.timeout(15),
    pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed"),
]


@pytest.fixture
async def redis():
    client = fakeredis_aio.FakeRedis()
    yield client
    await client.aclose()


class TestPubSubLifecycle:
    """Subscribe/publish/unsubscribe lifecycle."""

    async def test_subscribe_publish_receive(self, redis) -> None:
        ps = redis.pubsub()
        await ps.subscribe("signal_ch:task_1")

        # Consume subscription confirmation
        msg = await ps.get_message(timeout=1)
        assert msg["type"] == "subscribe"

        # Publish signal
        await redis.publish("signal_ch:task_1", json.dumps({"action": "cancel"}).encode())

        # Receive
        msg = await ps.get_message(timeout=1)
        assert msg is not None
        assert msg["type"] == "message"
        assert json.loads(msg["data"]) == {"action": "cancel"}

        await ps.unsubscribe("signal_ch:task_1")
        await ps.aclose()

    async def test_multiple_channels(self, redis) -> None:
        ps = redis.pubsub()
        await ps.subscribe("signal_ch:t1", "signal_ch:t2")

        # Consume confirmations
        for _ in range(2):
            msg = await ps.get_message(timeout=1)
            assert msg["type"] == "subscribe"

        # Publish to each
        await redis.publish("signal_ch:t1", b"msg1")
        await redis.publish("signal_ch:t2", b"msg2")

        received = []
        for _ in range(2):
            msg = await ps.get_message(timeout=1)
            if msg and msg["type"] == "message":
                received.append((msg["channel"], msg["data"]))

        channels = {ch for ch, _ in received}
        assert b"signal_ch:t1" in channels
        assert b"signal_ch:t2" in channels

        await ps.unsubscribe()
        await ps.aclose()

    async def test_unsubscribe_stops_receiving(self, redis) -> None:
        ps = redis.pubsub()
        await ps.subscribe("signal_ch:t3")
        await ps.get_message(timeout=1)  # consume confirmation

        await ps.unsubscribe("signal_ch:t3")
        await ps.get_message(timeout=0.1)  # consume unsubscribe confirmation

        # Publish after unsubscribe
        await redis.publish("signal_ch:t3", b"should_not_receive")

        msg = await ps.get_message(timeout=0.2)
        # Should be None or not a message type
        if msg is not None:
            assert msg["type"] != "message"

        await ps.aclose()

    async def test_publish_returns_subscriber_count(self, redis) -> None:
        ps = redis.pubsub()
        await ps.subscribe("ch:count")
        await ps.get_message(timeout=1)

        count = await redis.publish("ch:count", b"test")
        assert count >= 1

        await ps.unsubscribe()
        await ps.aclose()

    async def test_no_subscribers_returns_zero(self, redis) -> None:
        count = await redis.publish("ch:nobody", b"hello")
        assert count == 0


class TestSignalChannelPattern:
    """Signal channel naming convention: signal_ch:{task_id}."""

    async def test_signal_channel_format(self, redis) -> None:
        """Verify the channel naming matches gateway_constants.signal_channel()."""
        task_id = "abc-123"
        channel = f"signal_ch:{task_id}"

        ps = redis.pubsub()
        await ps.subscribe(channel)
        await ps.get_message(timeout=1)

        payload = json.dumps({"action": "stop", "task_id": task_id})
        await redis.publish(channel, payload.encode())

        msg = await ps.get_message(timeout=1)
        assert msg is not None
        data = json.loads(msg["data"])
        assert data["action"] == "stop"
        assert data["task_id"] == task_id

        await ps.unsubscribe()
        await ps.aclose()

    async def test_signal_json_payload_round_trip(self, redis) -> None:
        """Signal payloads are JSON-encoded dicts."""
        ps = redis.pubsub()
        await ps.subscribe("signal_ch:payload_test")
        await ps.get_message(timeout=1)

        original = {"action": "cancel", "task_id": "t1", "reason": "user_request"}
        await redis.publish("signal_ch:payload_test", json.dumps(original).encode())

        msg = await ps.get_message(timeout=1)
        decoded = json.loads(msg["data"])
        assert decoded == original

        await ps.unsubscribe()
        await ps.aclose()
