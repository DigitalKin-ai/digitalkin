"""Tests for SharedRedisListener and RedisSendBuffer.

Covers dispatch, deduplication, priority eviction, sentinel handling,
batching, flush triggers, ref-counting, and cleanup.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

pytestmark = pytest.mark.timeout(10)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakePubSub:
    """In-memory pub/sub for unit tests."""

    def __init__(self) -> None:
        self._subscribed: list[str] = []
        self._messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._closed = False

    async def subscribe(self, *channels: str) -> None:
        self._subscribed.extend(channels)

    async def unsubscribe(self, *_channels: str) -> None:
        self._subscribed.clear()

    async def aclose(self) -> None:
        self._closed = True

    async def get_message(self, ignore_subscribe_messages: bool = True, timeout: float = 0.5) -> dict[str, Any] | None:
        _ = ignore_subscribe_messages, timeout
        try:
            return self._messages.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.01)
            return None

    def inject(self, channel: str, data: str) -> None:
        self._messages.put_nowait({"type": "message", "channel": channel.encode(), "data": data.encode()})


class _FakePipeline:
    """In-memory pipeline for unit tests."""

    def __init__(self) -> None:
        self._commands: list[tuple[str, ...]] = []

    def hset(self, name: str, mapping: dict[str, str]) -> Any:
        self._commands.append(("hset", name, str(mapping)))
        return self

    def expire(self, name: str, seconds: int) -> Any:
        self._commands.append(("expire", name, str(seconds)))
        return self

    def publish(self, channel: str, message: str) -> Any:
        self._commands.append(("publish", channel, message))
        return self

    async def execute(self) -> list[bool]:
        return [True] * len(self._commands)


def _make_mock_client() -> MagicMock:
    mock = MagicMock()
    mock.pubsub.return_value = _FakePubSub()
    mock.pipeline.return_value = _FakePipeline()
    mock.hgetall = AsyncMock(return_value={})
    return mock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_instances() -> Generator[None]:
    from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer, SharedRedisListener

    SharedRedisListener._instances.clear()
    RedisSendBuffer._instances.clear()
    yield
    SharedRedisListener._instances.clear()
    RedisSendBuffer._instances.clear()


# ===========================================================================
# SharedRedisListener
# ===========================================================================


class TestSharedRedisListenerDispatch:
    """Signal dispatch, dedup, and priority."""

    async def test_dispatch_to_registered_task(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        q = await listener.register("task_1")

        data = {"action": "start", "task_id": "task_1"}
        assert listener.dispatch_signal("task_1", data, json.dumps(data)) is True
        assert not q.empty()

    async def test_dispatch_to_unknown_task_returns_false(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        data = {"action": "start", "task_id": "unknown"}
        assert listener.dispatch_signal("unknown", data, json.dumps(data)) is False

    async def test_dedup_skips_identical_json(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        q = await listener.register("task_1")

        data = {"action": "start"}
        raw = json.dumps(data)
        assert listener.dispatch_signal("task_1", data, raw) is True
        assert listener.dispatch_signal("task_1", data, raw) is False
        assert q.qsize() == 1

    async def test_different_payloads_not_deduped(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        q = await listener.register("task_1")

        d1 = {"action": "start"}
        d2 = {"action": "ack_start"}
        listener.dispatch_signal("task_1", d1, json.dumps(d1))
        listener.dispatch_signal("task_1", d2, json.dumps(d2))
        assert q.qsize() == 2

    async def test_priority_evicts_oldest_on_full(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        listener._queue_size = 1
        q = await listener.register("task_1")

        filler = {"action": "start"}
        listener.dispatch_signal("task_1", filler, json.dumps(filler))
        assert q.full()

        cancel = {"action": "cancel"}
        assert listener.dispatch_signal("task_1", cancel, json.dumps(cancel)) is True

    async def test_stop_sends_sentinel_and_unregisters(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        q = await listener.register("task_1")

        stop = {"action": "stop"}
        listener.dispatch_signal("task_1", stop, json.dumps(stop))

        items = []
        while not q.empty():
            items.append(q.get_nowait())
        assert items[-1] is None
        assert "task_1" not in listener._task_queues

    async def test_dispatches_to_correct_task(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        q1 = await listener.register("task_1")
        q2 = await listener.register("task_2")

        data = {"action": "start"}
        listener.dispatch_signal("task_1", data, json.dumps(data))

        assert not q1.empty()
        assert q2.empty()


class TestSharedRedisListenerLifecycle:
    """Ref-counting and cleanup."""

    async def test_get_or_create_reuses_instance(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        client = _make_mock_client()
        a = SharedRedisListener.get_or_create("url_1", client)
        b = SharedRedisListener.get_or_create("url_1", client)
        assert a is b
        assert a._refcount == 2

    async def test_release_closes_on_last_ref(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        client = _make_mock_client()
        SharedRedisListener.get_or_create("url_2", client)
        await SharedRedisListener.release("url_2")
        assert "url_2" not in SharedRedisListener._instances

    async def test_wake_sends_sentinel(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        q = await listener.register("task_w")
        listener.wake("task_w")
        assert q.get_nowait() is None

    async def test_unregister_removes_task(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        await listener.register("task_u")
        listener.unregister("task_u")
        assert "task_u" not in listener._task_queues


# ===========================================================================
# RedisSendBuffer
# ===========================================================================


class TestRedisSendBufferBatching:
    """Batch flush on size and pipeline execution."""

    async def test_flush_on_batch_size(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer

        client = _make_mock_client()
        buf = RedisSendBuffer(client, signal_ttl=3600)
        buf._max_batch_size = 3

        results = await asyncio.gather(
            buf.send("t1", '{"a":1}'),
            buf.send("t2", '{"a":2}'),
            buf.send("t3", '{"a":3}'),
        )
        assert all(results)
        assert len(buf._pending) == 0

    async def test_pipeline_packs_three_commands_per_signal(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer

        client = _make_mock_client()
        fake_pipe = _FakePipeline()
        client.pipeline.return_value = fake_pipe

        buf = RedisSendBuffer(client, signal_ttl=3600)
        buf._max_batch_size = 2

        await asyncio.gather(
            buf.send("t1", '{"a":1}'),
            buf.send("t2", '{"a":2}'),
        )

        # 2 signals x 3 commands (hset, expire, publish) = 6
        assert len(fake_pipe._commands) == 6

    async def test_flush_resolves_futures_on_failure(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer

        client = _make_mock_client()
        failing_pipe = MagicMock()
        failing_pipe.hset.return_value = failing_pipe
        failing_pipe.expire.return_value = failing_pipe
        failing_pipe.publish.return_value = failing_pipe
        failing_pipe.execute = AsyncMock(side_effect=ConnectionError("Redis down"))
        client.pipeline.return_value = failing_pipe

        buf = RedisSendBuffer(client, signal_ttl=3600)
        buf._max_batch_size = 1

        with pytest.raises(ConnectionError, match="Redis down"):
            await buf.send("t1", '{"a":1}')


class TestRedisSendBufferLifecycle:
    """Ref-counting and close."""

    async def test_get_or_create_reuses_instance(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer

        client = _make_mock_client()
        a = RedisSendBuffer.get_or_create("url_1", client, 3600)
        b = RedisSendBuffer.get_or_create("url_1", client, 3600)
        assert a is b
        assert a._refcount == 2

    async def test_close_flushes_pending(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer

        client = _make_mock_client()
        buf = RedisSendBuffer(client, signal_ttl=3600)
        buf._max_batch_size = 100  # Won't auto-flush

        # Send without hitting batch size
        future = asyncio.get_running_loop().create_future()
        buf._pending.append(("t1", '{"a":1}', future))

        await buf.close()
        assert future.result() is True
