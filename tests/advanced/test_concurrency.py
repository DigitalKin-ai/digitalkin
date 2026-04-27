"""Concurrency and race condition tests.

Simulates multi-task concurrent access to shared resources:
- CircuitBreaker state transitions under concurrent load
- SharedRedisListener concurrent register/dispatch/unregister
- RedisSendBuffer concurrent sends with batch flush
- StreamRegistry concurrent register/unregister
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock

import pytest

pytestmark = [pytest.mark.concurrency, pytest.mark.timeout(30)]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client() -> MagicMock:
    """Mock RedisClient with in-memory pipeline."""
    mock = MagicMock()
    mock.pubsub.return_value = MagicMock()

    class FakePipe:
        def __init__(self) -> None:
            self._n = 0

        def hset(self, *_a: object, **_kw: object) -> FakePipe:
            self._n += 1
            return self

        def expire(self, *_a: object) -> FakePipe:
            self._n += 1
            return self

        def publish(self, *_a: object) -> FakePipe:
            self._n += 1
            return self

        async def execute(self) -> list[bool]:
            return [True] * self._n

    mock.pipeline.return_value = FakePipe()
    return mock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_singletons() -> Generator[None]:
    """Reset all singletons between tests."""
    from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer, SharedRedisListener
    from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()
    RedisSendBuffer._instances.clear()
    yield
    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()
    RedisSendBuffer._instances.clear()


# ===========================================================================
# CircuitBreaker concurrency
# ===========================================================================


class TestCircuitBreakerConcurrency:
    """Concurrent state transitions don't corrupt the state machine."""

    async def test_concurrent_failures_open_exactly_once(self) -> None:
        """50 concurrent failures on a CB with fail_max=5 opens it, doesn't crash."""
        from digitalkin.grpc_servers.utils.circuit_breaker import CBState, CircuitBreaker

        cb = CircuitBreaker("conc_svc", fail_max=5, reset_timeout=30.0)

        async def fail() -> None:
            cb.record_failure()

        await asyncio.gather(*[fail() for _ in range(50)])
        assert cb.state == CBState.OPEN

    async def test_concurrent_success_and_failure(self) -> None:
        """Mixed concurrent success/failure doesn't corrupt state."""
        from digitalkin.grpc_servers.utils.circuit_breaker import CBState, CircuitBreaker

        cb = CircuitBreaker("mixed_svc", fail_max=10, reset_timeout=30.0)

        async def mixed(i: int) -> None:
            if i % 2 == 0:
                cb.record_failure()
            else:
                cb.record_success()

        await asyncio.gather(*[mixed(i) for i in range(100)])
        # State is valid (CLOSED or OPEN, never corrupted)
        assert cb.state in {CBState.CLOSED, CBState.OPEN}


# ===========================================================================
# SharedRedisListener concurrency
# ===========================================================================


class TestListenerConcurrency:
    """Concurrent register/dispatch/unregister is safe."""

    async def test_concurrent_register_and_dispatch(self) -> None:
        """Register 20 tasks and dispatch to each concurrently."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        queues: dict[str, asyncio.Queue] = {}

        for i in range(20):
            tid = f"task_{i}"
            queues[tid] = listener.register(tid)

        async def dispatch_to(tid: str) -> None:
            data = {"action": "start", "tid": tid}
            listener.dispatch_signal(tid, data, json.dumps(data))

        await asyncio.gather(*[dispatch_to(f"task_{i}") for i in range(20)])

        for tid, q in queues.items():
            assert not q.empty(), f"Queue for {tid} should have 1 item"

    async def test_concurrent_register_unregister(self) -> None:
        """Rapid register/unregister cycle doesn't corrupt internal state."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())

        async def cycle(i: int) -> None:
            tid = f"cycle_{i}"
            listener.register(tid)
            await asyncio.sleep(0)  # Yield to other tasks
            listener.unregister(tid)

        await asyncio.gather(*[cycle(i) for i in range(50)])
        assert len(listener._task_queues) == 0


# ===========================================================================
# RedisSendBuffer concurrency
# ===========================================================================


class TestSendBufferConcurrency:
    """Concurrent sends resolve correctly without data loss."""

    async def test_100_concurrent_sends_all_resolve(self) -> None:
        """100 concurrent sends all get resolved futures."""
        from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer

        buf = RedisSendBuffer(_make_mock_client(), signal_ttl=3600)
        buf._max_batch_size = 10  # Trigger flushes frequently

        results = await asyncio.gather(
            *[buf.send(f"task_{i}", json.dumps({"i": i})) for i in range(100)]
        )

        assert all(results)
        assert len(buf._pending) == 0  # All flushed


# ===========================================================================
# StreamRegistry concurrency
# ===========================================================================


class TestStreamRegistryConcurrency:
    """Concurrent session management is safe."""

    async def test_concurrent_register_up_to_capacity(self) -> None:
        """Registering up to max_streams succeeds, beyond returns False."""
        from digitalkin.grpc_servers.stream_registry import StreamRegistry
        from digitalkin.grpc_servers.stream_session import StreamSession

        redis = MagicMock()
        redis.eval = AsyncMock(side_effect=[1] * 10 + [0])
        pipe = MagicMock()
        pipe.decr = MagicMock(return_value=pipe)
        pipe.zrem = MagicMock(return_value=pipe)
        pipe.delete = MagicMock(return_value=pipe)
        pipe.execute = AsyncMock(return_value=[])
        redis.pipeline = MagicMock(return_value=pipe)

        registry = StreamRegistry(redis, max_streams=10)

        for i in range(10):
            accepted = await registry.register(StreamSession(task_id=f"t_{i}"))
            assert accepted is True

        assert registry.active_count == 10

        rejected = await registry.register(StreamSession(task_id="t_overflow"))
        assert rejected is False

    async def test_concurrent_register_unregister_race(self) -> None:
        """Rapid concurrent register/unregister doesn't corrupt state."""
        from digitalkin.grpc_servers.stream_registry import StreamRegistry
        from digitalkin.grpc_servers.stream_session import StreamSession

        redis = MagicMock()
        redis.eval = AsyncMock(return_value=1)
        pipe = MagicMock()
        pipe.decr = MagicMock(return_value=pipe)
        pipe.zrem = MagicMock(return_value=pipe)
        pipe.delete = MagicMock(return_value=pipe)
        pipe.execute = AsyncMock(return_value=[])
        redis.pipeline = MagicMock(return_value=pipe)

        registry = StreamRegistry(redis, max_streams=100)

        async def churn(i: int) -> None:
            tid = f"churn_{i}"
            s = StreamSession(task_id=tid)
            await registry.register(s)
            await asyncio.sleep(0)
            await registry.unregister(tid)

        await asyncio.gather(*[churn(i) for i in range(50)])
        assert registry.active_count == 0
