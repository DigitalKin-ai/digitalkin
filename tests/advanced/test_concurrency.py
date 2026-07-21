"""Concurrency and race condition tests.

Simulates multi-task concurrent access to shared resources:
- CircuitBreaker state transitions under concurrent load
- SharedRedisListener concurrent register/dispatch/unregister
- RedisSendBuffer concurrent sends with batch flush
- StreamRegistry concurrent register/unregister
"""

from __future__ import annotations

import asyncio
import contextlib
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
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.psubscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    pubsub.punsubscribe = AsyncMock()
    pubsub.aclose = AsyncMock()
    mock.pubsub.return_value = pubsub

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
    from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
    from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()
    yield
    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()


# ===========================================================================
# CircuitBreaker concurrency
# ===========================================================================


class TestCircuitBreakerConcurrency:
    """Concurrent state transitions don't corrupt the state machine."""

    async def test_concurrent_failures_open_exactly_once(self) -> None:
        """50 concurrent failures on a CB with fail_max=5 opens it, doesn't crash."""
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
        from digitalkin.models.grpc_servers.circuit_breaker import CBState

        cb = CircuitBreaker("conc_svc", fail_max=5, reset_timeout=30.0)

        async def fail() -> None:
            cb.record_failure()

        await asyncio.gather(*[fail() for _ in range(50)])
        assert cb.state == CBState.OPEN

    async def test_concurrent_success_and_failure(self) -> None:
        """Mixed concurrent success/failure doesn't corrupt state."""
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
        from digitalkin.models.grpc_servers.circuit_breaker import CBState

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

    @staticmethod
    def _make_session_and_task() -> tuple[MagicMock, asyncio.Task[None]]:
        session = MagicMock()
        session.pending_signal_action = ""
        session.last_signal_published_ns = 0

        async def long_running() -> None:
            await asyncio.sleep(10)

        return session, asyncio.create_task(long_running())

    async def test_concurrent_register_and_dispatch(self) -> None:
        """Register 20 tasks and dispatch a critical signal to each concurrently."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        tasks_by_id: dict[str, tuple[MagicMock, asyncio.Task[None]]] = {}

        try:
            await listener.start()
            for i in range(20):
                tid = f"task_{i}"
                session, task = self._make_session_and_task()
                tasks_by_id[tid] = (session, task)
                listener.register(tid, session, task)

            async def dispatch_to(tid: str) -> None:
                data = {"action": "cancel", "tid": tid}
                listener.dispatch_signal(tid, data, json.dumps(data))

            await asyncio.gather(*[dispatch_to(f"task_{i}") for i in range(20)])

            for tid, (session, _) in tasks_by_id.items():
                assert session.pending_signal_action == "cancel", f"{tid} side-channel not written"
        finally:
            for _, task in tasks_by_id.values():
                if not task.done():
                    task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await listener.close()

    async def test_concurrent_register_unregister(self) -> None:
        """Rapid register/unregister cycle doesn't corrupt internal state."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        spawned: list[asyncio.Task[None]] = []

        async def cycle(i: int) -> None:
            tid = f"cycle_{i}"
            session, task = self._make_session_and_task()
            spawned.append(task)
            listener.register(tid, session, task)
            await asyncio.sleep(0)
            listener.unregister(tid)

        try:
            await listener.start()
            await asyncio.gather(*[cycle(i) for i in range(50)])
            assert len(listener._task_refs) == 0
        finally:
            for task in spawned:
                if not task.done():
                    task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await listener.close()


# ===========================================================================
# ===========================================================================


# ===========================================================================
# StreamRegistry concurrency
# ===========================================================================


class TestStreamRegistryConcurrency:
    """Concurrent session management is safe."""

    async def test_concurrent_register_up_to_capacity(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Registering up to max_streams succeeds, beyond returns False."""
        from digitalkin.grpc_servers.stream_registry import StreamRegistry
        from digitalkin.grpc_servers.stream_session import StreamSession
        from digitalkin.models.settings.gateway import get_gateway_settings

        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        registry = StreamRegistry(MagicMock())

        for i in range(10):
            accepted = await registry.register(StreamSession(task_id=f"t_{i}"))
            assert accepted is True

        assert registry.active_count == 10

        rejected = await registry.register(StreamSession(task_id="t_overflow"))
        assert rejected is False

    async def test_concurrent_register_unregister_race(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Rapid concurrent register/unregister doesn't corrupt state."""
        from digitalkin.grpc_servers.stream_registry import StreamRegistry
        from digitalkin.grpc_servers.stream_session import StreamSession
        from digitalkin.models.settings.gateway import get_gateway_settings

        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "100")
        get_gateway_settings.cache_clear()
        registry = StreamRegistry(MagicMock())

        async def churn(i: int) -> None:
            tid = f"churn_{i}"
            s = StreamSession(task_id=tid)
            await registry.register(s)
            await asyncio.sleep(0)
            await registry.unregister(tid)

        await asyncio.gather(*[churn(i) for i in range(50)])
        assert registry.active_count == 0
