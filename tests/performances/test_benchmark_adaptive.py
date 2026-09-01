"""Adaptive performance benchmark for CI/CD.

Lightweight local benchmark that measures key operations and fails if
latency exceeds budgets. Inspired by scripts/scalability_bench.py but
designed for pytest: no external server, no Docker, runs in-process.

Three phases per operation:
1. Warmup — discard results
2. Measure — collect latency samples
3. Assert — fail if p95 exceeds budget

Budgets are intentionally generous for CI runners. Production targets
are tighter (see docs/architecture_presentation.md).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import statistics
import time
from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

pytestmark = [pytest.mark.stress, pytest.mark.timeout(30)]

# Latency budgets (milliseconds) — generous for CI, not prod targets
BUDGETS = {
    "circuit_breaker_check": 0.1,        # < 100µs
    "circuit_breaker_record": 0.1,       # < 100µs
    "signal_dispatch": 0.5,              # < 500µs
    "signal_dedup": 0.5,                 # < 500µs
    "send_buffer_enqueue": 1.0,          # < 1ms (no flush)
    "stream_session_enqueue": 1.0,       # < 1ms (queue not full)
    "stream_registry_register": 0.5,     # < 500µs
}

WARMUP_ITERATIONS = 10
MEASURE_ITERATIONS = 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client() -> MagicMock:
    mock = MagicMock()
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.psubscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    pubsub.punsubscribe = AsyncMock()
    pubsub.aclose = AsyncMock()
    mock.pubsub.return_value = pubsub
    return mock


def _measure(fn: Any, iterations: int = MEASURE_ITERATIONS) -> list[float]:
    """Run fn() iterations times, return latency_ms list."""
    latencies = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        fn()
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
        latencies.append(elapsed_ms)
    return latencies


async def _measure_async(fn: Any, iterations: int = MEASURE_ITERATIONS) -> list[float]:
    """Run async fn() iterations times, return latency_ms list."""
    latencies = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        await fn()
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
        latencies.append(elapsed_ms)
    return latencies


def _assert_budget(latencies: list[float], budget_ms: float, label: str) -> None:
    """Assert p95 latency is within budget."""
    p95 = sorted(latencies)[int(len(latencies) * 0.95)]
    p50 = statistics.median(latencies)
    mean = statistics.mean(latencies)
    assert p95 <= budget_ms, (
        f"{label}: p95={p95:.3f}ms exceeds budget={budget_ms}ms "
        f"(p50={p50:.3f}ms, mean={mean:.3f}ms, n={len(latencies)})"
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_singletons() -> Generator[None]:
    from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
    from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()
    yield
    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()


# ===========================================================================
# CircuitBreaker benchmarks
# ===========================================================================


class TestCircuitBreakerPerf:
    """CB operations must be sub-microsecond on the hot path."""

    def test_check_latency(self) -> None:
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

        cb = CircuitBreaker("perf_check", fail_max=100, reset_timeout=30.0)

        # Warmup
        for _ in range(WARMUP_ITERATIONS):
            cb.check()

        latencies = _measure(cb.check)
        _assert_budget(latencies, BUDGETS["circuit_breaker_check"], "CB.check()")

    def test_record_success_latency(self) -> None:
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

        cb = CircuitBreaker("perf_rec", fail_max=100, reset_timeout=30.0)

        for _ in range(WARMUP_ITERATIONS):
            cb.record_success()

        latencies = _measure(cb.record_success)
        _assert_budget(latencies, BUDGETS["circuit_breaker_record"], "CB.record_success()")


# ===========================================================================
# Signal dispatch benchmarks
# ===========================================================================


class TestSignalDispatchPerf:
    """Signal dispatch and dedup must be fast (sync path, no I/O)."""

    @staticmethod
    async def _setup_registered_task() -> tuple[Any, asyncio.Task[None]]:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        session = MagicMock()
        session.pending_signal_action = ""
        session.last_signal_published_ns = 0

        async def long_running() -> None:
            await asyncio.sleep(60)

        task = asyncio.create_task(long_running())
        await listener.start()  # register() requires the listen loop to be running
        listener.register("perf_task", session, task)
        return listener, task

    async def test_dispatch_latency(self) -> None:
        listener, task = await self._setup_registered_task()
        try:
            # Warmup — non-critical action so dispatch_signal doesn't task.cancel().
            for i in range(WARMUP_ITERATIONS):
                data = {"i": i, "action": "ping"}
                listener.dispatch_signal("perf_task", data, json.dumps(data))

            latencies = []
            for i in range(MEASURE_ITERATIONS):
                data = {"i": WARMUP_ITERATIONS + i, "action": "ping"}
                raw = json.dumps(data)
                start = time.perf_counter_ns()
                listener.dispatch_signal("perf_task", data, raw)
                elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
                latencies.append(elapsed_ms)

            _assert_budget(latencies, BUDGETS["signal_dispatch"], "dispatch_signal()")
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await listener.close()

    async def test_dedup_latency(self) -> None:
        listener, task = await self._setup_registered_task()
        try:
            data = {"action": "ping", "fixed": True}
            raw = json.dumps(data)
            listener.dispatch_signal("perf_task", data, raw)

            latencies = []
            for _ in range(MEASURE_ITERATIONS):
                start = time.perf_counter_ns()
                listener.dispatch_signal("perf_task", data, raw)
                elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
                latencies.append(elapsed_ms)

            _assert_budget(latencies, BUDGETS["signal_dedup"], "dispatch_signal(dedup)")
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await listener.close()


# ===========================================================================
# StreamSession enqueue benchmarks
# ===========================================================================


# StreamSessionPerf removed in Phase 4.A — StreamSession no longer holds
# asyncio.Queues; both directions go through Redis Streams.


# ===========================================================================
# StreamRegistry register benchmarks
# ===========================================================================


class TestStreamRegistryPerf:
    """Register/unregister must scale to thousands."""

    async def test_register_latency(self) -> None:
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

        reg = StreamRegistry(redis)

        for i in range(WARMUP_ITERATIONS):
            await reg.register(StreamSession(task_id=f"warmup_{i}"))

        latencies = []
        for i in range(MEASURE_ITERATIONS):
            s = StreamSession(task_id=f"bench_{i}")
            start = time.perf_counter_ns()
            await reg.register(s)
            elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
            latencies.append(elapsed_ms)

        _assert_budget(latencies, BUDGETS["stream_registry_register"], "registry.register()")
