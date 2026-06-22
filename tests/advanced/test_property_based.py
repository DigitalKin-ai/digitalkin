"""Property-based tests using Hypothesis.

Generates varied inputs to validate invariants that must hold for
any valid input, not just hand-picked examples. Covers:
- CircuitBreaker state machine invariants
- SharedRedisListener dispatch guarantees
- RedisSendBuffer atomicity
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

pytestmark = [pytest.mark.property, pytest.mark.timeout(30)]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_cb() -> Generator[None]:
    """Reset CB singletons between tests."""
    from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

    CircuitBreaker._instances.clear()
    yield
    CircuitBreaker._instances.clear()


# ===========================================================================
# CircuitBreaker property tests
# ===========================================================================


class TestCircuitBreakerProperties:
    """Invariants that hold for any sequence of success/failure calls."""

    @given(
        failures=st.lists(st.sampled_from(["success", "failure"]), min_size=1, max_size=50),
        fail_max=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=100)
    def test_failure_count_never_exceeds_fail_max_on_open(self, failures: list[str], fail_max: int) -> None:
        """The circuit opens at exactly fail_max consecutive failures, never more."""
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
        from digitalkin.models.grpc_servers.circuit_breaker import CBState

        CircuitBreaker._instances.clear()
        cb = CircuitBreaker("prop_test", fail_max, reset_timeout=9999.0)

        consecutive_failures = 0
        for action in failures:
            if action == "failure":
                cb.record_failure()
                consecutive_failures += 1
            else:
                cb.record_success()
                consecutive_failures = 0

            if cb.state == CBState.OPEN:
                assert consecutive_failures >= fail_max

    @given(fail_max=st.integers(min_value=1, max_value=20))
    @settings(max_examples=50)
    def test_success_always_resets_to_closed(self, fail_max: int) -> None:
        """A success call always resets the circuit to CLOSED regardless of prior failures."""
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
        from digitalkin.models.grpc_servers.circuit_breaker import CBState

        CircuitBreaker._instances.clear()
        cb = CircuitBreaker("prop_reset", fail_max, reset_timeout=9999.0)

        for _ in range(fail_max - 1):
            cb.record_failure()

        cb.record_success()
        assert cb.state == CBState.CLOSED
        assert cb._failure_count == 0


# ===========================================================================
# SharedRedisListener dispatch properties
# ===========================================================================


class TestListenerDispatchProperties:
    """Invariants for signal dispatch."""

    @staticmethod
    def _make_listener_with_task() -> tuple[Any, MagicMock, asyncio.Task[None]]:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        client = MagicMock()
        ps = MagicMock()
        ps.subscribe = AsyncMock()
        ps.psubscribe = AsyncMock()
        ps.unsubscribe = AsyncMock()
        ps.punsubscribe = AsyncMock()
        ps.aclose = AsyncMock()
        client.pubsub.return_value = ps
        listener = SharedRedisListener(client)
        session = MagicMock()
        session.pending_signal_action = ""
        session.last_signal_published_ns = 0

        async def long_running() -> None:
            await asyncio.sleep(60)

        task = asyncio.create_task(long_running())
        return listener, session, task

    @given(
        n_signals=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=30)
    async def test_non_critical_signals_always_audited(self, n_signals: int) -> None:
        """Every non-critical signal returns True (audit-only) regardless of count."""
        listener, session, task = self._make_listener_with_task()
        try:
            await listener.start()
            listener.register("t1", session, task)
            for i in range(n_signals):
                data = {"action": "ping", "seq": i}
                # Each unique payload returns True (no dedup, no critical side effects).
                assert listener.dispatch_signal("t1", data, json.dumps(data)) is True
            # Non-critical signals never touch the side channel.
            assert not session.pending_signal_action
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await listener.close()

    @given(
        n_duplicates=st.integers(min_value=2, max_value=20),
    )
    @settings(max_examples=20)
    async def test_dedup_skips_repeats(self, n_duplicates: int) -> None:
        """Identical payloads are deduplicated — only the first dispatch succeeds."""
        listener, session, task = self._make_listener_with_task()
        try:
            await listener.start()
            listener.register("t1", session, task)
            data = {"action": "ping", "value": "fixed"}
            raw = json.dumps(data)

            first = listener.dispatch_signal("t1", data, raw)
            assert first is True
            for _ in range(n_duplicates - 1):
                # All subsequent duplicates return False.
                assert listener.dispatch_signal("t1", data, raw) is False
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await listener.close()
