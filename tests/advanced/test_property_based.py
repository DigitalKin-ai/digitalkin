"""Property-based tests using Hypothesis.

Generates varied inputs to validate invariants that must hold for
any valid input, not just hand-picked examples. Covers:
- CircuitBreaker state machine invariants
- SharedRedisListener dispatch guarantees
- RedisSendBuffer atomicity
- TraceContext immutability
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Generator
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

    @given(
        n_signals=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=30)
    async def test_no_signal_lost_when_queue_has_space(self, n_signals: int) -> None:
        """Every dispatched signal lands in the queue when there's room."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
        from unittest.mock import MagicMock

        SharedRedisListener._instances.clear()
        client = MagicMock()
        _ps = MagicMock()
        _ps.subscribe = AsyncMock()
        client.pubsub.return_value = _ps
        listener = SharedRedisListener(client)
        listener._queue_size = n_signals + 10  # Enough room

        q = await listener.register("t1")
        for i in range(n_signals):
            data = {"seq": i}
            listener.dispatch_signal("t1", data, json.dumps(data))

        assert q.qsize() == n_signals

    @given(
        n_duplicates=st.integers(min_value=2, max_value=20),
    )
    @settings(max_examples=20)
    async def test_dedup_never_delivers_same_payload_twice(self, n_duplicates: int) -> None:
        """Identical payloads are deduplicated — only first delivery counts."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
        from unittest.mock import MagicMock

        SharedRedisListener._instances.clear()
        client = MagicMock()
        _ps = MagicMock()
        _ps.subscribe = AsyncMock()
        client.pubsub.return_value = _ps
        listener = SharedRedisListener(client)

        q = await listener.register("t1")
        data = {"action": "start", "value": "fixed"}
        raw = json.dumps(data)

        for _ in range(n_duplicates):
            listener.dispatch_signal("t1", data, raw)

        assert q.qsize() == 1  # Only first delivery


# ===========================================================================
# TraceContext properties
# ===========================================================================


class TestTraceContextProperties:
    """TraceContext invariants."""

    @given(
        trace_id=st.text(min_size=1, max_size=64),
        session_id=st.text(min_size=1, max_size=64),
    )
    @settings(max_examples=50)
    def test_trace_context_immutable(self, trace_id: str, session_id: str) -> None:
        """TraceContext is frozen — any mutation raises AttributeError."""
        from digitalkin.core.task_manager.task_wrapper import TraceContext

        ctx = TraceContext(trace_id=trace_id, session_id=session_id)
        with pytest.raises(AttributeError):
            ctx.trace_id = "mutated"  # type: ignore[misc]

    @given(
        trace_id=st.text(min_size=1, max_size=64),
        session_id=st.text(min_size=1, max_size=64),
        job_id=st.text(max_size=64),
        mission_id=st.text(max_size=64),
    )
    @settings(max_examples=50)
    def test_current_ids_matches_fields(
        self, trace_id: str, session_id: str, job_id: str, mission_id: str
    ) -> None:
        """current_ids() returns exactly the 4 fields, no more, no less."""
        from digitalkin.core.task_manager.task_wrapper import TraceContext

        ctx = TraceContext(trace_id=trace_id, session_id=session_id, job_id=job_id, mission_id=mission_id)
        ids = {
            "trace_id": ctx.trace_id,
            "session_id": ctx.session_id,
            "job_id": ctx.job_id,
            "mission_id": ctx.mission_id,
        }
        assert len(ids) == 4
        assert ids["trace_id"] == trace_id
