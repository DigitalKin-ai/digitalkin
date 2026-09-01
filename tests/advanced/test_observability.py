"""Observability assertion tests.

Validates that structured logging output contains the expected fields
and that key operations produce log events at the correct level.

Note: DigitalKin uses a custom JSON formatter. We enable propagation
to the root logger so caplog can capture the messages.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

pytestmark = [pytest.mark.timeout(10)]


@pytest.fixture(autouse=True)
def _propagate_dk_logger() -> None:  # type: ignore[misc]
    """Enable propagation and lower level on digitalkin loggers so caplog captures."""
    dk_logger = logging.getLogger("digitalkin")
    old_propagate = dk_logger.propagate
    old_level = dk_logger.level
    dk_logger.propagate = True
    dk_logger.setLevel(logging.DEBUG)
    yield
    dk_logger.propagate = old_propagate
    dk_logger.setLevel(old_level)


class TestCircuitBreakerLogging:
    """CB state transitions produce structured log events."""

    @pytest.fixture(autouse=True)
    def _clear(self) -> None:
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

        CircuitBreaker._instances.clear()
        yield  # type: ignore[misc]
        CircuitBreaker._instances.clear()

    def test_open_transition_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

        cb = CircuitBreaker("log_svc", fail_max=2, reset_timeout=30.0)
        with caplog.at_level(logging.WARNING):
            cb.record_failure()
            cb.record_failure()

        assert any("CLOSED -> OPEN" in r.message for r in caplog.records)

    def test_probe_success_logs_info(self, caplog: pytest.LogCaptureFixture) -> None:
        import time

        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

        cb = CircuitBreaker("probe_svc", fail_max=1, reset_timeout=0.01)
        cb.record_failure()
        time.sleep(0.02)  # Let it transition to HALF_OPEN

        with caplog.at_level(logging.INFO):
            cb.check()  # Allow probe
            cb.record_success()

        assert any("HALF_OPEN -> CLOSED" in r.message for r in caplog.records)


class TestRedisStateLogging:
    """RedisStateManager logs status transitions."""

    async def test_set_status_logs_debug(self, caplog: pytest.LogCaptureFixture) -> None:
        from digitalkin.core.task_manager.redis.redis_state import RedisStateManager

        client = MagicMock()
        pipe = MagicMock()
        pipe.hset.return_value = pipe
        pipe.expire.return_value = pipe

        async def fake_execute() -> list[bool]:
            return [True, True]

        pipe.execute = fake_execute
        client.pipeline.return_value = pipe

        mgr = RedisStateManager(client)

        with caplog.at_level(logging.DEBUG):
            await mgr.set_status("task_log", "running")

        assert any("task_log" in r.message and "running" in r.message for r in caplog.records)


class TestStreamSessionLogging:
    """StreamSession logs lifecycle events."""

    async def test_teardown_logs_debug(self, caplog: pytest.LogCaptureFixture) -> None:
        from digitalkin.grpc_servers.stream_session import StreamSession

        s = StreamSession(task_id="t_log_td")
        with caplog.at_level(logging.DEBUG):
            await s.teardown()

        assert any("teardown" in r.message and "t_log_td" in r.message for r in caplog.records)

    # test_enqueue_full_logs_warning removed in Phase 4.A — the
    # asyncio.Queue path was deleted; backpressure now lives in
    # ProtoStreamWriter._check_backpressure (covered separately).
