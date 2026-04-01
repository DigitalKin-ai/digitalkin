"""Tests for per-service circuit breaker.

Validates CLOSED -> OPEN -> HALF_OPEN -> CLOSED state machine,
failure counting, reset timeout, probe locking, and singleton pattern.
"""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from digitalkin.grpc_servers.utils.circuit_breaker import CBState, CircuitBreaker, CircuitOpenError

pytestmark = pytest.mark.timeout(10)


@pytest.fixture(autouse=True)
def _clear_instances() -> None:
    """Reset singleton state between tests."""
    CircuitBreaker._instances.clear()


class TestCircuitBreakerStates:
    """State machine transitions."""

    def test_starts_closed(self) -> None:
        cb = CircuitBreaker.get_or_create("svc_a", fail_max=3, reset_timeout=1.0)
        assert cb.state == CBState.CLOSED

    def test_opens_after_fail_max(self) -> None:
        cb = CircuitBreaker.get_or_create("svc_b", fail_max=3, reset_timeout=30.0)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CBState.OPEN

    def test_check_raises_when_open(self) -> None:
        cb = CircuitBreaker.get_or_create("svc_c", fail_max=1, reset_timeout=30.0)
        cb.record_failure()
        with pytest.raises(CircuitOpenError):
            cb.check()

    def test_transitions_to_half_open_after_timeout(self) -> None:
        cb = CircuitBreaker.get_or_create("svc_d", fail_max=1, reset_timeout=0.01)
        cb.record_failure()
        assert cb.state == CBState.OPEN
        time.sleep(0.02)
        assert cb.state == CBState.HALF_OPEN

    def test_half_open_allows_one_probe(self) -> None:
        cb = CircuitBreaker.get_or_create("svc_e", fail_max=1, reset_timeout=0.01)
        cb.record_failure()
        time.sleep(0.02)

        cb.check()  # First probe allowed

        with pytest.raises(CircuitOpenError):
            cb.check()  # Second probe blocked

    def test_probe_success_closes_circuit(self) -> None:
        cb = CircuitBreaker.get_or_create("svc_f", fail_max=1, reset_timeout=0.01)
        cb.record_failure()
        time.sleep(0.02)

        cb.check()  # Allow probe
        cb.record_success()  # Probe succeeded

        assert cb.state == CBState.CLOSED
        cb.check()  # Should not raise

    def test_probe_failure_reopens_circuit(self) -> None:
        cb = CircuitBreaker.get_or_create("svc_g", fail_max=1, reset_timeout=0.01)
        cb.record_failure()
        time.sleep(0.02)

        cb.check()  # Allow probe
        cb.record_failure()  # Probe failed

        assert cb.state == CBState.OPEN

    def test_success_resets_failure_count(self) -> None:
        cb = CircuitBreaker.get_or_create("svc_h", fail_max=3, reset_timeout=30.0)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb._failure_count == 0

        # Two more failures should not open (counter was reset)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CBState.CLOSED


class TestCircuitBreakerSingleton:
    """Per-service singleton behavior."""

    def test_same_service_returns_same_instance(self) -> None:
        a = CircuitBreaker.get_or_create("svc_x")
        b = CircuitBreaker.get_or_create("svc_x")
        assert a is b

    def test_different_services_are_independent(self) -> None:
        a = CircuitBreaker.get_or_create("svc_1", fail_max=1, reset_timeout=30.0)
        b = CircuitBreaker.get_or_create("svc_2", fail_max=1, reset_timeout=30.0)

        a.record_failure()
        assert a.state == CBState.OPEN
        assert b.state == CBState.CLOSED

    def test_reset_clears_state(self) -> None:
        cb = CircuitBreaker.get_or_create("svc_r", fail_max=1, reset_timeout=30.0)
        cb.record_failure()
        assert cb.state == CBState.OPEN

        cb.reset()
        assert cb.state == CBState.CLOSED
        assert cb._failure_count == 0


class TestCircuitBreakerIntegrationWithWrapper:
    """Verify CB is invoked from GrpcClientWrapper.exec_grpc_query."""

    async def test_circuit_breaker_is_checked_in_exec_grpc_query(self) -> None:
        """Ensure exec_grpc_query calls CB check/record_success on happy path."""
        from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper

        wrapper = object.__new__(GrpcClientWrapper)
        wrapper.service_name = "TestService"
        wrapper.stub = type("Stub", (), {"Query": lambda self, req, timeout: req})()

        # Pre-open the circuit
        cb = CircuitBreaker.get_or_create("TestService", fail_max=1, reset_timeout=30.0)
        cb.record_failure()

        from digitalkin.grpc_servers.utils.exceptions import ServerError

        with pytest.raises(ServerError, match="Circuit open"):
            await wrapper.exec_grpc_query("Query", "request")
