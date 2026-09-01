"""Tests for per-service circuit breaker.

Validates CLOSED -> OPEN -> HALF_OPEN -> CLOSED state machine,
failure counting, reset timeout, probe locking, and singleton pattern.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING
from unittest.mock import patch

import grpc
import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

from digitalkin.grpc_servers.exceptions import CircuitOpenError
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.models.grpc_servers.circuit_breaker import CBState

pytestmark = pytest.mark.timeout(10)


@pytest.fixture(autouse=True)
def _clear_instances() -> Iterator[None]:
    """Reset singleton state between tests, including after — the singleton leaks to other test files otherwise."""
    CircuitBreaker._instances.clear()
    yield
    CircuitBreaker._instances.clear()


class TestCircuitBreakerStates:
    """State machine transitions."""

    def test_starts_closed(self) -> None:
        cb = CircuitBreaker("svc_a", 3, 1.0)
        assert cb.state == CBState.CLOSED

    def test_opens_after_fail_max(self) -> None:
        cb = CircuitBreaker("svc_b", 3, 30.0)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CBState.OPEN

    def test_check_raises_when_open(self) -> None:
        cb = CircuitBreaker("svc_c", 1, 30.0)
        cb.record_failure()
        with pytest.raises(CircuitOpenError):
            cb.check()

    def test_transitions_to_half_open_after_timeout(self) -> None:
        cb = CircuitBreaker("svc_d", 1, 0.01)
        cb.record_failure()
        assert cb.state == CBState.OPEN
        time.sleep(0.02)
        assert cb.state == CBState.HALF_OPEN

    def test_half_open_allows_one_probe(self) -> None:
        cb = CircuitBreaker("svc_e", 1, 0.01)
        cb.record_failure()
        time.sleep(0.02)

        cb.check()  # First probe allowed

        with pytest.raises(CircuitOpenError):
            cb.check()  # Second probe blocked

    def test_probe_success_closes_circuit(self) -> None:
        cb = CircuitBreaker("svc_f", 1, 0.01)
        cb.record_failure()
        time.sleep(0.02)

        cb.check()  # Allow probe
        cb.record_success()  # Probe succeeded

        assert cb.state == CBState.CLOSED
        cb.check()  # Should not raise

    def test_probe_failure_reopens_circuit(self) -> None:
        cb = CircuitBreaker("svc_g", 1, 0.01)
        cb.record_failure()
        time.sleep(0.02)

        cb.check()  # Allow probe
        cb.record_failure()  # Probe failed

        assert cb.state == CBState.OPEN

    def test_success_resets_failure_count(self) -> None:
        cb = CircuitBreaker("svc_h", 3, 30.0)
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
        a = CircuitBreaker("svc_1", 1, 30.0)
        b = CircuitBreaker("svc_2", 1, 30.0)

        a.record_failure()
        assert a.state == CBState.OPEN
        assert b.state == CBState.CLOSED

    def test_reset_clears_state(self) -> None:
        cb = CircuitBreaker("svc_r", 1, 30.0)
        cb.record_failure()
        assert cb.state == CBState.OPEN

        cb.reset()
        assert cb.state == CBState.CLOSED
        assert cb._failure_count == 0


class TestCircuitBreakerIntegrationWithWrapper:
    """Verify CB is invoked from GrpcClientWrapper.exec_grpc_query."""

    async def test_circuit_breaker_is_checked_in_exec_grpc_query(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure exec_grpc_query calls CB check/record_success on happy path."""
        from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
        from digitalkin.models.settings.grpc_client import get_circuit_breaker_settings

        wrapper = object.__new__(GrpcClientWrapper)
        wrapper.service_name = "TestService"
        wrapper.stub = type("Stub", (), {"Query": lambda _self, req, _timeout, _metadata=None: req})()

        # Pre-open the circuit (fail_max=1 → single failure opens it)
        monkeypatch.setenv("DIGITALKIN_CB_FAIL_MAX", "1")
        get_circuit_breaker_settings.cache_clear()
        cb = CircuitBreaker.get_or_create("TestService")
        cb.record_failure()

        from digitalkin.grpc_servers.exceptions import ServerError

        with pytest.raises(ServerError, match="Circuit open"):
            await wrapper.exec_grpc_query("Query", "request")

    @staticmethod
    def _stub_raising(code: grpc.StatusCode) -> object:
        """Build a one-method stub whose RPC raises an RpcError with ``code``."""

        class _Err(grpc.RpcError):
            def code(self) -> grpc.StatusCode:
                return code

            def details(self) -> str:
                return "boom"

        async def _raise(  # noqa: RUF029
            _self: object, _req: object, timeout: object = None, metadata: object = None
        ) -> None:
            raise _Err

        return type("Stub", (), {"ReadRecord": _raise})()

    @pytest.mark.unit
    async def test_not_found_does_not_trip_breaker(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """NOT_FOUND is an application response, not a service-health failure.

        Regression (prod load, StorageService): a burst of new-session reads
        each returns NOT_FOUND; those must not count toward opening the
        breaker. The service answered, so the failure counter must stay 0 no
        matter how many misses occur.
        """
        from digitalkin.grpc_servers.exceptions import ServerError
        from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
        from digitalkin.models.settings.grpc_client import (
            get_circuit_breaker_settings,
            get_grpc_client_settings,
        )

        monkeypatch.setenv("DIGITALKIN_CB_FAIL_MAX", "3")
        monkeypatch.setenv("DIGITALKIN_GRPC_QUERY_MAX_RETRIES", "0")
        get_circuit_breaker_settings.cache_clear()
        get_grpc_client_settings.cache_clear()

        wrapper = object.__new__(GrpcClientWrapper)
        wrapper.service_name = "StorageService"
        wrapper.stub = self._stub_raising(grpc.StatusCode.NOT_FOUND)

        digitalkin_logger = logging.getLogger("digitalkin")
        monkeypatch.setattr(digitalkin_logger, "propagate", True)
        with caplog.at_level(logging.WARNING, logger="digitalkin"):
            for _ in range(6):  # well past fail_max=3
                with pytest.raises(ServerError):
                    await wrapper.exec_grpc_query("ReadRecord", "request")

        cb = CircuitBreaker.get_or_create("StorageService")
        assert cb.state == CBState.CLOSED
        assert cb._failure_count == 0
        assert [r for r in caplog.records if "circuit-breaker tick" in r.getMessage()] == []

    @pytest.mark.unit
    async def test_unavailable_trips_breaker(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Real service-health failures (UNAVAILABLE) still open the breaker."""
        from digitalkin.grpc_servers.exceptions import ServerError
        from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
        from digitalkin.models.settings.grpc_client import (
            get_circuit_breaker_settings,
            get_grpc_client_settings,
        )

        monkeypatch.setenv("DIGITALKIN_CB_FAIL_MAX", "3")
        monkeypatch.setenv("DIGITALKIN_GRPC_QUERY_MAX_RETRIES", "0")
        get_circuit_breaker_settings.cache_clear()
        get_grpc_client_settings.cache_clear()

        wrapper = object.__new__(GrpcClientWrapper)
        wrapper.service_name = "StorageService"
        wrapper.stub = self._stub_raising(grpc.StatusCode.UNAVAILABLE)

        digitalkin_logger = logging.getLogger("digitalkin")
        monkeypatch.setattr(digitalkin_logger, "propagate", True)
        with caplog.at_level(logging.WARNING, logger="digitalkin"):
            for _ in range(3):
                with pytest.raises(ServerError):
                    await wrapper.exec_grpc_query("ReadRecord", "request")

        cb = CircuitBreaker.get_or_create("StorageService")
        assert cb.state == CBState.OPEN
        # Dedupe by emission: pytest 9.1.x's caplog can capture a propagated
        # record as multiple copies, so count distinct (time, message) emissions.
        ticks = {
            (r.created, r.getMessage())
            for r in caplog.records
            if "circuit-breaker tick" in r.getMessage()
        }
        assert len(ticks) == 3, [r.getMessage() for r in caplog.records]
        assert all("StorageService.ReadRecord [UNAVAILABLE]" in msg for _, msg in ticks)
