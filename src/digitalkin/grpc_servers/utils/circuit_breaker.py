"""Per-service circuit breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.

Protects outbound gRPC calls from cascade failure. When a service fails
repeatedly, the circuit opens and all calls fail fast with ``CircuitOpenError``
instead of waiting for the full timeout.

Integrates into ``GrpcClientWrapper.exec_grpc_query()`` as a pre/post hook.
"""

from __future__ import annotations

import os
import time
from enum import Enum
from typing import ClassVar

from digitalkin.logger import logger


class CBState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a call is attempted on an open circuit."""


class CircuitBreaker:
    """Per-service circuit breaker with local state.

    State machine:
    - CLOSED: all calls pass. Failure counter increments on error, resets on success.
    - OPEN (after fail_max consecutive failures): all calls fail with CircuitOpenError.
    - HALF_OPEN (after reset_timeout): one probe call allowed. Success -> CLOSED, failure -> OPEN.

    Attributes:
        service_id: Identifier for the protected service.
    """

    _instances: ClassVar[dict[str, CircuitBreaker]] = {}

    service_id: str
    _state: CBState
    _failure_count: int
    _fail_max: int
    _reset_timeout: float
    _last_failure_time: float
    _half_open_lock: bool

    @classmethod
    def get_or_create(
        cls,
        service_id: str,
        fail_max: int = int(os.environ.get("DIGITALKIN_CB_FAIL_MAX", "5")),
        reset_timeout: float = float(os.environ.get("DIGITALKIN_CB_RESET_TIMEOUT", "30")),
    ) -> CircuitBreaker:
        """Get existing circuit breaker for a service or create one.

        Args:
            service_id: Service identifier.
            fail_max: Consecutive failures before opening.
            reset_timeout: Seconds to wait before half-open probe.

        Returns:
            Circuit breaker for this service.
        """
        if service_id not in cls._instances:
            cls._instances[service_id] = cls(service_id, fail_max, reset_timeout)
        return cls._instances[service_id]

    def __init__(self, service_id: str, fail_max: int, reset_timeout: float) -> None:
        """Initialize circuit breaker internal state.

        Args:
            service_id: Identifier for the protected service.
            fail_max: Consecutive failures before opening.
            reset_timeout: Seconds before half-open probe.

        Raises:
            ValueError: If fail_max or reset_timeout are not positive.
        """
        if fail_max <= 0:
            msg = f"fail_max must be > 0, got {fail_max}"
            raise ValueError(msg)
        if reset_timeout <= 0:
            msg = f"reset_timeout must be > 0, got {reset_timeout}"
            raise ValueError(msg)
        self.service_id = service_id
        self._state = CBState.CLOSED
        self._failure_count = 0
        self._fail_max = fail_max
        self._reset_timeout = reset_timeout
        self._last_failure_time = 0.0
        self._half_open_lock = False

    @property
    def state(self) -> CBState:
        """Current circuit state, auto-transitioning OPEN -> HALF_OPEN on timeout."""
        if self._state == CBState.OPEN and time.monotonic() - self._last_failure_time >= self._reset_timeout:
            self._state = CBState.HALF_OPEN
            self._half_open_lock = False
            logger.info("Circuit breaker %s: OPEN -> HALF_OPEN", self.service_id)
        return self._state

    def check(self) -> None:
        """Check if a call is allowed. Must be called before each outbound call.

        Raises:
            CircuitOpenError: If the circuit is open and not yet eligible for probe.
        """
        current = self.state
        if current == CBState.OPEN:
            remaining = self._reset_timeout - (time.monotonic() - self._last_failure_time)
            msg = f"Circuit open for {self.service_id}, retry after {remaining:.1f}s"
            raise CircuitOpenError(msg)
        if current == CBState.HALF_OPEN and self._half_open_lock:
            msg = f"Circuit half-open for {self.service_id}, probe in progress"
            raise CircuitOpenError(msg)
        if current == CBState.HALF_OPEN:
            self._half_open_lock = True

    def record_success(self) -> None:
        """Record a successful call. Resets failure counter and closes circuit."""
        if self._state == CBState.HALF_OPEN:
            logger.info("Circuit breaker %s: HALF_OPEN -> CLOSED (probe succeeded)", self.service_id)
        self._state = CBState.CLOSED
        self._failure_count = 0
        self._half_open_lock = False

    def record_failure(self) -> None:
        """Record a failed call. Increments counter and may open circuit."""
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CBState.HALF_OPEN:
            self._state = CBState.OPEN
            self._half_open_lock = False
            logger.warning("Circuit breaker %s: HALF_OPEN -> OPEN (probe failed)", self.service_id)
        elif self._failure_count >= self._fail_max:
            self._state = CBState.OPEN
            logger.warning(
                "Circuit breaker %s: CLOSED -> OPEN (%d consecutive failures)",
                self.service_id,
                self._failure_count,
            )

    def reset(self) -> None:
        """Force reset to CLOSED state."""
        self._state = CBState.CLOSED
        self._failure_count = 0
        self._half_open_lock = False

    @classmethod
    def remove(cls, service_id: str) -> None:
        """Remove a circuit breaker for a service. Prevents singleton leak.

        Called when the last channel for a service is closed.

        Args:
            service_id: Service identifier to remove.
        """
        cls._instances.pop(service_id, None)

    @classmethod
    def clear_all(cls) -> None:
        """Remove all circuit breaker instances. For shutdown and testing."""
        cls._instances.clear()
