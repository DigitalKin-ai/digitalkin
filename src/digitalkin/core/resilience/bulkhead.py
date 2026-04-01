"""Bulkhead pattern — per-service concurrency limits.

Prevents one slow service from consuming all available concurrency.
Each service gets its own ``asyncio.Semaphore`` with a configurable
limit. When the limit is reached, callers wait up to ``acquire_timeout``
before raising ``BulkheadFullError``.

Usage in ModuleContext or service wrapper::

    bulkhead = Bulkhead.for_service("storage", max_concurrent=10)
    async with bulkhead:
        await storage.read(...)
"""

from __future__ import annotations

import asyncio
import os
from typing import ClassVar

from typing_extensions import Self

from digitalkin.logger import logger


class BulkheadFullError(Exception):
    """Raised when a bulkhead semaphore cannot be acquired within timeout."""


class Bulkhead:
    """Per-service concurrency limiter with timeout.

    Implements the bulkhead pattern: each service gets isolated concurrency
    so a failing/slow service cannot starve others. Singleton per service_id.
    """

    _instances: ClassVar[dict[str, Bulkhead]] = {}

    _service_id: str
    _semaphore: asyncio.Semaphore
    _max_concurrent: int
    _acquire_timeout: float
    _active: int

    @classmethod
    def for_service(
        cls,
        service_id: str,
        max_concurrent: int | None = None,
        acquire_timeout: float | None = None,
    ) -> Bulkhead:
        """Get or create a bulkhead for a service.

        Args:
            service_id: Service identifier (e.g., "storage", "registry").
            max_concurrent: Max concurrent calls. Defaults to env
                ``DIGITALKIN_BULKHEAD_{SERVICE_ID}_MAX`` or 50.
            acquire_timeout: Max seconds to wait for a slot. Defaults to env
                ``DIGITALKIN_BULKHEAD_TIMEOUT`` or 2.0.

        Returns:
            Bulkhead for this service.
        """
        if service_id in cls._instances:
            return cls._instances[service_id]

        env_key = f"DIGITALKIN_BULKHEAD_{service_id.upper()}_MAX"
        default_max = int(os.environ.get(env_key, os.environ.get("DIGITALKIN_BULKHEAD_DEFAULT_MAX", "50")))
        default_timeout = float(os.environ.get("DIGITALKIN_BULKHEAD_TIMEOUT", "2.0"))

        inst = cls(
            service_id=service_id,
            max_concurrent=max_concurrent or default_max,
            acquire_timeout=acquire_timeout or default_timeout,
        )
        cls._instances[service_id] = inst
        return inst

    @classmethod
    def clear_all(cls) -> None:
        """Remove all bulkhead instances. For shutdown and testing."""
        cls._instances.clear()

    def __init__(self, service_id: str, max_concurrent: int, acquire_timeout: float) -> None:
        """Initialize the bulkhead.

        Args:
            service_id: Service identifier.
            max_concurrent: Maximum concurrent calls allowed.
            acquire_timeout: Seconds to wait before raising BulkheadFullError.
        """
        self._service_id = service_id
        self._max_concurrent = max_concurrent
        self._acquire_timeout = acquire_timeout
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active = 0

    async def __aenter__(self) -> Self:
        """Acquire a slot, waiting up to acquire_timeout.

        Returns:
            Self for use as async context manager.

        Raises:
            BulkheadFullError: If the semaphore cannot be acquired in time.
        """
        try:
            acquired = await asyncio.wait_for(self._semaphore.acquire(), timeout=self._acquire_timeout)
        except asyncio.TimeoutError:
            active, limit, timeout = self._active, self._max_concurrent, self._acquire_timeout
            msg = f"Bulkhead full for {self._service_id}: {active}/{limit} active, waited {timeout}s"
            raise BulkheadFullError(msg) from None
        if acquired:
            self._active += 1
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Release the slot."""
        self._semaphore.release()
        self._active -= 1

    @property
    def active(self) -> int:
        """Number of currently active calls."""
        return self._active

    @property
    def available(self) -> int:
        """Number of available slots."""
        return self._max_concurrent - self._active
