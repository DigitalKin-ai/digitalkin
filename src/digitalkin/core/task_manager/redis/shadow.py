"""Shadow Redis client for canary deployments.

Dual-writes to both stable and canary Redis instances. Returns the
stable response always. Canary errors are logged, never propagated.
Circuit breaker disables canary if error rate exceeds threshold.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

from digitalkin.logger import logger

if TYPE_CHECKING:
    from collections.abc import Awaitable

_ERROR_THRESHOLD_DEFAULT = 5.0
_WINDOW_SECONDS_DEFAULT = 60.0


class ShadowRedisClient:
    """Dual-write Redis client for canary validation.

    Writes go to both stable and canary. Reads come from stable only.
    Canary failures are logged, never raised to callers.
    """

    _canary_enabled: bool
    _canary_errors: int
    _stable_errors: int
    _window_start: float

    def __init__(
        self,
        stable: Any,
        canary: Any,
        error_threshold_ratio: float = _ERROR_THRESHOLD_DEFAULT,
        window_seconds: float = _WINDOW_SECONDS_DEFAULT,
    ) -> None:
        """Initialize shadow client.

        Args:
            stable: Primary RedisClient instance.
            canary: Canary RedisClient instance.
            error_threshold_ratio: Disable canary if canary_errors > ratio * stable_errors.
            window_seconds: Error counting window duration.
        """
        self._stable = stable
        self._canary = canary
        self._canary_enabled = True
        self._canary_errors = 0
        self._stable_errors = 0
        self._window_start = time.monotonic()
        self._error_threshold_ratio = error_threshold_ratio
        self._window_seconds = window_seconds

    def _reset_window_if_needed(self) -> None:
        """Reset error counters if window has elapsed."""
        now = time.monotonic()
        if now - self._window_start >= self._window_seconds:
            self._canary_errors = 0
            self._stable_errors = 0
            self._window_start = now
            if not self._canary_enabled:
                self._canary_enabled = True
                logger.info("Shadow canary re-enabled after window reset")

    def _check_circuit(self) -> None:
        """Disable canary if error rate exceeds threshold."""
        if not self._canary_enabled:
            return
        stable_baseline = max(self._stable_errors, 1)
        if self._canary_errors > self._error_threshold_ratio * stable_baseline:
            self._canary_enabled = False
            logger.warning(
                "Shadow canary disabled: canary_errors=%d > %.0fx stable_errors=%d",
                self._canary_errors,
                self._error_threshold_ratio,
                self._stable_errors,
            )

    async def _dual(self, stable_coro: Awaitable[Any], canary_coro: Awaitable[Any] | None) -> Any:
        """Execute on both clients, return stable result.

        Args:
            stable_coro: Awaitable from the stable client.
            canary_coro: Awaitable from the canary client, or None if disabled.

        Returns:
            Result from stable client.

        Raises:
            Exception: If stable client fails (re-raised to caller).
        """
        self._reset_window_if_needed()

        if canary_coro is None:
            return await stable_coro

        results = await asyncio.gather(stable_coro, canary_coro, return_exceptions=True)
        stable_result, canary_result = results

        if isinstance(stable_result, Exception):
            self._stable_errors += 1
            raise stable_result

        if isinstance(canary_result, Exception):
            self._canary_errors += 1
            logger.debug("Shadow canary error: %s", canary_result)
            self._check_circuit()

        return stable_result

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        """Dual-write SET.

        Returns:
            True if set on stable.
        """
        stable_coro = self._stable.set(name, value, ex=ex)
        canary_coro = self._canary.set(name, value, ex=ex) if self._canary_enabled else None
        return await self._dual(stable_coro, canary_coro)

    async def get(self, name: str) -> bytes | None:
        """Read from stable only.

        Returns:
            Value as bytes or None.
        """
        return await self._stable.get(name)

    async def hset(self, name: str, mapping: dict) -> int:
        """Dual-write HSET.

        Returns:
            Number of new fields added (stable).
        """
        return await self._dual(
            self._stable.hset(name, mapping),
            self._canary.hset(name, mapping) if self._canary_enabled else None,
        )

    async def hgetall(self, name: str) -> dict:
        """Read from stable only.

        Returns:
            All field-value pairs.
        """
        return await self._stable.hgetall(name)

    async def xadd(self, name: str, fields: dict, *, maxlen: int | None = None) -> bytes:
        """Dual-write XADD.

        Returns:
            Entry ID from stable.
        """
        return await self._dual(
            self._stable.xadd(name, fields, maxlen=maxlen),
            self._canary.xadd(name, fields, maxlen=maxlen) if self._canary_enabled else None,
        )

    async def delete(self, *names: str) -> int:
        """Dual-write DELETE.

        Returns:
            Number of keys deleted (stable).
        """
        return await self._dual(
            self._stable.delete(*names),
            self._canary.delete(*names) if self._canary_enabled else None,
        )

    async def expire(self, name: str, seconds: int) -> bool:
        """Dual-write EXPIRE.

        Returns:
            True if timeout was set (stable).
        """
        return await self._dual(
            self._stable.expire(name, seconds),
            self._canary.expire(name, seconds) if self._canary_enabled else None,
        )

    async def close(self) -> None:
        """Close both clients."""
        await self._stable.close()
        await self._canary.close()

    @property
    def canary_enabled(self) -> bool:
        """Whether the canary is currently active."""
        return self._canary_enabled
