"""Instrumented Redis client wrapper for observability.

Wraps every RedisClient command with timing, structured logging,
and error tracking. Key values are never logged — only structural
patterns (key prefix). Pipeline operations are delegated directly.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import builtins

from digitalkin.logger import logger

_MIN_KEY_PARTS_FOR_REDACTION = 2


class InstrumentedRedisClient:  # noqa: PLR0904
    """Observability wrapper around RedisClient.

    Logs every command with duration, status, and key pattern.
    Values are never included in logs — only key prefixes.
    """

    _inner: Any
    _command_count: int
    _error_count: int

    def __init__(self, inner: Any) -> None:
        """Wrap a RedisClient instance.

        Args:
            inner: The underlying RedisClient to instrument.
        """
        self._inner = inner
        self._command_count = 0
        self._error_count = 0

    @staticmethod
    def _key_pattern(key: str) -> str:
        """Extract structural pattern from a key (redact specific IDs).

        Args:
            key: Full Redis key.

        Returns:
            Structural pattern with IDs replaced by ``*``.
        """
        parts = key.split(":")
        if len(parts) <= 1:
            return key
        if len(parts) > _MIN_KEY_PARTS_FOR_REDACTION:
            return ":".join(parts[0:1] + ["*"] * (len(parts) - _MIN_KEY_PARTS_FOR_REDACTION) + parts[-1:])
        return f"{parts[0]}:*"

    async def _execute(self, command: str, key: str, coro: Any) -> Any:
        """Execute a command with timing and logging.

        Args:
            command: Redis command name (SET, GET, XADD, etc.).
            key: Primary key for the operation.
            coro: Awaitable command coroutine.

        Returns:
            Command result from the underlying client.

        Raises:
            Exception: Re-raises any Redis error after logging.
        """
        self._command_count += 1
        pattern = self._key_pattern(key)
        t0 = time.monotonic()

        try:
            result = await coro
        except Exception as e:
            self._error_count += 1
            duration_ms = (time.monotonic() - t0) * 1000
            logger.error("redis.%s %s FAILED %.1fms: %s", command, pattern, duration_ms, type(e).__name__)
            raise
        else:
            duration_ms = (time.monotonic() - t0) * 1000
            logger.debug("redis.%s %s %.1fms", command, pattern, duration_ms)
            return result

    # -- String --

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        """SET with timing. See RedisClient.set for full docs.

        Returns:
            True if set successfully.
        """
        return await self._execute("SET", name, self._inner.set(name, value, ex=ex))

    async def get(self, name: str) -> bytes | None:
        """GET with timing. See RedisClient.get for full docs.

        Returns:
            Value as bytes or None.
        """
        return await self._execute("GET", name, self._inner.get(name))

    async def decr(self, name: str) -> int:
        """DECR with timing.

        Returns:
            Value after decrement.
        """
        return await self._execute("DECR", name, self._inner.decr(name))

    # -- Hash --

    async def hset(self, name: str, mapping: dict) -> int:
        """HSET with timing.

        Returns:
            Number of new fields added.
        """
        return await self._execute("HSET", name, self._inner.hset(name, mapping))

    async def hgetall(self, name: str) -> dict:
        """HGETALL with timing.

        Returns:
            All field-value pairs.
        """
        return await self._execute("HGETALL", name, self._inner.hgetall(name))

    # -- Stream --

    async def xadd(self, name: str, fields: dict, *, maxlen: int | None = None) -> bytes:
        """XADD with timing.

        Returns:
            Auto-generated entry ID.
        """
        return await self._execute("XADD", name, self._inner.xadd(name, fields, maxlen=maxlen))

    async def xread(self, streams: dict, *, count: int = 50, block: int = 100) -> list:
        """XREAD with timing.

        Returns:
            List of stream entries.
        """
        key = next(iter(streams), "unknown")
        return await self._execute("XREAD", key, self._inner.xread(streams, count=count, block=block))

    async def xlen(self, name: str) -> int:
        """XLEN with timing.

        Returns:
            Entry count.
        """
        return await self._execute("XLEN", name, self._inner.xlen(name))

    async def xrevrange(self, name: str, max_id: str = "+", min_id: str = "-", count: int | None = None) -> list:
        """XREVRANGE with timing.

        Returns:
            Entries in reverse order.
        """
        return await self._execute("XREVRANGE", name, self._inner.xrevrange(name, max_id, min_id, count))

    # -- Sorted Set --

    async def zadd(self, name: str, mapping: dict[str, float]) -> int:
        """ZADD with timing.

        Returns:
            Number of new members added.
        """
        return await self._execute("ZADD", name, self._inner.zadd(name, mapping))

    async def zrangebyscore(self, name: str, min_score: float | str = "-inf", max_score: float | str = "+inf") -> list:
        """ZRANGEBYSCORE with timing.

        Returns:
            Members within score range.
        """
        return await self._execute("ZRANGEBYSCORE", name, self._inner.zrangebyscore(name, min_score, max_score))

    async def zrem(self, name: str, *members: str) -> int:
        """ZREM with timing.

        Returns:
            Number of members removed.
        """
        return await self._execute("ZREM", name, self._inner.zrem(name, *members))

    # -- Set --

    async def sadd(self, name: str, *values: str) -> int:
        """SADD with timing.

        Returns:
            Number of new members added.
        """
        return await self._execute("SADD", name, self._inner.sadd(name, *values))

    async def srem(self, name: str, *values: str) -> int:
        """SREM with timing.

        Returns:
            Number of members removed.
        """
        return await self._execute("SREM", name, self._inner.srem(name, *values))

    async def smembers(self, name: str) -> builtins.set:
        """SMEMBERS with timing.

        Returns:
            Set of all members.
        """
        return await self._execute("SMEMBERS", name, self._inner.smembers(name))

    # -- Key ops --

    async def delete(self, *names: str) -> int:
        """DELETE with timing.

        Returns:
            Number of keys deleted.
        """
        key = names[0] if names else "unknown"
        return await self._execute("DELETE", key, self._inner.delete(*names))

    async def expire(self, name: str, seconds: int) -> bool:
        """EXPIRE with timing.

        Returns:
            True if timeout was set.
        """
        return await self._execute("EXPIRE", name, self._inner.expire(name, seconds))

    async def ping(self) -> bool:
        """PING with timing.

        Returns:
            True if Redis responds.
        """
        return await self._execute("PING", "", self._inner.ping())

    async def publish(self, channel: str, message: str | bytes) -> int:
        """PUBLISH with timing.

        Returns:
            Number of subscribers reached.
        """
        return await self._execute("PUBLISH", channel, self._inner.publish(channel, message))

    # -- Lua --

    async def eval(self, script: str, keys: list[str], args: list[str]) -> Any:
        """EVAL with timing.

        Returns:
            Script return value.
        """
        key = keys[0] if keys else "unknown"
        return await self._execute("EVAL", key, self._inner.eval(script, keys, args))

    # -- Delegation (not instrumented per-command) --

    def pipeline(self) -> Any:
        """Delegate pipeline creation.

        Returns:
            Pipeline from the underlying client.
        """
        return self._inner.pipeline()

    def pubsub(self) -> Any:
        """Delegate pubsub creation.

        Returns:
            PubSub from the underlying client.
        """
        return self._inner.pubsub()

    async def close(self) -> None:
        """Close the underlying client."""
        await self._inner.close()

    @property
    def command_count(self) -> int:
        """Total commands executed."""
        return self._command_count

    @property
    def error_count(self) -> int:
        """Total commands that raised errors."""
        return self._error_count
