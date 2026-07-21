"""Redis connection pool manager with split read/write pools.

Uses two pools: ``_client`` for non-blocking commands (xadd, hset, etc.)
and ``_blocking_client`` for blocking commands (xread). This prevents
blocking readers from starving writers under high concurrency.

Created once at startup, passed via dependency injection, closed on shutdown.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import redis.asyncio as aioredis

from digitalkin.grpc_servers.utils.validators import GatewayValidator
from digitalkin.logger import logger
from digitalkin.models.settings.redis import get_redis_settings

if TYPE_CHECKING:
    import builtins


class RedisClient:  # noqa: PLR0904
    """Redis connection pool manager with split read/write pools.

    Attributes:
        url: The Redis connection URL (masked in logs).
    """

    _client: aioredis.Redis
    _blocking_client: aioredis.Redis
    url: str

    def __init__(self, redis_url: str) -> None:
        """Initialize Redis client with split pools.

        Pool sizing comes from ``RedisPoolSettings`` (env
        ``DIGITALKIN_REDIS_POOL_SIZE``, ``…POOL_SIZE_DEFAULT``, ``…POOL_SIZE_BLOCKING``).

        Args:
            redis_url: Redis connection URL. Falls back to ``RedisPoolSettings.url``.
        """
        pool = get_redis_settings().pool
        self.url = redis_url or pool.url.get_secret_value()

        default_size = pool.get_default_pool_size()
        blocking_size = pool.get_blocking_pool_size()

        self._client = aioredis.Redis.from_url(
            self.url,
            max_connections=default_size,
            decode_responses=False,
            health_check_interval=pool.health_check_interval,
        )
        self._blocking_client = aioredis.Redis.from_url(
            self.url,
            max_connections=blocking_size,
            decode_responses=False,
            health_check_interval=pool.health_check_interval,
        )

        logger.debug(
            "RedisClient created for %s (default_pool=%d, blocking_pool=%d)",
            GatewayValidator.mask_redis_url(self.url),
            default_size,
            blocking_size,
        )

    async def verify(self) -> bool:
        """Verify Redis is reachable by pinging both pools.

        Pings ``_client`` and ``_blocking_client`` concurrently so the first
        XADD and first XREAD don't each pay DNS+TCP+AUTH on cold pools.

        Timeout comes from ``RedisPoolSettings.health_check_timeout`` (env
        ``DIGITALKIN_REDIS_HEALTH_CHECK_TIMEOUT``).

        Returns:
            True if both pools responded, False if either is unreachable.
        """
        try:
            timeout = get_redis_settings().pool.health_check_timeout
            results = await asyncio.gather(
                asyncio.wait_for(self._client.ping(), timeout=timeout),
                asyncio.wait_for(self._blocking_client.ping(), timeout=timeout),
            )
            return all(results)
        except Exception:
            logger.warning("Redis health check failed for %s", GatewayValidator.mask_redis_url(self.url), exc_info=True)
            return False

    async def close(self) -> None:
        """Close both connection pools."""
        await self._client.aclose()
        await self._blocking_client.aclose()
        logger.debug("RedisClient closed")

    async def hset(self, name: str, mapping: dict[str, str | bytes]) -> int:
        """Set fields in a Redis hash.

        Args:
            name: Redis hash key.
            mapping: Field-value pairs to set.

        Returns:
            Number of fields added (not updated).
        """
        return await self._client.hset(name, mapping=mapping)  # type: ignore[arg-type]

    async def hgetall(self, name: str) -> dict[bytes, bytes]:
        """Get all fields and values in a Redis hash.

        Args:
            name: Redis hash key.

        Returns:
            All field-value pairs as bytes.
        """
        return await self._client.hgetall(name)  # type: ignore[return-value]

    async def publish(self, channel: str, message: str | bytes) -> int:
        """Publish a message to a Redis pub/sub channel.

        Args:
            channel: Channel name.
            message: Message payload.

        Returns:
            Number of subscribers that received the message.
        """
        return await self._client.publish(channel, message)

    async def delete(self, *names: str) -> int:
        """Delete one or more keys.

        Args:
            *names: Keys to delete.

        Returns:
            Number of keys deleted.
        """
        return await self._client.delete(*names)

    async def expire(self, name: str, seconds: int) -> bool:
        """Set a TTL on a key.

        Args:
            name: Key to expire.
            seconds: TTL in seconds.

        Returns:
            True if the timeout was set.
        """
        return await self._client.expire(name, seconds)

    async def ping(self) -> bool:
        """Health check.

        Returns:
            True if Redis responds.
        """
        return await self._client.ping()

    def pubsub(self) -> aioredis.client.PubSub:
        """Return a PubSub object for subscribe operations.

        Returns:
            PubSub instance bound to this client's connection pool.
        """
        return self._client.pubsub()

    def pipeline(self) -> aioredis.client.Pipeline:
        """Return a Pipeline for batched command execution.

        Returns:
            Pipeline instance that queues commands and executes them in one round-trip.
        """
        return self._client.pipeline()

    async def xadd(
        self,
        name: str,
        fields: dict[str, str | bytes],
        *,
        maxlen: int | None = None,
    ) -> bytes:
        """Append an entry to a Redis Stream.

        Args:
            name: Stream key.
            fields: Field-value pairs for the stream entry.
            maxlen: Optional cap on stream length (approximate trimming).

        Returns:
            The auto-generated entry ID.
        """
        kwargs: dict[str, Any] = {}
        if maxlen is not None:
            kwargs["maxlen"] = maxlen
            kwargs["approximate"] = True
        return await self._client.xadd(name, fields, **kwargs)  # type: ignore[arg-type, return-value]

    async def xread(
        self,
        streams: dict[str, str | bytes],
        *,
        count: int = 50,
        block: int = 1000,
    ) -> list:
        """Read entries from one or more Redis Streams.

        Uses the dedicated blocking pool so long-held connections don't
        starve non-blocking operations (xadd, hset, etc.).

        Args:
            streams: Mapping of stream_key to last-seen entry ID.
            count: Maximum entries per stream per call.
            block: Milliseconds to block waiting for new entries (0 = no block).

        Returns:
            List of [stream_key, [(entry_id, fields), ...]] pairs.
        """
        return await self._blocking_client.xread(streams, count=count, block=block)  # type: ignore[arg-type, return-value]

    async def xlen(self, name: str) -> int:
        """Get the number of entries in a Redis Stream.

        Args:
            name: Stream key.

        Returns:
            Number of entries.
        """
        return await self._client.xlen(name)

    async def xrevrange(
        self,
        name: str,
        max_id: str = "+",
        min_id: str = "-",
        count: int | None = None,
    ) -> list:
        """Read stream entries in reverse order (newest first).

        Args:
            name: Stream key.
            max_id: Upper bound entry ID (inclusive). Default "+" = newest.
            min_id: Lower bound entry ID (inclusive). Default "-" = oldest.
            count: Maximum entries to return.

        Returns:
            List of (entry_id, fields) tuples, newest first.
        """
        return await self._client.xrevrange(name, max=max_id, min=min_id, count=count)  # type: ignore[return-value]

    async def zadd(self, name: str, mapping: dict[str, float]) -> int:
        """Add members to a sorted set with scores.

        Args:
            name: Sorted set key.
            mapping: {member: score} pairs.

        Returns:
            Number of members added.
        """
        return await self._client.zadd(name, mapping)  # type: ignore[return-value]

    async def zrangebyscore(
        self,
        name: str,
        min_score: float | str = "-inf",
        max_score: float | str = "+inf",
    ) -> list[bytes]:
        """Get members with scores between min and max.

        Args:
            name: Sorted set key.
            min_score: Minimum score (inclusive).
            max_score: Maximum score (inclusive).

        Returns:
            List of member values.
        """
        return await self._client.zrangebyscore(name, min_score, max_score)  # type: ignore[return-value]

    async def zrem(self, name: str, *members: str) -> int:
        """Remove members from a sorted set.

        Args:
            name: Sorted set key.
            *members: Members to remove.

        Returns:
            Number of members removed.
        """
        return await self._client.zrem(name, *members)

    async def decr(self, name: str) -> int:
        """Decrement a key's integer value by 1.

        Args:
            name: Key to decrement.

        Returns:
            Value after decrement.
        """
        return await self._client.decr(name)

    async def eval(self, script: str, keys: list[str], args: list[str]) -> int | str | bytes | None:
        """Execute a Lua script on Redis.

        Args:
            script: Lua script source.
            keys: Redis keys accessed by the script (KEYS[]).
            args: Arguments passed to the script (ARGV[]).

        Returns:
            Script return value.
        """
        return await self._client.eval(script, len(keys), *keys, *args)

    async def get(self, name: str) -> bytes | None:
        """Get the value of a key.

        Args:
            name: Key name.

        Returns:
            Value as bytes, or None if key does not exist.
        """
        return await self._client.get(name)  # type: ignore[return-value]

    async def set(
        self,
        name: str,
        value: str | bytes,
        *,
        ex: int | None = None,
    ) -> bool:
        """Set a key to a value with optional TTL.

        Args:
            name: Key name.
            value: Value to set.
            ex: TTL in seconds.

        Returns:
            True if set successfully.
        """
        return await self._client.set(name, value, ex=ex)  # type: ignore[return-value]

    async def sadd(self, name: str, *values: str) -> int:
        """Add members to a Redis set.

        Args:
            name: Set key.
            *values: Members to add.

        Returns:
            Number of members added.
        """
        return await self._client.sadd(name, *values)

    async def srem(self, name: str, *values: str) -> int:
        """Remove members from a Redis set.

        Args:
            name: Set key.
            *values: Members to remove.

        Returns:
            Number of members removed.
        """
        return await self._client.srem(name, *values)

    async def smembers(self, name: str) -> builtins.set[bytes]:
        """Get all members of a Redis set.

        Args:
            name: Set key.

        Returns:
            Set of member values as bytes.
        """
        return await self._client.smembers(name)  # type: ignore[return-value]
