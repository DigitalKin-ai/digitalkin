"""Redis-backed stream registry with capacity enforcement and zombie reaping.

Sessions are authoritative in Redis (``gateway:session:{task_id}`` hashes).
A local bounded LRU cache holds hot sessions (active BiDi connections on
this instance). Capacity is enforced cluster-wide via a Lua atomic
increment on ``gateway:session_count``. Zombie detection uses a Redis
sorted set ``gateway:heartbeats`` for O(log N) range queries.

Redis is expected in production. Without it, the registry operates in
local-only mode (dev/test) with a WARNING — cluster-wide capacity and
heartbeat-based reaping are disabled.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import OrderedDict
from typing import TYPE_CHECKING

from digitalkin.grpc_servers.gateway_constants import (
    HEARTBEAT_TTL_S,
    MAX_LOCAL_CACHE,
    MAX_STREAMS,
    REAPER_INTERVAL_S,
    REDIS_KEY_HEARTBEATS,
    REDIS_KEY_SESSION_COUNT,
    SESSION_STATE_TTL_S,
)
from digitalkin.logger import logger

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.grpc_servers.stream_session import StreamSession

# Lua: atomic register — capacity check + heartbeat + session state in 1 RTT.
# KEYS: [1]=count_key, [2]=hb_key, [3]=session_key (optional, empty string to skip)
# ARGV: [1]=max, [2]=task_id, [3]=now, [4]=setup_id, [5]=mission_id, [6]=session_ttl
# Returns 1 if registered, 0 if at capacity.
_LUA_REGISTER = f"""
local count_key = KEYS[1]
local hb_key = KEYS[2]
local session_key = KEYS[3]
local max = tonumber(ARGV[1])
local task_id = ARGV[2]
local now = tonumber(ARGV[3])
local current = tonumber(redis.call('GET', count_key) or '0')
if current >= max then
    return 0
end
redis.call('INCR', count_key)
redis.call('EXPIRE', count_key, {SESSION_STATE_TTL_S})
redis.call('ZADD', hb_key, now, task_id)
if session_key ~= '' then
    redis.call('HSET', session_key, 'status', 'starting', 'setup_id', ARGV[4], 'mission_id', ARGV[5])
    redis.call('EXPIRE', session_key, tonumber(ARGV[6]))
end
return 1
"""


class StreamRegistry:
    """Tracks active stream sessions with Redis-backed state.

    Local dict is a bounded LRU cache of sessions with active BiDi
    connections on this gateway instance. Redis is the source of truth
    for capacity and heartbeats.
    """

    _local_cache: OrderedDict[str, StreamSession]
    _max_local: int
    _max_streams: int
    _heartbeat_ttl: float
    _reaper_interval: float
    _reaper_task: asyncio.Task[None] | None
    _redis_client: RedisClient

    @staticmethod
    def session_key(task_id: str) -> str:
        """Redis hash key for session metadata.

        Returns:
            Key in the format ``gateway:session:{task_id}``.
        """
        return f"gateway:session:{task_id}"

    def __init__(
        self,
        redis_client: RedisClient,
        max_streams: int = MAX_STREAMS,
        max_local: int = MAX_LOCAL_CACHE,
        heartbeat_ttl: float = HEARTBEAT_TTL_S,
        reaper_interval: float = REAPER_INTERVAL_S,
    ) -> None:
        """Initialize the stream registry.

        Args:
            max_streams: Cluster-wide maximum concurrent streams.
            max_local: Maximum sessions cached locally on this instance.
            heartbeat_ttl: Seconds before a session is considered zombie.
            reaper_interval: Seconds between reaper scans.
            redis_client: Redis client for distributed state.
        """
        self._local_cache = OrderedDict()
        self._max_local = max_local
        self._max_streams = max_streams
        self._heartbeat_ttl = heartbeat_ttl
        self._reaper_interval = reaper_interval
        self._reaper_task = None
        self._redis_client = redis_client

    @property
    def active_count(self) -> int:
        """Number of locally cached sessions."""
        return len(self._local_cache)

    async def register(
        self,
        session: StreamSession,
        setup_id: str = "",
        mission_id: str = "",
    ) -> bool:
        """Register a new session with cluster-wide capacity enforcement.

        When ``setup_id`` and ``mission_id`` are provided, session state
        (HSET + EXPIRE) is written inside the same Lua script — 1 Redis
        round-trip instead of 3.

        Args:
            session: The stream session to register.
            setup_id: Setup ID to store in session state (optional).
            mission_id: Mission ID to store in session state (optional).

        Returns:
            True if registered, False if at capacity.
        """
        sess_key = self.session_key(session.task_id) if setup_id else ""
        try:
            allowed = await self._redis_client.eval(
                _LUA_REGISTER,
                [REDIS_KEY_SESSION_COUNT, REDIS_KEY_HEARTBEATS, sess_key],
                [
                    str(self._max_streams),
                    session.task_id,
                    str(time.time()),
                    setup_id,
                    mission_id,
                    str(SESSION_STATE_TTL_S),
                ],
            )
        except Exception:
            logger.exception("Redis capacity check failed: task_id=%s", session.task_id)
            return False

        if not allowed:
            return False

        # LRU cache — evict oldest if full (don't unregister from Redis,
        # reaper handles that; session may be active on another instance)
        if len(self._local_cache) >= self._max_local:
            self._local_cache.popitem(last=False)

        self._local_cache[session.task_id] = session
        self._local_cache.move_to_end(session.task_id)
        logger.debug("StreamRegistry.register: task_id=%s local=%d", session.task_id, len(self._local_cache))
        return True

    def get(self, task_id: str) -> StreamSession | None:
        """Get a session from local cache.

        Args:
            task_id: Session identifier.

        Returns:
            The session, or None if not cached locally.
        """
        session = self._local_cache.get(task_id)
        if session is not None:
            self._local_cache.move_to_end(task_id)
        return session

    async def unregister(self, task_id: str) -> StreamSession | None:
        """Unregister a session and decrement cluster counter.

        Args:
            task_id: Session to remove.

        Returns:
            The removed session, or None if not found locally.
        """
        session = self._local_cache.pop(task_id, None)

        try:
            pipe = self._redis_client.pipeline()
            pipe.decr(REDIS_KEY_SESSION_COUNT)
            pipe.zrem(REDIS_KEY_HEARTBEATS, task_id)
            pipe.delete(self.session_key(task_id))
            await pipe.execute()
        except Exception:
            logger.exception("Redis unregister pipeline failed: task_id=%s", task_id)

        if session is not None:
            logger.debug("StreamRegistry.unregister: task_id=%s local=%d", task_id, len(self._local_cache))
        return session

    async def touch_heartbeat(self, task_id: str) -> None:
        """Update the heartbeat timestamp for a session.

        Args:
            task_id: Session to touch.
        """
        try:
            await self._redis_client.zadd(
                REDIS_KEY_HEARTBEATS,
                {task_id: time.time()},
            )
        except Exception:
            logger.debug("Heartbeat update failed: task_id=%s", task_id)

    async def start_reaper(self) -> None:
        """Start the background zombie session reaper."""
        if self._reaper_task is None or self._reaper_task.done():
            self._reaper_task = asyncio.create_task(self._reaper_loop(), name="stream_reaper")

    async def _reaper_loop(self) -> None:
        """Scan for zombie sessions using Redis sorted set range query."""
        try:
            while True:
                await asyncio.sleep(self._reaper_interval)

                try:
                    cutoff = time.time() - self._heartbeat_ttl
                    zombies = await self._redis_client.zrangebyscore(
                        REDIS_KEY_HEARTBEATS,
                        "-inf",
                        cutoff,
                    )
                    for raw_id in zombies:
                        sid = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
                        session = await self.unregister(sid)
                        if session is not None:
                            logger.warning("Reaping zombie session: task_id=%s", sid)
                            await session.teardown()
                except Exception:
                    logger.exception("Reaper scan failed")

        except asyncio.CancelledError:
            pass

    async def shutdown(self) -> None:
        """Stop the reaper and tear down all sessions."""
        if self._reaper_task is not None and not self._reaper_task.done():
            self._reaper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reaper_task

        for sid in list(self._local_cache):
            session = await self.unregister(sid)
            if session is not None:
                await session.teardown()
