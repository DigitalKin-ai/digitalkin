"""Stream registry: per-instance session tracking + dial-back asyncio task supervision.

Sessions are tracked in a local bounded LRU cache. Session lifecycle is
bound to the dial-back asyncio task: the task's ``finally`` calls
``unregister`` on normal completion; if it doesn't run (process killed,
``BaseException`` propagated past finally), the task done-callback
force-unregisters as a backstop.

No Redis I/O on register/unregister — the gateway is fully local for
session lifecycle. The previous Redis session-state mirror had no readers
once the heartbeat reaper was retired.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from digitalkin.core.resilience.task_supervisor import log_unhandled
from digitalkin.logger import logger
from digitalkin.models.settings.gateway import GatewaySettings

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.grpc_servers.stream_session import StreamSession


class StreamRegistry:
    """Tracks active stream sessions per-instance + supervises spawned tasks.

    Local dict is a bounded LRU cache of sessions with active BiDi
    connections on this gateway instance. Capacity is enforced
    process-locally against ``max_streams``.
    """

    _local_cache: OrderedDict[str, StreamSession]
    _max_local: int
    _max_streams: int
    _monitored_tasks: set[asyncio.Task[Any]]

    def __init__(
        self,
        redis_client: RedisClient | None = None,  # noqa: ARG002 — kept for back-compat with callers
        max_streams: int | None = None,
        max_local: int | None = None,
    ) -> None:
        """Initialize the stream registry.

        Args:
            redis_client: Unused (kept for back-compat); the registry no
                longer touches Redis on register/unregister.
            max_streams: Maximum concurrent streams on this instance.
                Defaults to ``GatewaySettings.max_streams``.
            max_local: Maximum sessions cached locally on this instance.
                Defaults to ``GatewaySettings.max_local_cache``.
        """
        settings = GatewaySettings()
        self._local_cache = OrderedDict()
        self._max_local = max_local if max_local is not None else settings.max_local_cache
        self._max_streams = max_streams if max_streams is not None else settings.max_streams
        self._monitored_tasks = set()

    @property
    def active_count(self) -> int:
        """Number of locally cached sessions."""
        return len(self._local_cache)

    async def register(
        self,
        session: StreamSession,
        setup_id: str = "",  # noqa: ARG002 — accepted for back-compat with callers
        mission_id: str = "",  # noqa: ARG002 — accepted for back-compat with callers
    ) -> bool:
        """Register a new session. Capacity is enforced process-locally.

        Args:
            session: The stream session to register.
            setup_id: Accepted for back-compat; no longer persisted to Redis.
            mission_id: Accepted for back-compat; no longer persisted to Redis.

        Returns:
            True if registered, False if at capacity (this instance).
        """
        # Process-local capacity check.
        if len(self._local_cache) >= self._max_streams:
            return False

        # LRU cache — evict oldest if past max_local (separate from
        # max_streams which gates new admissions above).
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
        """Unregister a session from the local cache.

        Args:
            task_id: Session to remove.

        Returns:
            The removed session, or None if not found locally.
        """
        session = self._local_cache.pop(task_id, None)
        if session is not None:
            logger.debug("StreamRegistry.unregister: task_id=%s local=%d", task_id, len(self._local_cache))
        return session

    def monitor_task(self, task: asyncio.Task[Any]) -> None:
        """Track a fire-and-forget asyncio task for the reaper to supervise.

        The reaper has one job: monitor tasks and clean them. Calling
        ``monitor_task`` enrolls ``task`` in that watch:

        - The registry holds a strong reference, so the task can't be
          garbage-collected mid-flight.
        - When the task finishes, the done-callback runs:
          cancellation and clean exits are silent; an unhandled exception
          is logged at error level. This replaces asyncio's opaque
          ``Task exception was never retrieved`` warning with a real,
          actionable log line tagged with the task name.
        - On ``shutdown()``, every still-running monitored task is
          cancelled and awaited.

        Args:
            task: An ``asyncio.Task`` to supervise.
        """
        self._monitored_tasks.add(task)
        task.add_done_callback(self._on_monitored_task_done)

    def _on_monitored_task_done(self, task: asyncio.Task[Any]) -> None:
        """Done-callback: log exceptions via shared helper + reap local zombies.

        For tasks named ``dial_consumer_<task_id>``, if the matching session
        is still in ``_local_cache``, the dial-back's ``finally`` didn't run
        (e.g., ``BaseException`` like ``SystemExit`` propagated past it).
        Schedule an async unregister + teardown as a backstop.
        """
        self._monitored_tasks.discard(task)
        log_unhandled(task)

        # Local zombie sweep — replaces the old heartbeat-based reaper loop.
        name = task.get_name()
        if name.startswith("dial_consumer_"):
            task_id = name[len("dial_consumer_") :]
            if task_id in self._local_cache:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    return  # No loop — registry is shutting down.
                loop.create_task(self._reap_local(task_id), name=f"reap_{task_id}")

    async def _reap_local(self, task_id: str) -> None:
        """Force-unregister a session whose dial-back finished without cleanup."""
        session = await self.unregister(task_id)
        if session is not None:
            logger.warning(
                "Reaping local zombie: dial-back finished without unregister, task_id=%s",
                task_id,
            )
            await session.teardown()

    async def shutdown(self) -> None:
        """Cancel monitored tasks then tear down any remaining sessions.

        Order matters:

        1. Cancel every monitored asyncio task. Their ``finally`` blocks run
           — including ``_dial_consumer.finally``, which calls
           ``unregister(task_id)`` — so most sessions clean themselves up.
        2. Sweep any sessions left in ``_local_cache`` defensively.
        """
        for task in list(self._monitored_tasks):
            if not task.done():
                task.cancel()
        if self._monitored_tasks:
            await asyncio.gather(*self._monitored_tasks, return_exceptions=True)
            self._monitored_tasks.clear()

        for sid in list(self._local_cache):
            session = await self.unregister(sid)
            if session is not None:
                await session.teardown()
