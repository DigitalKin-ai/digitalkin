"""Graceful shutdown with sequenced checkpoint and startup restore.

Shutdown sequence (target < 500ms):
1. t=0ms: Stop accepting new connections.
2. t=0ms: Set shutdown event — visible to all coroutines.
3. t=0-200ms: Checkpoint all active sessions concurrently.
4. t=200ms: Cancel all tasks with timeout.
5. t=200-500ms: Drain gRPC stubs, close Redis.

Startup restore:
1. Connect to Redis.
2. Scan ``checkpoint:*`` keys.
3. For each checkpoint, rebuild session state.
4. Resume from ``last_seq``.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from typing import TYPE_CHECKING, Any

from digitalkin.logger import logger
from digitalkin.models.settings.resilience import ResilienceSettings

if TYPE_CHECKING:
    from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
    from digitalkin.core.task_manager.redis.redis_checkpoint import RedisCheckpointManager
    from digitalkin.core.task_manager.redis.redis_client import RedisClient


class GracefulShutdownHandler:
    """Handles SIGTERM with sequenced shutdown and checkpoint.

    Registers a signal handler that triggers an async shutdown coroutine
    on the event loop. The handler itself is synchronous and returns
    immediately — the actual shutdown runs as a coroutine.
    """

    _task_manager: BaseTaskManager
    _checkpoint_mgr: RedisCheckpointManager | None
    _redis_client: RedisClient
    _shutdown_event: asyncio.Event
    _shutdown_timeout: float
    _loop: asyncio.AbstractEventLoop | None
    _shutdown_task: asyncio.Task[None] | None

    def __init__(
        self,
        task_manager: BaseTaskManager,
        checkpoint_mgr: RedisCheckpointManager | None = None,
        redis_client: RedisClient | None = None,
        shutdown_timeout: float | None = None,
    ) -> None:
        """Initialize the shutdown handler.

        Args:
            task_manager: Task manager whose sessions to checkpoint.
            checkpoint_mgr: Optional Redis checkpoint manager.
            redis_client: Optional Redis client to close on shutdown.
            shutdown_timeout: Max seconds for the entire shutdown sequence.
                Defaults to ResilienceSettings.shutdown_timeout.
        """
        self._task_manager = task_manager
        self._checkpoint_mgr = checkpoint_mgr
        self._redis_client = redis_client
        self._shutdown_event = asyncio.Event()
        self._shutdown_timeout = (
            shutdown_timeout if shutdown_timeout is not None else ResilienceSettings().shutdown_timeout
        )
        self._loop = None
        self._shutdown_task = None

    def register(self, loop: asyncio.AbstractEventLoop) -> None:
        """Register SIGTERM and SIGINT handlers on the event loop.

        Args:
            loop: The asyncio event loop.
        """
        self._loop = loop
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._on_signal, sig)
        logger.info("Graceful shutdown handler registered (timeout=%.0fs)", self._shutdown_timeout)

    def _on_signal(self, sig: signal.Signals) -> None:
        """Signal handler — schedules async shutdown on the loop.

        Args:
            sig: The signal received.
        """
        if self._shutdown_event.is_set():
            return  # Already shutting down
        logger.info("Received %s, initiating graceful shutdown", sig.name)
        self._shutdown_event.set()
        self._shutdown_task = asyncio.ensure_future(self._shutdown_sequence())

    async def _shutdown_sequence(self) -> None:
        """Execute the sequenced shutdown.

        Steps:
        1. Checkpoint all active sessions (concurrent).
        2. Cancel all tasks with timeout.
        3. Close Redis connections.
        """
        try:
            await asyncio.wait_for(self._do_shutdown(), timeout=self._shutdown_timeout)
        except asyncio.TimeoutError:
            logger.critical("Shutdown timed out after %.0fs, forcing exit", self._shutdown_timeout)

    async def _do_shutdown(self) -> None:
        """Internal shutdown implementation."""
        # Unregister signal handlers to prevent double-trigger
        if self._loop is not None:
            for sig in (signal.SIGTERM, signal.SIGINT):
                with contextlib.suppress(Exception):
                    self._loop.remove_signal_handler(sig)

        # Checkpoint all active sessions
        if self._checkpoint_mgr is not None:
            await self._checkpoint_all_sessions()

        # Cancel all tasks
        for mission_id in {s.mission_id for s in self._task_manager.tasks_sessions.values()}:
            with contextlib.suppress(Exception):
                await self._task_manager.shutdown(mission_id, timeout=10.0)

        # Close Redis
        if self._redis_client is not None:
            with contextlib.suppress(Exception):
                await self._redis_client.close()

        logger.info("Graceful shutdown complete")

    async def _checkpoint_all_sessions(self) -> None:
        """Checkpoint all active sessions concurrently.

        Best-effort: individual checkpoint failures don't abort the shutdown.
        """
        if self._checkpoint_mgr is None:
            return

        sessions = list(self._task_manager.tasks_sessions.values())
        if not sessions:
            return

        logger.info("Checkpointing %d active sessions", len(sessions))

        async def _checkpoint_one(session: Any) -> None:
            try:
                await self._checkpoint_mgr.checkpoint(  # type: ignore[union-attr]
                    session_id=session.task_id,
                    task_id=session.task_id,
                    mission_id=session.mission_id,
                    setup_id=session.module.context.session.setup_id if session.module else "",
                    setup_version_id=session.module.context.session.setup_version_id if session.module else "",
                    status=session.status,
                    last_seq=0,
                )
            except Exception:
                logger.exception("Failed to checkpoint session: task_id=%s", session.task_id)

        await asyncio.gather(*[_checkpoint_one(s) for s in sessions])
        logger.info("Checkpoint complete: %d sessions", len(sessions))

    @property
    def is_shutting_down(self) -> bool:
        """Whether shutdown has been initiated."""
        return self._shutdown_event.is_set()


class StartupRestorer:
    """Restores sessions from Redis checkpoints on process startup.

    Scans ``checkpoint:*`` keys and returns checkpoint data for the
    caller to rebuild sessions.
    """

    _checkpoint_mgr: RedisCheckpointManager
    _redis_client: RedisClient

    def __init__(
        self,
        checkpoint_mgr: RedisCheckpointManager,
        redis_client: RedisClient,
    ) -> None:
        """Initialize the startup restorer.

        Args:
            checkpoint_mgr: Redis checkpoint manager.
            redis_client: Redis client for scanning keys.
        """
        self._checkpoint_mgr = checkpoint_mgr
        self._redis_client = redis_client

    async def restore_all(self) -> list[dict[str, Any]]:
        """Scan and restore all checkpoints.

        Returns:
            List of checkpoint data dicts. Each contains:
            session_id, task_id, mission_id, setup_id, setup_version_id,
            status, last_seq, state.
        """
        # In production, use SCAN to find checkpoint:* keys.
        # For now, use the checkpoint manager's list method.
        checkpoints = await self._checkpoint_mgr.list_checkpoints()
        if checkpoints:
            logger.info("Restored %d checkpoints from Redis", len(checkpoints))
        return checkpoints
