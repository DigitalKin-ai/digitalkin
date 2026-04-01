"""Session TTL reaper for SingleJobManager.

Catches leaked sessions that ``_deferred_cleanup`` missed — for example
when the supervisor task completes but the done callback fails to fire,
or when a module hangs without producing output.

Scans ``tasks_sessions`` periodically and marks sessions as zombie if
they have no recent activity (no queue writes, no status change).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from typing import TYPE_CHECKING

from digitalkin.logger import logger

if TYPE_CHECKING:
    from digitalkin.core.task_manager.base_task_manager import BaseTaskManager


class SessionReaper:
    """Background task that reaps leaked sessions in local mode.

    A session is considered zombie if:
    - Its supervisor task is done (or missing from tasks dict)
    - AND it has been in tasks_sessions for longer than ``ttl`` seconds

    This is a safety net — the primary cleanup path is ``_deferred_cleanup``.
    """

    _task_manager: BaseTaskManager
    _ttl: float
    _interval: float
    _task: asyncio.Task[None] | None
    _running: bool

    def __init__(
        self,
        task_manager: BaseTaskManager,
        ttl: float = float(os.environ.get("DIGITALKIN_SESSION_REAPER_TTL", "300")),
        interval: float = float(os.environ.get("DIGITALKIN_SESSION_REAPER_INTERVAL", "60")),
    ) -> None:
        """Initialize the session reaper.

        Args:
            task_manager: The task manager whose sessions to monitor.
            ttl: Seconds a session can be idle before reaping.
            interval: Seconds between reaper scans.
        """
        self._task_manager = task_manager
        self._ttl = ttl
        self._interval = interval
        self._task = None
        self._running = False

    async def start(self) -> None:
        """Start the background reaper task."""
        if self._task is not None and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._scan_loop(), name="session_reaper")
        logger.info("SessionReaper started (ttl=%.0fs, interval=%.0fs)", self._ttl, self._interval)

    async def stop(self) -> None:
        """Stop the reaper task."""
        self._running = False
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("SessionReaper stopped")

    async def _scan_loop(self) -> None:
        """Periodically scan for zombie sessions."""
        try:
            while self._running:
                await asyncio.sleep(self._interval)
                if not self._running:
                    break
                await self._scan_once()
        except asyncio.CancelledError:
            pass

    async def _scan_once(self) -> None:
        """Single scan: find and reap zombie sessions."""
        zombies: list[tuple[str, str]] = []

        for task_id, session in list(self._task_manager.tasks_sessions.items()):
            # Session still has a running supervisor → not zombie
            supervisor = self._task_manager.tasks.get(task_id)
            if supervisor is not None and not supervisor.done():
                continue

            # Supervisor is done or missing — check age
            if session.completed_at is not None:
                # Already completed but not cleaned up — zombie
                zombies.append((task_id, session.mission_id))
            elif session.created_at is not None:
                from datetime import datetime, timezone

                age = (datetime.now(tz=timezone.utc) - session.created_at).total_seconds()
                if age > self._ttl:
                    zombies.append((task_id, session.mission_id))

        for task_id, mission_id in zombies:
            logger.warning("Reaping zombie session: task_id=%s mission_id=%s", task_id, mission_id)
            try:
                await self._task_manager._cleanup_task(task_id, mission_id)  # noqa: SLF001
            except Exception:
                logger.exception("Failed to reap session: task_id=%s", task_id)

        if zombies:
            logger.info("SessionReaper: reaped %d zombie sessions", len(zombies))
