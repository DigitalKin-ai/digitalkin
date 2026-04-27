"""Task executor — runs module as a single asyncio task.

Signal cancellation handled by SharedRedisListener directly —
no per-task signal listener, no supervisor wrapper.
"""

import asyncio
import datetime
from collections.abc import Coroutine
from typing import Any

from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import CancellationReason


class TaskExecutor:
    """Runs module coroutine as a single asyncio task.

    Signal cancellation: SharedRedisListener calls task.cancel() directly
    when a cancel/stop signal arrives via Redis pub/sub. No supervisor,
    no signal listener task — just the module coroutine.
    """

    @staticmethod
    async def execute_task(
        task_id: str,
        mission_id: str,
        coro: Coroutine[Any, Any, None],
        session: TaskSession,
    ) -> asyncio.Task[None]:
        """Execute a task as a single asyncio task.

        Args:
            task_id: Unique identifier for the task.
            mission_id: Mission identifier for the task.
            coro: The coroutine to execute (module.start(...)).
            session: TaskSession for state management.

        Returns:
            The module task.
        """
        ids = {"mission_id": mission_id, "task_id": task_id}

        async def _run() -> None:
            session.started_at = datetime.datetime.now(datetime.timezone.utc)
            session.status = "running"

            try:
                await coro

                session.status = "completed"
                session.cancellation_reason = CancellationReason.COMPLETED
                logger.info("Task completed", extra=ids)

            except asyncio.CancelledError:
                session.status = "cancelled"
                if session.cancellation_reason == CancellationReason.UNKNOWN:
                    session.cancellation_reason = CancellationReason.SIGNAL_SERVICE_CANCEL
                logger.info("Task cancelled (%s)", session.cancellation_reason.value, extra=ids)
            except Exception as e:
                session.status = "failed"
                session.record_exception(e)
                logger.exception("Task failed: '%s'", task_id, extra=ids)
            finally:
                session.completed_at = datetime.datetime.now(datetime.timezone.utc)
                session.close_stream()

                duration = (
                    (session.completed_at - session.started_at).total_seconds()
                    if session.started_at and session.completed_at
                    else None
                )
                logger.info(
                    "Task done: '%s' status=%s duration=%.2fs",
                    task_id,
                    session.status,
                    duration or 0,
                    extra=ids,
                )

        task = asyncio.create_task(_run(), name=f"{task_id}_main")

        # Register task with SharedRedisListener for direct cancellation via Redis pub/sub
        if session.signal_service is not None:
            try:
                from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

                for listener in SharedRedisListener._instances.values():  # noqa: SLF001
                    listener.register_task(task_id, task)
                    break
            except Exception:
                logger.debug("Signal registration skipped for task %s", task_id)

        return task
