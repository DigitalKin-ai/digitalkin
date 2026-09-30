"""Task executor — runs module as a single asyncio task.

Signal cancellation: ``SharedRedisListener.dispatch_signal`` writes the
side channel (``pending_signal_action`` + ``last_signal_published_ns``)
on the ``TaskSession`` and calls ``task.cancel()``. The
``except asyncio.CancelledError`` block below reads
``pending_signal_action`` and invokes ``_handle_cancel``; ``stop`` is a
hard cancel that only changes the recorded reason.
"""

import asyncio
import datetime
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

from digitalkin.core.resilience.task_supervisor import log_unhandled
from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import CancellationReason


class TaskExecutor:
    """Runs module coroutine as a single asyncio task.

    Signal cancellation: SharedRedisListener calls task.cancel() directly
    when a cancel/stop signal arrives via Redis pub/sub.
    """

    _backstops: set[asyncio.Task[None]]

    def __init__(self) -> None:
        """Initialize the executor."""
        self._backstops = set()

    async def execute_task(  # noqa: C901, PLR0915
        self,
        task_id: str,
        mission_id: str,
        coro: Coroutine[Any, Any, None],
        session: TaskSession,
        *,
        on_finalize: Callable[[], Awaitable[None]] | None = None,
    ) -> asyncio.Task[None]:
        """Execute a task as a single asyncio task.

        Cleanup is folded into the task's ``finally``; a done-callback backstop
        runs it when the task is cancelled before its first step.

        Args:
            task_id: Unique identifier for the task.
            mission_id: Mission identifier for the task.
            coro: The coroutine to execute (module.start(...)).
            session: TaskSession for state management.
            on_finalize: Optional async callable invoked at the end of the
                task's ``finally`` — typically ``manager._cleanup_task(task_id, mission_id)``.

        Returns:
            The module task.
        """
        ids = {"mission_id": mission_id, "task_id": task_id}
        started = False

        async def _cancelled() -> None:
            action = session.pending_signal_action
            session.pending_signal_action = ""
            if action == "stop":
                await session._handle_cancel(CancellationReason.SIGNAL_SERVICE_STOP)  # noqa: SLF001
                return
            if session.cancellation_reason == CancellationReason.UNKNOWN:
                session.cancellation_reason = CancellationReason.SIGNAL_SERVICE_CANCEL
            await session._handle_cancel(session.cancellation_reason)  # noqa: SLF001

        async def _finalize() -> None:
            if on_finalize is None:
                return
            try:
                await on_finalize()
            except Exception:
                logger.exception("on_finalize raised — task may leak resources", extra=ids)

        async def _run() -> None:
            nonlocal started
            started = True
            session.started_at = datetime.datetime.now(datetime.timezone.utc)
            try:
                await session.set_status("running")
                await coro

                await session.set_status("completed")
                session.cancellation_reason = CancellationReason.COMPLETED
                logger.info("Task completed", extra=ids)

            except asyncio.CancelledError:
                await _cancelled()
                logger.info("Task cancelled (%s)", session.cancellation_reason.value, extra=ids)
            except Exception as e:
                await session.set_status("failed")
                session.record_exception(e)
                logger.exception("Task failed: '%s'", task_id, extra=ids)
            finally:
                coro.close()
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
                await _finalize()

        async def _early_cancel() -> None:
            session.completed_at = datetime.datetime.now(datetime.timezone.utc)
            await _cancelled()
            session.close_stream()
            await _finalize()

        def _backstop(_: asyncio.Task[None]) -> None:
            if started:
                return
            coro.close()
            # TODO(validate): CANCEL-EARLY a task cancelled before its first step is finalized
            logger.warning("[VALIDATE CANCEL-EARLY] task cancelled before its first step; finalizing", extra=ids)
            fin = asyncio.create_task(_early_cancel(), name=f"{task_id}_backstop")
            self._backstops.add(fin)
            fin.add_done_callback(self._backstops.discard)
            fin.add_done_callback(log_unhandled)

        task = asyncio.create_task(_run(), name=f"{task_id}_main")
        task.add_done_callback(_backstop)

        if session.signal_service is not None:
            listener = SharedRedisListener.singleton_or_none()
            if listener is None:
                logger.warning(
                    "No SharedRedisListener instance — signals disabled for task_id=%s",
                    task_id,
                    extra=ids,
                )
            else:
                try:
                    listener.register(task_id, session, task)
                except Exception:
                    logger.warning(
                        "Signal registration failed — signals disabled for task_id=%s",
                        task_id,
                        extra=ids,
                        exc_info=True,
                    )

        return task
