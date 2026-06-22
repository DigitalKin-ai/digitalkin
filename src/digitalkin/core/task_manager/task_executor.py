"""Task executor — runs module as a single asyncio task.

Signal cancellation: ``SharedRedisListener.dispatch_signal`` writes the
side channel (``pending_signal_action`` + ``last_signal_published_ns``)
on the ``TaskSession`` and calls ``task.cancel()``. The
``except asyncio.CancelledError`` block below reads
``pending_signal_action`` and invokes ``_handle_stop`` /
``_handle_cancel`` so ACK + audit fire on the live path.
"""

import asyncio
import datetime
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
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
    async def execute_task(  # noqa: C901
        task_id: str,
        mission_id: str,
        coro: Coroutine[Any, Any, None],
        session: TaskSession,
        *,
        on_finalize: Callable[[], Awaitable[None]] | None = None,
        stream_drain_timeout: float = 2.0,
    ) -> asyncio.Task[None]:
        """Execute a task as a single asyncio task.

        Cleanup is folded into the supervisor's ``finally`` so no separate
        fire-and-forget cleanup task is spawned (one fewer task per message).

        Args:
            task_id: Unique identifier for the task.
            mission_id: Mission identifier for the task.
            coro: The coroutine to execute (module.start(...)).
            session: TaskSession for state management.
            on_finalize: Optional async callable invoked at the end of the
                supervisor's ``finally`` after stream drain — typically
                ``manager._cleanup_task(task_id, mission_id)``.
            stream_drain_timeout: Max seconds to wait for ``session.stream_closed_event``
                before forcing finalize.

        Returns:
            The module task.
        """
        ids = {"mission_id": mission_id, "task_id": task_id}

        async def _run() -> None:
            session.started_at = datetime.datetime.now(datetime.timezone.utc)
            await session.set_status("running")

            try:
                await coro

                await session.set_status("completed")
                session.cancellation_reason = CancellationReason.COMPLETED
                logger.info("Task completed", extra=ids)

            except asyncio.CancelledError:
                action = session.pending_signal_action
                session.pending_signal_action = ""
                if action == "stop":
                    await session._handle_stop()  # noqa: SLF001
                else:
                    if session.cancellation_reason == CancellationReason.UNKNOWN:
                        session.cancellation_reason = CancellationReason.SIGNAL_SERVICE_CANCEL
                    await session._handle_cancel(session.cancellation_reason)  # noqa: SLF001
                logger.info("Task cancelled (%s)", session.cancellation_reason.value, extra=ids)
            except Exception as e:
                await session.set_status("failed")
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

                # Wait for stream drain then run finalize (slot release + session pop).
                if on_finalize is not None:
                    try:
                        await asyncio.wait_for(
                            session.stream_closed_event.wait(),
                            timeout=stream_drain_timeout,
                        )
                    except asyncio.TimeoutError:
                        logger.warning("Stream drain timeout, proceeding with cleanup", extra=ids)
                    try:
                        await on_finalize()
                    except Exception:
                        logger.exception("on_finalize raised — task may leak resources", extra=ids)

        task = asyncio.create_task(_run(), name=f"{task_id}_main")

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
