"""Task executor for running tasks with full lifecycle management."""

import asyncio
import contextlib
import datetime
from collections.abc import Coroutine
from typing import Any

from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import (
    CancellationReason,
    SignalMessage,
    SignalType,
)


class TaskExecutor:
    """Executes tasks with the supervisor pattern (main + signal listener).

    Pure execution logic - no task registry or orchestration.
    Used by workers to run distributed tasks or by TaskManager for local execution.
    """

    @staticmethod
    async def execute_task(  # noqa: C901, PLR0915 — supervisor pattern
        task_id: str,
        mission_id: str,
        coro: Coroutine[Any, Any, None],
        session: TaskSession,
    ) -> asyncio.Task[None]:
        """Execute a task using the supervisor pattern.

        Runs two concurrent sub-tasks:
        - Main coroutine (the actual work)
        - Signal listener (watches for stop/cancel signals)

        The first task to complete determines the outcome.

        Args:
            task_id: Unique identifier for the task
            mission_id: Mission identifier for the task
            coro: The coroutine to execute (module.start(...))
            session: TaskSession for state management

        Returns:
            asyncio.Task: The supervisor task managing the lifecycle
        """

        async def signal_wrapper() -> None:
            """Send initial signal and listen for signals."""
            try:
                # Send start signal via signal service
                await session.signal_service.send_signal(
                    task_id,
                    SignalMessage(
                        task_id=task_id,
                        mission_id=mission_id,
                        setup_id=session.setup_id,
                        setup_version_id=session.setup_version_id,
                        action=SignalType.START,
                    ).model_dump(exclude_none=True),
                )
                logger.info(
                    "Task start signal sent",
                    extra={"mission_id": mission_id, "task_id": task_id},
                )
                # Start listening for signals
                await session.listen_signals()

            except asyncio.CancelledError:
                logger.info("Signal listener cancelled", extra={"mission_id": mission_id, "task_id": task_id})
            finally:
                with contextlib.suppress(Exception):
                    await session.signal_service.send_signal(
                        task_id,
                        SignalMessage(
                            task_id=task_id,
                            mission_id=mission_id,
                            setup_id=session.setup_id,
                            setup_version_id=session.setup_version_id,
                            action=SignalType.STOP,
                            cancellation_reason=session.cancellation_reason,
                            error_message=session._last_exception,  # noqa: SLF001
                            exception_traceback=session._last_traceback,  # noqa: SLF001
                        ).model_dump(exclude_none=True),
                    )
                logger.info("Signal listener ended", extra={"mission_id": mission_id, "task_id": task_id})

        async def supervisor() -> None:  # noqa: C901, PLR0915
            """Supervise the two concurrent tasks and handle outcomes.

            Raises:
                asyncio.CancelledError: If the supervisor task is cancelled.
            """
            session.started_at = datetime.datetime.now(datetime.timezone.utc)
            session.status = "running"

            main_task = None
            sig_task = None
            cleanup_reason = CancellationReason.UNKNOWN

            try:
                main_task = asyncio.create_task(coro, name=f"{task_id}_main")
                sig_task = asyncio.create_task(signal_wrapper(), name=f"{task_id}_listener")
                done, pending = await asyncio.wait(
                    [main_task, sig_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Determine cleanup reason based on which task completed first
                completed = next(iter(done))

                if completed is main_task:
                    cleanup_reason = CancellationReason.SUCCESS_CLEANUP
                elif completed is sig_task:
                    cleanup_reason = CancellationReason.SIGNAL_SERVICE_CANCEL

                # Signal stream to close
                session.close_stream()

                # Cancel pending tasks with proper reason logging
                if pending:
                    await asyncio.sleep(0.01)  # Allow one event loop cycle

                    pending_names = [t.get_name() for t in pending]
                    logger.debug(
                        "Cancelling pending tasks: %s, reason: %s",
                        pending_names,
                        cleanup_reason.value,
                        extra={
                            "mission_id": mission_id,
                            "task_id": task_id,
                            "pending_tasks": pending_names,
                            "cancellation_reason": cleanup_reason.value,
                        },
                    )
                    for t in pending:
                        t.cancel()

                # Propagate exception/result from the finished task
                await completed

                # Determine final status based on which task completed
                if completed is main_task:
                    session.status = "completed"
                    session.cancellation_reason = CancellationReason.COMPLETED
                    logger.info(
                        "Main task completed successfully",
                        extra={"mission_id": mission_id, "task_id": task_id},
                    )
                elif completed is sig_task:
                    session.status = "cancelled"
                    session.cancellation_reason = CancellationReason.SIGNAL_SERVICE_CANCEL
                    logger.info(
                        "Task cancelled via signal service",
                        extra={
                            "mission_id": mission_id,
                            "task_id": task_id,
                            "cancellation_reason": CancellationReason.SIGNAL_SERVICE_CANCEL.value,
                        },
                    )

            except asyncio.CancelledError:
                session.status = "cancelled"
                logger.info(
                    "Task cancelled externally: '%s', reason: %s",
                    task_id,
                    session.cancellation_reason.value,
                    extra={
                        "mission_id": mission_id,
                        "task_id": task_id,
                        "cancellation_reason": session.cancellation_reason.value,
                    },
                )
                cleanup_reason = CancellationReason.FAILURE_CLEANUP
                raise
            except Exception as e:
                session.status = "failed"
                cleanup_reason = CancellationReason.FAILURE_CLEANUP
                session.record_exception(e)
                logger.exception(
                    "Task failed with exception: '%s'",
                    task_id,
                    extra={"mission_id": mission_id, "task_id": task_id},
                )
                raise
            finally:
                session.completed_at = datetime.datetime.now(datetime.timezone.utc)
                # Ensure all tasks are cleaned up with proper reason
                tasks_to_cleanup = [t for t in [main_task, sig_task] if t is not None and not t.done()]
                if tasks_to_cleanup:
                    cleanup_names = [t.get_name() for t in tasks_to_cleanup]
                    logger.debug(
                        "Final cleanup of %d remaining tasks: %s, reason: %s",
                        len(tasks_to_cleanup),
                        cleanup_names,
                        cleanup_reason.value,
                        extra={
                            "mission_id": mission_id,
                            "task_id": task_id,
                            "cleanup_count": len(tasks_to_cleanup),
                            "cleanup_tasks": cleanup_names,
                            "cancellation_reason": cleanup_reason.value,
                        },
                    )
                    for t in tasks_to_cleanup:
                        t.cancel()
                    await asyncio.gather(*tasks_to_cleanup, return_exceptions=True)

                duration = (
                    (session.completed_at - session.started_at).total_seconds()
                    if session.started_at and session.completed_at
                    else None
                )
                logger.info(
                    "Task execution completed: '%s', status: %s, reason: %s, duration: %.2fs",
                    task_id,
                    session.status,
                    session.cancellation_reason.value if session.status == "cancelled" else "n/a",
                    duration or 0,
                    extra={
                        "mission_id": mission_id,
                        "task_id": task_id,
                        "status": session.status,
                        "cancellation_reason": session.cancellation_reason.value,
                        "duration": duration,
                    },
                )

        # Return the supervisor task to be awaited by caller
        return asyncio.create_task(supervisor(), name=f"{task_id}_supervisor")
