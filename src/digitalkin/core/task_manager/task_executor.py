"""Task executor for running tasks with full lifecycle management."""

import asyncio
import contextlib
import datetime
import os
from collections.abc import Coroutine
from typing import Any

from digitalkin.core.profiling.task_profiler import ProfilerMode, TaskProfiler
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

    _profiler_mode: ProfilerMode = ProfilerMode(os.environ.get("DIGITALKIN_PROFILER", "none"))
    _profile_output_dir: str = os.environ.get("DIGITALKIN_PROFILE_OUTPUT_DIR", "./profiles")

    @staticmethod
    async def execute_task(  # noqa: C901, PLR0915
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
            task_id: Unique identifier for the task.
            mission_id: Mission identifier for the task.
            coro: The coroutine to execute (module.start(...)).
            session: TaskSession for state management (signal service from session.signal_service).

        Returns:
            The supervisor task managing the lifecycle.
        """

        async def signal_wrapper() -> None:
            """Send start signal, listen for signals, send stop signal on exit."""
            ack_start_ok = False
            try:
                await session.signal_service.send_signal(
                    task_id,
                    SignalMessage(
                        task_id=task_id,
                        mission_id=mission_id,
                        setup_id=session.setup_id,
                        setup_version_id=session.setup_version_id,
                        action=SignalType.ACK_START,
                    ).model_dump(exclude_none=True),
                )
                ack_start_ok = True
                logger.debug(
                    "Task start signal sent",
                    extra={"mission_id": mission_id, "task_id": task_id},
                )
                await session.listen_signals()
            except asyncio.CancelledError:
                logger.debug("Signal listener cancelled", extra={"mission_id": mission_id, "task_id": task_id})
            finally:
                if ack_start_ok:
                    with contextlib.suppress(Exception):
                        await session.signal_service.send_signal(
                            task_id,
                            SignalMessage(
                                task_id=task_id,
                                mission_id=mission_id,
                                setup_id=session.setup_id,
                                setup_version_id=session.setup_version_id,
                                action=SignalType.ACK_STOP,
                                cancellation_reason=session.cancellation_reason,
                                error_message=session._last_exception,  # noqa: SLF001
                                exception_traceback=session._last_traceback,  # noqa: SLF001
                            ).model_dump(exclude_none=True),
                        )
                logger.debug("Signal listener ended", extra={"mission_id": mission_id, "task_id": task_id})

        async def supervisor() -> None:  # noqa: C901, PLR0915
            """Supervise the two concurrent tasks and handle outcomes.

            Raises:
                asyncio.CancelledError: If the supervisor task is cancelled externally.
                Exception: If the main task raises an unhandled exception.
            """
            profiler = TaskProfiler(
                task_id,
                TaskExecutor._profiler_mode,
                TaskExecutor._profile_output_dir,
            )
            profiler.start()

            session.started_at = datetime.datetime.now(datetime.timezone.utc)
            session.status = "running"

            main_task = None
            sig_task = None

            try:
                main_task = asyncio.create_task(coro, name=f"{task_id}_main")
                sig_task = asyncio.create_task(signal_wrapper(), name=f"{task_id}_listener")
                done, pending = await asyncio.wait(
                    [main_task, sig_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                completed = next(iter(done))

                session.close_stream()

                # Extract exception BEFORE cancelling pending tasks so
                # signal_wrapper's finally block has error details for ACK_STOP.
                exc = completed.exception() if not completed.cancelled() else None
                if exc is not None:
                    session.status = "failed"
                    session.cancellation_reason = CancellationReason.FAILURE_CLEANUP
                    session.record_exception(exc)

                for t in pending:
                    t.cancel()

                if exc is not None:
                    raise exc  # noqa: TRY301

                if completed is main_task:
                    session.status = "completed"
                    session.cancellation_reason = CancellationReason.COMPLETED
                    logger.info(
                        "Main task completed successfully",
                        extra={"mission_id": mission_id, "task_id": task_id},
                    )
                else:
                    session.status = "cancelled"
                    session.cancellation_reason = CancellationReason.SIGNAL_SERVICE_CANCEL
                    logger.info(
                        "Task cancelled via external signal",
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
                raise
            except Exception as e:
                if session.status != "failed":
                    session.status = "failed"
                    session.cancellation_reason = CancellationReason.FAILURE_CLEANUP
                    session.record_exception(e)
                logger.exception(
                    "Task failed with exception: '%s'",
                    task_id,
                    extra={"mission_id": mission_id, "task_id": task_id},
                )
                raise
            finally:
                session.completed_at = datetime.datetime.now(datetime.timezone.utc)
                tasks_to_cleanup = [t for t in [main_task, sig_task] if t is not None and not t.done()]
                if tasks_to_cleanup:
                    for t in tasks_to_cleanup:
                        t.cancel()
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*tasks_to_cleanup, return_exceptions=True),
                            timeout=5.0,
                        )
                    except TimeoutError:
                        logger.warning(
                            "Cleanup timed out for task '%s', %d sub-task(s) still pending",
                            task_id,
                            len(tasks_to_cleanup),
                            extra={"mission_id": mission_id, "task_id": task_id},
                        )

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
                profiler.stop()

        return asyncio.create_task(supervisor(), name=f"{task_id}_supervisor")
