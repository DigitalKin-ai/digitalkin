"""Task executor — runs module + signal canceller, cleans up in parallel."""

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
    """Runs module coroutine with signal-based cancellation.

    - Signal canceller subscribes to Redis pub/sub and calls
      ``main_task.cancel()`` directly on cancel/stop.
    - No FIRST_COMPLETED race — supervisor just awaits main_task.
    - Cleanup (stream drain, ack signal, session teardown) runs in parallel.
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
        """Execute a task with signal-based cancellation.

        Args:
            task_id: Unique identifier for the task.
            mission_id: Mission identifier for the task.
            coro: The coroutine to execute (module.start(...)).
            session: TaskSession for state management.

        Returns:
            The supervisor task managing the lifecycle.
        """
        ids = {"mission_id": mission_id, "task_id": task_id}

        async def _signal_canceller(target: asyncio.Task[None]) -> None:
            """Subscribe to signals and cancel target task on cancel/stop."""
            try:
                await session.listen_signals()
            except asyncio.CancelledError:
                return
            except Exception:
                session._signal_listener_failed = True  # noqa: SLF001
                logger.exception("Signal listener fatal error", extra=ids)
                return
            # listen_signals returned normally → cancel/stop was received
            if not session.cancelled:
                session.cancellation_reason = CancellationReason.SIGNAL_SERVICE_CANCEL
                session.is_cancelled.set()
            if not target.done():
                target.cancel()

        async def supervisor() -> None:
            """Run main task, cancel via signal, clean up in parallel.

            Raises:
                asyncio.CancelledError: If the supervisor is cancelled externally.
            """
            profiler = TaskProfiler(task_id, TaskExecutor._profiler_mode, TaskExecutor._profile_output_dir)
            profiler.start()

            session.started_at = datetime.datetime.now(datetime.timezone.utc)
            session.status = "running"

            main_task: asyncio.Task[None] | None = None
            sig_task: asyncio.Task[None] | None = None

            try:
                # Publish START signal
                with contextlib.suppress(Exception):
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

                main_task = asyncio.create_task(coro, name=f"{task_id}_main")
                sig_task = asyncio.create_task(
                    _signal_canceller(main_task),
                    name=f"{task_id}_listener",
                )

                # Just await main — signal canceller will .cancel() it if needed
                await main_task

                session.status = "completed"
                session.cancellation_reason = CancellationReason.COMPLETED
                logger.info("Task completed", extra=ids)

            except asyncio.CancelledError:
                if session.cancelled:
                    # Cancellation came from our signal canceller — absorb it
                    session.status = "cancelled"
                    logger.info("Task cancelled (%s)", session.cancellation_reason.value, extra=ids)
                else:
                    # External cancellation (supervisor.cancel()) — propagate
                    session.status = "cancelled"
                    session.cancellation_reason = CancellationReason.SIGNAL_SERVICE_CANCEL
                    logger.info("Task cancelled externally", extra=ids)
                    raise
            except Exception as e:
                session.status = "failed"
                session.record_exception(e)
                logger.exception("Task failed: '%s'", task_id, extra=ids)
                raise
            finally:
                profiler.stop()
                session.completed_at = datetime.datetime.now(datetime.timezone.utc)

                # 1. DRAIN — close stream so consumers see EOS
                session.close_stream()

                # 2. SIGNAL — publish STOP acknowledgment
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

                # 3. CLEAN — cancel signal listener in parallel
                if sig_task is not None and not sig_task.done():
                    sig_task.cancel()
                if main_task is not None and not main_task.done():
                    main_task.cancel()
                await asyncio.gather(
                    *[t for t in (main_task, sig_task) if t is not None and not t.done()],
                    return_exceptions=True,
                )

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

        return asyncio.create_task(supervisor(), name=f"{task_id}_supervisor")
