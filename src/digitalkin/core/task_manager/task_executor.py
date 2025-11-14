"""Task executor for running tasks with full lifecycle management."""

import asyncio
import datetime
from collections.abc import Coroutine
from typing import Any

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import SignalMessage, SignalType, TaskStatus


class TaskExecutor:
    """Executes tasks with the supervisor pattern (main + heartbeat + signal listener).

    Pure execution logic - no task registry or orchestration.
    Used by workers to run distributed tasks or by TaskManager for local execution.
    """

    async def execute_task(
        self,
        task_id: str,
        mission_id: str,
        coro: Coroutine[Any, Any, None],
        session: TaskSession,
        channel: SurrealDBConnection,
    ) -> asyncio.Task[None]:
        """Execute a task using the supervisor pattern.

        Runs three concurrent sub-tasks:
        - Main coroutine (the actual work)
        - Heartbeat generator (sends heartbeats to SurrealDB)
        - Signal listener (watches for stop/pause/resume signals)

        The first task to complete determines the outcome.

        Args:
            task_id: Unique identifier for the task
            mission_id: Mission identifier for the task
            coro: The coroutine to execute (module.start(...))
            session: TaskSession for state management
            channel: SurrealDB connection for signals

        Returns:
            asyncio.Task: The supervisor task managing the lifecycle
        """

        async def signal_wrapper() -> None:
            """Create initial signal record and listen for signals."""
            try:
                await channel.create(
                    "tasks",
                    SignalMessage(
                        task_id=task_id,
                        mission_id=mission_id,
                        status=session.status,
                        action=SignalType.START,
                    ).model_dump(),
                )
                await session.listen_signals()
            except asyncio.CancelledError:
                logger.debug("Signal listener cancelled", extra={"mission_id": mission_id, "task_id": task_id})
            finally:
                await channel.create(
                    "tasks",
                    SignalMessage(
                        task_id=task_id,
                        mission_id=mission_id,
                        status=session.status,
                        action=SignalType.STOP,
                    ).model_dump(),
                )
                logger.info("Signal listener ended", extra={"mission_id": mission_id, "task_id": task_id})

        async def heartbeat_wrapper() -> None:
            """Generate heartbeats for task health monitoring."""
            try:
                await session.generate_heartbeats()
            except asyncio.CancelledError:
                logger.debug("Heartbeat cancelled", extra={"mission_id": mission_id, "task_id": task_id})
            finally:
                logger.info("Heartbeat task ended", extra={"mission_id": mission_id, "task_id": task_id})

        async def supervisor() -> None:
            """Supervise the three concurrent tasks and handle outcomes.

            Raises:
                RuntimeError: If the heartbeat task stops unexpectedly.
                asyncio.CancelledError: If the supervisor task is cancelled.
            """
            session.started_at = datetime.datetime.now(datetime.timezone.utc)
            session.status = TaskStatus.RUNNING

            # Create tasks with proper exception handling
            main_task = None
            hb_task = None
            sig_task = None

            try:
                main_task = asyncio.create_task(coro, name=f"{task_id}_main")
                hb_task = asyncio.create_task(heartbeat_wrapper(), name=f"{task_id}_heartbeat")
                sig_task = asyncio.create_task(signal_wrapper(), name=f"{task_id}_listener")
                done, pending = await asyncio.wait(
                    [main_task, sig_task, hb_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # One task completed -> cancel the others
                for t in pending:
                    t.cancel()

                # Propagate exception/result from the finished task
                completed = next(iter(done))
                await completed

                # Determine final status based on which task completed
                if completed is main_task:
                    session.status = TaskStatus.COMPLETED
                    logger.info(
                        "Main task completed successfully",
                        extra={"mission_id": mission_id, "task_id": task_id},
                    )
                elif completed is sig_task or (completed is hb_task and sig_task.done()):
                    logger.debug(
                        f"Task cancelled due to signal {sig_task=}",
                        extra={"mission_id": mission_id, "task_id": task_id},
                    )
                    session.status = TaskStatus.CANCELLED
                elif completed is hb_task:
                    session.status = TaskStatus.FAILED
                    logger.error(
                        f"Heartbeat stopped for {task_id}",
                        extra={"mission_id": mission_id, "task_id": task_id},
                    )
                    msg = f"Heartbeat stopped for {task_id}"
                    raise RuntimeError(msg)  # noqa: TRY301

            except asyncio.CancelledError:
                session.status = TaskStatus.CANCELLED
                logger.info("Task cancelled", extra={"mission_id": mission_id, "task_id": task_id})
                raise
            except Exception:
                session.status = TaskStatus.FAILED
                logger.exception("Task failed", extra={"mission_id": mission_id, "task_id": task_id})
                raise
            finally:
                session.completed_at = datetime.datetime.now(datetime.timezone.utc)
                # Ensure all tasks are cleaned up
                tasks_to_cleanup = [t for t in [main_task, hb_task, sig_task] if t is not None]
                for t in tasks_to_cleanup:
                    if not t.done():
                        t.cancel()
                if tasks_to_cleanup:
                    await asyncio.gather(*tasks_to_cleanup, return_exceptions=True)

                logger.info(
                    "Task execution completed with status: %s",
                    session.status,
                    extra={
                        "mission_id": mission_id,
                        "task_id": task_id,
                        "status": session.status,
                        "duration": (
                            (session.completed_at - session.started_at).total_seconds()
                            if session.started_at and session.completed_at
                            else None
                        ),
                    },
                )

        # Return the supervisor task to be awaited by caller
        return asyncio.create_task(supervisor(), name=f"{task_id}_supervisor")
