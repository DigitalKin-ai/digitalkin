"""Task manager with comprehensive lifecycle management."""

import asyncio
import contextlib
import datetime
from collections.abc import Coroutine
from typing import Any

from digitalkin.logger import logger
from digitalkin.models.module.task_monitor import SignalMessage, SignalType, TaskStatus
from digitalkin.modules._base_module import BaseModule
from digitalkin.modules.job_manager.task_session import SurrealDBConnection, TaskSession


class TaskManager:
    """Task manager with comprehensive lifecycle management."""

    tasks: dict[str, asyncio.Task]
    tasks_sessions: dict[str, TaskSession]
    channel: SurrealDBConnection
    default_timeout: float
    max_concurrent_tasks: int
    _shutdown_event: asyncio.Event

    def __init__(self, default_timeout: float = 10.0, max_concurrent_tasks: int = 100) -> None:
        """."""
        self.tasks = {}
        self.tasks_sessions = {}
        self.default_timeout = default_timeout
        self.max_concurrent_tasks = max_concurrent_tasks
        self._shutdown_event = asyncio.Event()

        logger.info(
            "TaskManager initialized with max_concurrent_tasks: %d, default_timeout: %.1f",
            max_concurrent_tasks,
            default_timeout,
            extra={"max_concurrent_tasks": max_concurrent_tasks, "default_timeout": default_timeout},
        )

    @property
    def task_count(self) -> int:
        """."""
        return len(self.tasks_sessions)

    @property
    def running_tasks(self) -> set[str]:
        """."""
        return {task_id for task_id, task in self.tasks.items() if not task.done()}

    async def _cleanup_task(self, task_id: str) -> None:
        """Clean up task resources."""
        logger.debug("Cleaning up resources for task: '%s'", task_id, extra={"task_id": task_id})
        if task_id in self.tasks_sessions:
            await self.tasks_sessions[task_id].db.close()
        # Remove from collections

    async def _task_wrapper(  # noqa: C901, PLR0915
        self,
        task_id: str,
        coro: Coroutine[Any, Any, None],
        session: TaskSession,
    ) -> asyncio.Task[None]:
        """Task wrapper that runs main, heartbeat, and listener concurrently.

        The first to finish determines the outcome. Returns a Task that the
        caller can await externally.

        Returns:
            asyncio.Task[None]: The supervisor task managing the lifecycle.
        """

        async def signal_wrapper() -> None:
            try:
                await self.channel.create(
                    "tasks",
                    SignalMessage(
                        task_id=task_id,
                        status=session.status,
                        action=SignalType.START,
                    ).model_dump(),
                )
                await session.listen_signals()
            except asyncio.CancelledError:
                logger.debug("Signal listener cancelled", extra={"task_id": task_id})
            finally:
                await self.channel.create(
                    "tasks",
                    SignalMessage(
                        task_id=task_id,
                        status=session.status,
                        action=SignalType.STOP,
                    ).model_dump(),
                )
                logger.info("Signal listener ended", extra={"task_id": task_id})

        async def heartbeat_wrapper() -> None:
            try:
                await session.generate_heartbeats()
            except asyncio.CancelledError:
                logger.debug("Signal listener cancelled", extra={"task_id": task_id})
            finally:
                logger.info("Heartbeat task ended", extra={"task_id": task_id})

        async def supervisor() -> None:
            session.started_at = datetime.datetime.now(datetime.timezone.utc)
            session.status = TaskStatus.RUNNING

            main_task = asyncio.create_task(coro, name=f"{task_id}_main")
            hb_task = asyncio.create_task(heartbeat_wrapper(), name=f"{task_id}_heartbeat")
            sig_task = asyncio.create_task(signal_wrapper(), name=f"{task_id}_listener")

            try:
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

                if completed is main_task:
                    session.status = TaskStatus.COMPLETED
                elif completed is sig_task or (completed is hb_task and sig_task.done()):
                    logger.debug(f"Task cancelled due to signal {sig_task=}")
                    session.status = TaskStatus.CANCELLED
                elif completed is hb_task:
                    session.status = TaskStatus.FAILED
                    msg = f"Heartbeat stopped for {task_id}"
                    raise RuntimeError(msg)  # noqa: TRY301

            except asyncio.CancelledError:
                session.status = TaskStatus.CANCELLED
                raise
            except Exception:
                session.status = TaskStatus.FAILED
                raise
            finally:
                session.completed_at = datetime.datetime.now(datetime.timezone.utc)
                # Ensure all tasks are cleaned up
                for t in [main_task, hb_task, sig_task]:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(main_task, hb_task, sig_task, return_exceptions=True)

        # Return the supervisor task to be awaited outside
        return asyncio.create_task(supervisor(), name=f"{task_id}_supervisor")

    async def create_task(
        self,
        task_id: str,
        module: BaseModule,
        coro: Coroutine[Any, Any, None],
        heartbeat_interval: datetime.timedelta = datetime.timedelta(seconds=2),
        connection_timeout: datetime.timedelta = datetime.timedelta(seconds=5),
    ) -> None:
        """Create and start a new managed task.

        Raises:
            ValueError: task_id duplicated
            RuntimeError: task overload
        """
        if task_id in self.tasks:
            # close Coroutine during runtime
            coro.close()
            logger.warning("Task creation failed - task already exists: '%s'", task_id, extra={"task_id": task_id})
            msg = f"Task {task_id} already exists"
            raise ValueError(msg)

        if len(self.tasks) >= self.max_concurrent_tasks:
            coro.close()
            logger.error(
                "Task creation failed - max concurrent tasks reached: %d",
                self.max_concurrent_tasks,
                extra={
                    "task_id": task_id,
                    "current_count": len(self.tasks),
                    "max_concurrent": self.max_concurrent_tasks,
                },
            )
            msg = f"Maximum concurrent tasks ({self.max_concurrent_tasks}) reached"
            raise RuntimeError(msg)

        logger.info(
            "Creating new task: '%s'",
            task_id,
            extra={
                "task_id": task_id,
                "heartbeat_interval": heartbeat_interval,
                "connection_timeout": connection_timeout,
            },
        )

        try:
            # Initialize components
            channel: SurrealDBConnection = SurrealDBConnection("task_manager", connection_timeout)
            await channel.init_surreal_instance()
            session = TaskSession(task_id, channel, module, heartbeat_interval)

            self.tasks_sessions[task_id] = session

            # Create wrapper task
            self.tasks[task_id] = asyncio.create_task(self._task_wrapper(task_id, coro, session), name=task_id)

            logger.info(
                "Task created successfully: '%s'",
                task_id,
                extra={
                    "task_id": task_id,
                    "total_tasks": len(self.tasks),
                },
            )

        except Exception as e:
            logger.error(
                "Failed to create task: '%s'", task_id, extra={"task_id": task_id, "error": str(e)}, exc_info=True
            )
            # Cleanup on failure
            await self._cleanup_task(task_id)
            raise

    async def send_signal(self, task_id: str, signal_type: str, payload: dict) -> bool:
        """Send signal to a specific task.

        Returns:
            bool: True if the task sent successfully the given signal, False otherwise.
        """
        if task_id not in self.tasks_sessions:
            logger.warning(
                "Cannot send signal - task not found: '%s'",
                task_id,
                extra={"task_id": task_id, "signal_type": signal_type},
            )
            return False

        logger.info(
            "Sending signal '%s' to task: '%s'",
            signal_type,
            task_id,
            extra={"task_id": task_id, "signal_type": signal_type, "payload": payload},
        )

        await self.channel.update("tasks", signal_type, payload)
        return True

    async def cancel_task(self, task_id: str, timeout: float | None = None) -> bool:
        """Cancel a task with graceful shutdown and fallback.

        Returns:
            bool: True if the task was cancelled successfully, False otherwise.
        """
        if task_id not in self.tasks:
            logger.warning("Cannot cancel - task not found: '%s'", task_id, extra={"task_id": task_id})
            return True

        timeout = timeout or self.default_timeout
        task = self.tasks[task_id]

        logger.info(
            "Initiating task cancellation: '%s', timeout: %.1fs",
            task_id,
            timeout,
            extra={"task_id": task_id, "timeout": timeout},
        )

        try:
            # Phase 1: Cooperative cancellation
            # await self.send_signal(task_id, "cancel")  # noqa: ERA001

            # Wait for graceful shutdown
            await asyncio.wait_for(task, timeout=timeout)

            logger.info("Task cancelled gracefully: '%s'", task_id, extra={"task_id": task_id})

        except asyncio.TimeoutError:
            logger.warning(
                "Graceful cancellation timed out for task: '%s', forcing cancellation",
                task_id,
                extra={"task_id": task_id, "timeout": timeout},
            )

            # Phase 2: Force cancellation
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

            logger.warning("Task force-cancelled: '%s'", task_id, extra={"task_id": task_id})
            return True

        except Exception as e:
            logger.error(
                "Error during task cancellation: '%s'",
                task_id,
                extra={"task_id": task_id, "error": str(e)},
                exc_info=True,
            )
            return False
        return True

    async def clean_session(self, task_id: str) -> bool:
        """Clean up task session without cancelling the task.

        Returns:
            bool: True if the task was cleaned successfully, False otherwise.
        """
        if task_id not in self.tasks_sessions:
            logger.warning("Cannot clean session - task not found: '%s'", task_id, extra={"task_id": task_id})
            return False

        await self.tasks_sessions[task_id].module.stop()
        await self.cancel_task(task_id)

        logger.info("Cleaning up session for task: '%s'", task_id, extra={"task_id": task_id})
        self.tasks_sessions.pop(task_id, None)
        return True

    async def pause_task(self, task_id: str) -> bool:
        """Pause a running task.

        Returns:
            bool: True if the task was paused successfully, False otherwise.
        """
        return await self.send_signal(task_id, "pause", {})

    async def resume_task(self, task_id: str) -> bool:
        """Resume a paused task.

        Returns:
            bool: True if the task was paused successfully, False otherwise.
        """
        return await self.send_signal(task_id, "resume", {})

    async def get_task_status(self, task_id: str) -> bool:
        """Request status from a task.

        Returns:
            bool: True if the task was paused successfully, False otherwise.
        """
        return await self.send_signal(task_id, "status", {})

    async def cancel_all_tasks(self, timeout: float | None = None) -> dict[str, bool]:
        """Cancel all running tasks.

        Returns:
            dict[str: bool]: True if the tasks were paused successfully, False otherwise.
        """
        timeout = timeout or self.default_timeout
        task_ids = list(self.running_tasks)

        logger.info(
            "Cancelling all tasks: %d tasks", len(task_ids), extra={"task_count": len(task_ids), "timeout": timeout}
        )

        results = {}
        for task_id in task_ids:
            results[task_id] = await self.cancel_task(task_id, timeout)

        return results

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Graceful shutdown of all tasks."""
        logger.info(
            "TaskManager shutdown initiated, timeout: %.1fs",
            timeout,
            extra={"timeout": timeout, "active_tasks": len(self.running_tasks)},
        )

        self._shutdown_event.set()
        results = await self.cancel_all_tasks(timeout)

        failed_tasks = [task_id for task_id, success in results.items() if not success]
        if failed_tasks:
            logger.error(
                "Failed to cancel %d tasks during shutdown: %s",
                len(failed_tasks),
                failed_tasks,
                extra={"failed_tasks": failed_tasks, "failed_count": len(failed_tasks)},
            )

        logger.info(
            "TaskManager shutdown completed, cancelled: %d, failed: %d",
            len(results) - len(failed_tasks),
            len(failed_tasks),
            extra={"cancelled_count": len(results) - len(failed_tasks), "failed_count": len(failed_tasks)},
        )
