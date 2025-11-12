"""Base task manager with common lifecycle management."""

import asyncio
import contextlib
import datetime
from abc import ABC, abstractmethod
from collections.abc import Coroutine
import signal
from typing import Any, final

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.modules._base_module import BaseModule


class BaseTaskManager(ABC):
    """Base task manager with common lifecycle management.

    Provides shared functionality for task orchestration, monitoring, signaling, and cancellation.
    Subclasses implement specific execution strategies (local or remote).
    """

    tasks: dict[str, asyncio.Task]
    tasks_sessions: dict[str, TaskSession]
    channel: SurrealDBConnection
    default_timeout: float
    max_concurrent_tasks: int
    _shutdown_event: asyncio.Event

    def __init__(
        self,
        default_timeout: float = 10.0,
        max_concurrent_tasks: int = 100,
    ) -> None:
        """Initialize task manager properties.

        Args:
            default_timeout: Default timeout for task operations in seconds
            max_concurrent_tasks: Maximum number of concurrent tasks
        """
        self.tasks = {}
        self.tasks_sessions = {}
        self.default_timeout = default_timeout
        self.max_concurrent_tasks = max_concurrent_tasks
        self._shutdown_event = asyncio.Event()

        logger.info(
            "%s initialized with max_concurrent_tasks: %d, default_timeout: %.1f",
            self.__class__.__name__,
            max_concurrent_tasks,
            default_timeout,
            extra={
                "max_concurrent_tasks": max_concurrent_tasks,
                "default_timeout": default_timeout,
            },
        )

    @property
    def task_count(self) -> int:
        """Number of managed tasks."""
        return len(self.tasks_sessions)

    @property
    def running_tasks(self) -> set[str]:
        """Get IDs of currently running tasks."""
        return {task_id for task_id, task in self.tasks.items() if not task.done()}

    async def _cleanup_task(self, task_id: str, mission_id: str) -> None:
        """Clean up task resources.

        Args:
            task_id: The ID of the task to clean up
            mission_id: The ID of the mission associated with the task
        """
        logger.debug(
            "Cleaning up resources for task: '%s'", task_id, extra={"mission_id": mission_id, "task_id": task_id}
        )
        if task_id in self.tasks_sessions:
            await self.tasks_sessions[task_id].db.close()

    async def _validate_task_creation(self, task_id: str, mission_id: str, coro: Coroutine[Any, Any, None]) -> None:
        """Validate task creation preconditions.

        Args:
            task_id: The ID of the task to create
            mission_id: The ID of the mission associated with the task
            coro: The coroutine to execute

        Raises:
            ValueError: If task_id already exists
            RuntimeError: If max concurrent tasks reached
        """
        if task_id in self.tasks_sessions:
            coro.close()
            logger.warning(
                "Task creation failed - task already exists: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id},
            )
            msg = f"Task {task_id} already exists"
            raise ValueError(msg)

        if len(self.tasks_sessions) >= self.max_concurrent_tasks:
            coro.close()
            logger.error(
                "Task creation failed - max concurrent tasks reached: %d",
                self.max_concurrent_tasks,
                extra={
                    "mission_id": mission_id,
                    "task_id": task_id,
                    "current_count": len(self.tasks_sessions),
                    "max_concurrent": self.max_concurrent_tasks,
                },
            )
            msg = f"Maximum concurrent tasks ({self.max_concurrent_tasks}) reached"
            raise RuntimeError(msg)

    async def _create_session(
        self,
        task_id: str,
        mission_id: str,
        module: BaseModule,
        heartbeat_interval: datetime.timedelta,
        connection_timeout: datetime.timedelta,
    ) -> tuple[SurrealDBConnection, TaskSession]:
        """Create SurrealDB connection and task session.

        Args:
            task_id: The ID of the task
            mission_id: The ID of the mission
            module: The module instance
            heartbeat_interval: Interval between heartbeats
            connection_timeout: Connection timeout for SurrealDB

        Returns:
            Tuple of (channel, session)
        """
        channel: SurrealDBConnection = SurrealDBConnection("task_manager", connection_timeout)
        await channel.init_surreal_instance()
        session = TaskSession(
            task_id=task_id,
            mission_id=mission_id,
            db=channel,
            module=module,
            heartbeat_interval=heartbeat_interval,
        )
        self.tasks_sessions[task_id] = session
        return channel, session

    @abstractmethod
    async def create_task(
        self,
        task_id: str,
        mission_id: str,
        module: BaseModule,
        coro: Coroutine[Any, Any, None],
        heartbeat_interval: datetime.timedelta = datetime.timedelta(seconds=2),
        connection_timeout: datetime.timedelta = datetime.timedelta(seconds=5),
    ) -> None:
        """Create and manage a new task.

        Subclasses implement specific execution strategies.

        Args:
            task_id: Unique identifier for the task
            mission_id: Mission identifier
            module: Module instance to execute
            coro: Coroutine to execute
            heartbeat_interval: Interval between heartbeats
            connection_timeout: Connection timeout for SurrealDB

        Raises:
            ValueError: If task_id duplicated
            RuntimeError: If task overload
        """
        ...

    async def send_signal(self, task_id: str, mission_id: str, signal_type: str, payload: dict) -> bool:
        """Send signal to a specific task.

        Args:
            task_id: The ID of the task
            mission_id: The ID of the mission
            signal_type: Type of signal to send
            payload: Signal payload

        Returns:
            True if the signal was sent successfully, False otherwise
        """
        if task_id not in self.tasks_sessions:
            logger.warning(
                "Cannot send signal - task not found: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id, "signal_type": signal_type},
            )
            return False

        logger.info(
            "Sending signal '%s' to task: '%s'",
            signal_type,
            task_id,
            extra={"mission_id": mission_id, "task_id": task_id, "signal_type": signal_type, "payload": payload},
        )

        await self.channel.update("tasks", signal_type, payload)
        return True

    async def cancel_task(self, task_id: str, mission_id: str, timeout: float | None = None) -> bool:
        """Cancel a task with graceful shutdown and fallback.

        Args:
            task_id: The ID of the task to cancel
            mission_id: The ID of the mission
            timeout: Optional timeout for cancellation

        Returns:
            True if the task was cancelled successfully, False otherwise
        """
        if task_id not in self.tasks:
            logger.warning(
                "Cannot cancel - task not found: '%s'", task_id, extra={"mission_id": mission_id, "task_id": task_id}
            )
            return True

        timeout = timeout or self.default_timeout
        task = self.tasks[task_id]

        logger.info(
            "Initiating task cancellation: '%s', timeout: %.1fs",
            task_id,
            timeout,
            extra={"mission_id": mission_id, "task_id": task_id, "timeout": timeout},
        )

        try:
            # Wait for graceful shutdown
            await asyncio.wait_for(task, timeout=timeout)

            logger.info(
                "Task cancelled gracefully: '%s'", task_id, extra={"mission_id": mission_id, "task_id": task_id}
            )

        except asyncio.TimeoutError:
            logger.warning(
                "Graceful cancellation timed out for task: '%s', forcing cancellation",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id, "timeout": timeout},
            )

            # Phase 2: Force cancellation
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

            logger.warning("Task force-cancelled: '%s'", task_id, extra={"mission_id": mission_id, "task_id": task_id})
            return True

        except Exception as e:
            logger.error(
                "Error during task cancellation: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id, "error": str(e)},
                exc_info=True,
            )
            return False
        finally:
            self.tasks.pop(task_id, None)

        return True

    async def clean_session(self, task_id: str, mission_id: str) -> bool:
        """Clean up task session without cancelling the task.

        Args:
            task_id: The ID of the task
            mission_id: The ID of the mission
        Returns:
            True if the task was cleaned successfully, False otherwise
        """
        if task_id not in self.tasks_sessions:
            logger.warning(
                "Cannot clean session - task not found: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id},
            )
            return False

        await self.tasks_sessions[task_id].module.stop()
        await self.cancel_task(mission_id=mission_id, task_id=task_id)

        logger.info("Cleaning up session for task: '%s'", task_id, extra={"mission_id": mission_id, "task_id": task_id})
        self.tasks_sessions.pop(task_id, None)
        return True

    async def pause_task(self, task_id: str, mission_id: str) -> bool:
        """Pause a running task.

        Args:
            task_id: The ID of the task
            mission_id: The ID of the mission

        Returns:
            True if the task was paused successfully, False otherwise
        """
        return await self.send_signal(task_id=task_id, mission_id=mission_id, signal_type="pause", payload={})

    async def resume_task(self, task_id: str, mission_id: str) -> bool:
        """Resume a paused task.

        Args:
            task_id: The ID of the task
            mission_id: The ID of the mission

        Returns:
            True if the task was resumed successfully, False otherwise
        """
        return await self.send_signal(task_id=task_id, mission_id=mission_id, signal_type="resume", payload={})

    async def get_task_status(self, task_id: str, mission_id: str) -> bool:
        """Request status from a task.

        Args:
            task_id: The ID of the task
            mission_id: The ID of the mission

        Returns:
            True if the status request was sent successfully, False otherwise
        """
        return await self.send_signal(task_id=task_id, mission_id=mission_id, signal_type="status", payload={})

    async def cancel_all_tasks(self, mission_id: str, timeout: float | None = None) -> dict[str, bool]:
        """Cancel all running tasks.

        Args:
            mission_id: The ID of the mission
            timeout: Optional timeout for cancellation

        Returns:
            Dictionary mapping task_id to cancellation success status
        """
        timeout = timeout or self.default_timeout
        task_ids = list(self.running_tasks)

        logger.info(
            "Cancelling all tasks in parallel: %d tasks",
            len(task_ids),
            extra={"mission_id": mission_id, "task_count": len(task_ids), "timeout": timeout},
        )

        # Cancel all tasks in parallel to reduce latency
        cancel_coros = [
            self.cancel_task(
                task_id=task_id,
                mission_id=mission_id,
                timeout=timeout,
            )
            for task_id in task_ids
        ]
        results_list = await asyncio.gather(*cancel_coros, return_exceptions=True)

        # Build results dictionary
        results = {}
        for task_id, result in zip(task_ids, results_list):
            if isinstance(result, Exception):
                logger.error("Exception cancelling task %s: %s", task_id, result)
                results[task_id] = False
            else:
                results[task_id] = result

        return results

    async def shutdown(self, mission_id: str, timeout: float = 30.0) -> None:
        """Graceful shutdown of all tasks.

        Args:
            mission_id: The ID of the mission
            timeout: Timeout for shutdown operations
        """
        logger.info(
            "TaskManager shutdown initiated, timeout: %.1fs",
            timeout,
            extra={"mission_id": mission_id, "timeout": timeout, "active_tasks": len(self.running_tasks)},
        )

        self._shutdown_event.set()
        results = await self.cancel_all_tasks(mission_id, timeout)

        failed_tasks = [task_id for task_id, success in results.items() if not success]
        if failed_tasks:
            logger.error(
                "Failed to cancel %d tasks during shutdown: %s",
                len(failed_tasks),
                failed_tasks,
                extra={"mission_id": mission_id, "failed_tasks": failed_tasks, "failed_count": len(failed_tasks)},
            )

        logger.info(
            "TaskManager shutdown completed, cancelled: %d, failed: %d",
            len(results) - len(failed_tasks),
            len(failed_tasks),
            extra={
                "mission_id": mission_id,
                "cancelled_count": len(results) - len(failed_tasks),
                "failed_count": len(failed_tasks),
            },
        )
