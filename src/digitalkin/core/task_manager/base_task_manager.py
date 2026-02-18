"""Base task manager with common lifecycle management."""

import asyncio
import contextlib
import types
from abc import ABC, abstractmethod
from collections.abc import Coroutine
from typing import Any

from typing_extensions import Self

from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import CancellationReason, SignalMessage, SignalType
from digitalkin.modules._base_module import BaseModule


class BaseTaskManager(ABC):
    """Base task manager with common lifecycle management.

    Provides shared functionality for task orchestration, monitoring, signaling, and cancellation.
    Subclasses implement specific execution strategies (local or remote).

    Supports async context manager protocol for automatic resource cleanup:
        async with LocalTaskManager() as manager:
            await manager.create_task(...)
            # Resources automatically cleaned up on exit
    """

    tasks: dict[str, asyncio.Task]
    tasks_sessions: dict[str, TaskSession]
    default_timeout: float
    max_concurrent_tasks: int
    _shutdown_event: asyncio.Event
    _tasks_lock: asyncio.Lock

    def __init__(
        self,
        default_timeout: float = 10.0,
        max_concurrent_tasks: int = 100,
    ) -> None:
        """Initialize task manager properties.

        Args:
            default_timeout: Default timeout for task operations in seconds.
            max_concurrent_tasks: Maximum number of concurrent tasks.
        """
        self.tasks = {}
        self.tasks_sessions = {}
        self.default_timeout = default_timeout
        self.max_concurrent_tasks = max_concurrent_tasks
        self._shutdown_event = asyncio.Event()
        self._tasks_lock = asyncio.Lock()

        logger.info(
            "%s initialized (max_concurrent_tasks=%d, default_timeout=%.1fs)",
            self.__class__.__name__,
            max_concurrent_tasks,
            default_timeout,
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
            task_id: The ID of the task to clean up.
            mission_id: The ID of the mission associated with the task.
        """
        session = self.tasks_sessions.get(task_id)
        cancellation_reason = session.cancellation_reason.value if session else "no_session"
        final_status = session.status if session else "unknown"

        logger.debug(
            "Cleaning up resources (managed_tasks=%d, sessions=%d, asyncio_tasks=%d)",
            len(self.tasks),
            len(self.tasks_sessions),
            len(asyncio.all_tasks()),
            extra={
                "mission_id": mission_id,
                "task_id": task_id,
                "final_status": final_status,
                "cancellation_reason": cancellation_reason,
            },
        )

        if session:
            await session.cleanup()

        async with self._tasks_lock:
            self.tasks_sessions.pop(task_id, None)
            self.tasks.pop(task_id, None)

        logger.info(
            "Task cleanup done (managed_tasks=%d, sessions=%d, asyncio_tasks=%d)",
            len(self.tasks),
            len(self.tasks_sessions),
            len(asyncio.all_tasks()),
            extra={"mission_id": mission_id, "task_id": task_id},
        )

    async def _validate_task_creation(self, task_id: str, mission_id: str, coro: Coroutine[Any, Any, None]) -> None:
        """Validate task creation preconditions.

        Args:
            task_id: The ID of the task to create.
            mission_id: The ID of the mission associated with the task.
            coro: The coroutine to execute.

        Raises:
            ValueError: If task_id already exists.
            RuntimeError: If max concurrent tasks reached.
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
    ) -> TaskSession:
        """Create task session (signal service derived from module context).

        Args:
            task_id: The ID of the task.
            mission_id: The ID of the mission.
            module: The module instance.

        Returns:
            The created TaskSession.
        """
        session = TaskSession(
            task_id=task_id,
            mission_id=mission_id,
            module=module,
        )
        self.tasks_sessions[task_id] = session

        logger.info(
            "Task session created (managed_tasks=%d, sessions=%d, asyncio_tasks=%d)",
            len(self.tasks),
            len(self.tasks_sessions),
            len(asyncio.all_tasks()),
            extra={"mission_id": mission_id, "task_id": task_id},
        )
        return session

    @abstractmethod
    async def create_task(
        self,
        task_id: str,
        mission_id: str,
        module: BaseModule,
        coro: Coroutine[Any, Any, None],
    ) -> None:
        """Create and manage a new task.

        Subclasses implement specific execution strategies.

        Args:
            task_id: Unique identifier for the task.
            mission_id: Mission identifier.
            module: Module instance to execute.
            coro: Coroutine to execute.

        Raises:
            ValueError: If task_id duplicated.
            RuntimeError: If task overload.
        """
        ...

    async def send_signal(self, task_id: str, mission_id: str, signal_type: str, payload: dict) -> bool:
        """Send signal to a specific task.

        Args:
            task_id: The ID of the task.
            mission_id: The ID of the mission.
            signal_type: Type of signal to send.
            payload: Signal payload.

        Returns:
            True if the signal was sent successfully, False otherwise.
        """
        if task_id not in self.tasks_sessions:
            logger.warning(
                "Cannot send signal - task not found: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id, "signal_type": signal_type},
            )
            return False

        session = self.tasks_sessions[task_id]

        logger.info(
            "Sending signal '%s' to task '%s' (current_status=%s)",
            signal_type,
            task_id,
            session.status,
            extra={"mission_id": mission_id, "task_id": task_id},
        )
        await session.signal_service.send_signal(
            task_id,
            SignalMessage(
                task_id=task_id,
                mission_id=mission_id,
                setup_id=session.setup_id,
                setup_version_id=session.setup_version_id,
                action=SignalType[signal_type.upper()],
                payload=payload,
            ).model_dump(exclude_none=True),
        )
        return True

    async def cancel_task(self, task_id: str, mission_id: str, timeout: float | None = None) -> bool:
        """Cancel a task with graceful shutdown and fallback.

        Args:
            task_id: The ID of the task to cancel.
            mission_id: The ID of the mission.
            timeout: Optional timeout for cancellation.

        Returns:
            True if the task was cancelled successfully, False otherwise.
        """
        if task_id not in self.tasks:
            logger.warning(
                "Cannot cancel - task not found: '%s'", task_id, extra={"mission_id": mission_id, "task_id": task_id}
            )
            # Still cleanup any orphaned session
            await self._cleanup_task(task_id, mission_id)
            return True

        timeout = timeout or self.default_timeout
        task = self.tasks[task_id]

        logger.info(
            "Initiating task cancellation: '%s', timeout: %.1fs (managed_tasks=%d, sessions=%d, asyncio_tasks=%d)",
            task_id,
            timeout,
            len(self.tasks),
            len(self.tasks_sessions),
            len(asyncio.all_tasks()),
            extra={"mission_id": mission_id, "task_id": task_id, "timeout": timeout},
        )

        try:
            # Phase 1: Send cancel signal for graceful shutdown
            await self.send_signal(task_id, mission_id, "cancel", {})
            await asyncio.wait_for(task, timeout=timeout)

            logger.info(
                "Task cancelled gracefully: '%s'", task_id, extra={"mission_id": mission_id, "task_id": task_id}
            )
        except asyncio.TimeoutError:
            # Set timeout as cancellation reason
            if task_id in self.tasks_sessions:
                session = self.tasks_sessions[task_id]
                if session.cancellation_reason == CancellationReason.UNKNOWN:
                    session.cancellation_reason = CancellationReason.TIMEOUT

            logger.warning(
                "Graceful cancellation timed out for task '%s', forcing cancellation",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id},
            )

            # Phase 2: Force cancellation
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

            logger.warning(
                "Task force-cancelled: '%s' (%s)",
                task_id,
                CancellationReason.TIMEOUT.value,
                extra={"mission_id": mission_id, "task_id": task_id},
            )
            return True

        except Exception as e:
            logger.error(
                "Error during task cancellation '%s': %s",
                task_id,
                e,
                extra={"mission_id": mission_id, "task_id": task_id},
                exc_info=True,
            )
            return False
        finally:
            await self._cleanup_task(task_id, mission_id)
        return True

    async def clean_session(self, task_id: str, mission_id: str) -> bool:
        """Clean up task session, only cancelling if the task is still running.

        Args:
            task_id: The ID of the task.
            mission_id: The ID of the mission.

        Returns:
            True if the task session was cleaned successfully, False otherwise.
        """
        if task_id not in self.tasks_sessions:
            logger.warning(
                "Cannot clean session - task not found: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id},
            )
            return False

        task = self.tasks.get(task_id)
        session = self.tasks_sessions[task_id]

        logger.info(
            "Cleaning session for task '%s' (status=%s, task_done=%s, managed_tasks=%d, sessions=%d, asyncio_tasks=%d)",
            task_id,
            session.status,
            task.done() if task else "no_task",
            len(self.tasks),
            len(self.tasks_sessions),
            len(asyncio.all_tasks()),
            extra={"mission_id": mission_id, "task_id": task_id},
        )

        # Only cancel if the task is still running; otherwise just clean up resources
        if task is not None and not task.done():
            await self.cancel_task(mission_id=mission_id, task_id=task_id)
        else:
            await self._cleanup_task(task_id, mission_id)

        return True

    async def get_task_status(self, task_id: str, mission_id: str) -> bool:
        """Request status from a task.

        Args:
            task_id: The ID of the task.
            mission_id: The ID of the mission.

        Returns:
            True if the status request was sent successfully, False otherwise.
        """
        return await self.send_signal(task_id=task_id, mission_id=mission_id, signal_type="status", payload={})

    async def cancel_all_tasks(self, mission_id: str, timeout: float | None = None) -> dict[str, bool | BaseException]:
        """Cancel all running tasks.

        Args:
            mission_id: The ID of the mission.
            timeout: Optional timeout for cancellation.

        Returns:
            Dictionary mapping task_id to cancellation success status.
        """
        timeout = timeout or self.default_timeout
        task_ids = list(self.running_tasks)

        logger.info(
            "Cancelling all tasks in parallel: %d tasks",
            len(task_ids),
            extra={"mission_id": mission_id, "task_count": len(task_ids), "timeout": timeout},
        )

        cancel_coros = [
            self.cancel_task(
                task_id=task_id,
                mission_id=mission_id,
                timeout=timeout,
            )
            for task_id in task_ids
        ]
        results_list = await asyncio.gather(*cancel_coros, return_exceptions=True)

        results: dict[str, bool | BaseException] = {}
        for task_id, result in zip(task_ids, results_list):
            if isinstance(result, Exception):
                logger.error(
                    "Exception cancelling task: '%s', error: %s",
                    task_id,
                    result,
                    extra={
                        "mission_id": mission_id,
                        "task_id": task_id,
                        "error": str(result),
                    },
                )
                results[task_id] = False
            else:
                results[task_id] = result

        return results

    async def shutdown(self, mission_id: str, timeout: float = 30.0) -> None:
        """Graceful shutdown of all tasks.

        Args:
            mission_id: The ID of the mission.
            timeout: Timeout for shutdown operations.
        """
        logger.info(
            "TaskManager shutdown initiated, timeout: %.1fs",
            timeout,
            extra={"mission_id": mission_id, "timeout": timeout, "active_tasks": len(self.running_tasks)},
        )

        self._shutdown_event.set()

        # Mark all sessions with shutdown reason before cancellation
        for task_id, session in self.tasks_sessions.items():
            if session.cancellation_reason == CancellationReason.UNKNOWN:
                session.cancellation_reason = CancellationReason.SHUTDOWN
                logger.debug(
                    "Marking task for shutdown: '%s'",
                    task_id,
                    extra={
                        "mission_id": mission_id,
                        "task_id": task_id,
                        "cancellation_reason": CancellationReason.SHUTDOWN.value,
                    },
                )

        results = await self.cancel_all_tasks(mission_id, timeout)

        failed_tasks = [task_id for task_id, success in results.items() if not success]
        if failed_tasks:
            logger.error(
                "Failed to cancel %d tasks during shutdown: %s",
                len(failed_tasks),
                failed_tasks,
                extra={
                    "mission_id": mission_id,
                    "failed_tasks": failed_tasks,
                    "failed_count": len(failed_tasks),
                    "cancellation_reason": CancellationReason.SHUTDOWN.value,
                },
            )

        # Clean up any remaining sessions
        remaining_sessions = list(self.tasks_sessions.keys())
        if remaining_sessions:
            logger.info(
                "Cleaning up %d remaining task sessions after shutdown",
                len(remaining_sessions),
                extra={
                    "mission_id": mission_id,
                    "remaining_sessions": remaining_sessions,
                    "remaining_count": len(remaining_sessions),
                },
            )
            cleanup_coros = [self._cleanup_task(task_id, mission_id) for task_id in remaining_sessions]
            await asyncio.gather(*cleanup_coros, return_exceptions=True)

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

    async def __aenter__(self) -> Self:
        """Enter async context manager.

        Returns:
            Self for use in async with statements.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Exit async context manager and clean up resources.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        await self.shutdown(mission_id="context_manager_cleanup")
