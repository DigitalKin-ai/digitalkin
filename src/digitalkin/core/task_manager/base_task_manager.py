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
from digitalkin.models.settings.task_manager import get_task_manager_settings
from digitalkin.modules._base_module import BaseModule


class BaseTaskManager(ABC):
    """Shared task orchestration, signaling, and cancellation logic.

    Subclasses implement local or remote execution strategies.
    """

    tasks: dict[str, asyncio.Task]
    tasks_sessions: dict[str, TaskSession]
    default_timeout: float
    _shutdown_event: asyncio.Event
    _tasks_lock: asyncio.Lock

    def __init__(self, default_timeout: float = 300.0) -> None:
        """Initialize task manager properties.

        Args:
            default_timeout: Default timeout for task operations in seconds.
        """
        settings = get_task_manager_settings()
        self.tasks = {}
        self.tasks_sessions = {}
        self.default_timeout = default_timeout
        self._shutdown_event = asyncio.Event()
        self._tasks_lock = asyncio.Lock()
        self._task_slot = asyncio.Semaphore(settings.max_concurrent_tasks)
        self._active_slots = 0
        self._system_gate = asyncio.Semaphore(settings.max_concurrent_tasks + settings.max_queued_tasks)
        self._waiting_count = 0

        logger.info(
            "%s initialized (max_concurrent_tasks=%d, max_queued=%d, default_timeout=%.1fs)",
            self.__class__.__name__,
            settings.max_concurrent_tasks,
            settings.max_queued_tasks,
            default_timeout,
        )

    @property
    def max_concurrent_tasks(self) -> int:
        """Maximum number of concurrent tasks (from ``TaskManagerSettings``)."""
        return get_task_manager_settings().max_concurrent_tasks

    @property
    def task_count(self) -> int:
        """Number of active tasks (pending or running)."""
        return sum(1 for s in list(self.tasks_sessions.values()) if s.status in {"pending", "running"})

    @property
    def running_tasks(self) -> set[str]:
        """IDs of currently running tasks."""
        return {task_id for task_id, task in list(self.tasks.items()) if not task.done()}

    async def _cleanup_task(self, task_id: str, mission_id: str) -> None:
        """Drain in-flight writes, pop the session, release slot. Idempotent.

        Args:
            task_id: Task to clean up.
            mission_id: Mission associated with the task.
        """
        session = self.tasks_sessions.get(task_id)
        if session is not None:
            # Close stream under the write lock so pending writes see stream_closed.
            async with session._write_lock:  # noqa: SLF001
                session.close_stream()

        session = self.tasks_sessions.pop(task_id, None)
        self.tasks.pop(task_id, None)

        if session is None:
            return

        cancellation_reason = session.cancellation_reason.value
        final_status = session.status

        try:
            await session.cleanup()
        except Exception:
            logger.exception(
                "Session cleanup failed",
                extra={"mission_id": mission_id, "task_id": task_id},
            )
        finally:
            self._active_slots -= 1
            self._task_slot.release()
            if get_task_manager_settings().max_queued_tasks > 0:
                self._system_gate.release()
            logger.info(
                "Task cleaned up (%d remaining) final_status=%s cancellation_reason=%s",
                len(self.tasks_sessions),
                final_status,
                cancellation_reason,
                extra={"mission_id": mission_id, "task_id": task_id},
            )

    async def _validate_task_creation(self, task_id: str, mission_id: str, coro: Coroutine[Any, Any, None]) -> None:
        """Validate task creation preconditions.

        Args:
            task_id: The ID of the task to create
            mission_id: The ID of the mission associated with the task
            coro: The coroutine to execute

        Raises:
            ValueError: If task_id already exists
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

    async def _acquire_task_slot(self, coro: Coroutine[Any, Any, None]) -> None:
        """Acquire a task slot, queueing if necessary.

        Args:
            coro: The coroutine to close if admission is denied.

        Raises:
            RuntimeError: If the system is at full capacity.
        """
        if get_task_manager_settings().max_queued_tasks > 0:
            await self._acquire_with_queue(coro)
        else:
            await self._acquire_direct(coro)

    async def _acquire_direct(self, coro: Coroutine[Any, Any, None]) -> None:
        """Legacy path: single semaphore with timeout (DIGITALKIN_TASK_MANAGER_MAX_QUEUED_TASKS=0).

        Raises:
            RuntimeError: If no slot becomes available within the timeout.
        """
        settings = get_task_manager_settings()
        try:
            await asyncio.wait_for(self._task_slot.acquire(), timeout=settings.task_wait_timeout)
        except asyncio.TimeoutError:
            coro.close()
            msg = (
                f"Maximum concurrent tasks ({settings.max_concurrent_tasks}) reached, "
                f"waited {settings.task_wait_timeout}s"
            )
            raise RuntimeError(msg) from None

        self._active_slots += 1
        max_conc = settings.max_concurrent_tasks
        available = max_conc - self._active_slots
        if available < max_conc * 2 // 10:
            logger.warning(
                "Task slot capacity low: %d/%d available",
                available,
                max_conc,
            )

    async def _acquire_with_queue(self, coro: Coroutine[Any, Any, None]) -> None:
        """Queue-based admission: enter system gate, then wait for execution slot.

        Raises:
            RuntimeError: If the system is at full capacity.
        """
        settings = get_task_manager_settings()
        max_conc = settings.max_concurrent_tasks
        total_capacity = max_conc + settings.max_queued_tasks

        try:
            await asyncio.wait_for(self._system_gate.acquire(), timeout=settings.admission_timeout)
        except asyncio.TimeoutError:
            coro.close()
            msg = (
                f"System at full capacity ({total_capacity} tasks admitted), "
                f"rejected after {settings.admission_timeout}s"
            )
            raise RuntimeError(msg) from None

        self._waiting_count += 1
        if self._waiting_count > 0:
            logger.info(
                "Task queued for execution (%d waiting, %d/%d slots busy)",
                self._waiting_count,
                self._active_slots,
                max_conc,
            )
        try:
            await asyncio.wait_for(self._task_slot.acquire(), timeout=settings.queue_slot_timeout)
        except asyncio.TimeoutError:
            self._system_gate.release()
            coro.close()
            msg = f"Queued task waited {settings.queue_slot_timeout}s for execution slot, giving up"
            raise RuntimeError(msg) from None
        except BaseException:
            self._system_gate.release()
            coro.close()
            raise
        finally:
            self._waiting_count -= 1

        self._active_slots += 1
        available = max_conc - self._active_slots
        if available < max_conc * 2 // 10:
            logger.warning(
                "Task slot capacity low: %d/%d available",
                available,
                max_conc,
            )

    def _release_admission(self) -> None:
        """Undo one admission acquired by ``_acquire_task_slot`` but never registered.

        Mirrors ``_cleanup_task``'s slot accounting — releases the execution slot,
        decrements the active counter, and frees the system gate.
        """
        self._active_slots -= 1
        self._task_slot.release()
        if get_task_manager_settings().max_queued_tasks > 0:
            self._system_gate.release()

    def _create_session(
        self,
        task_id: str,
        mission_id: str,
        module: BaseModule,
    ) -> TaskSession:
        """Create task session.

        Args:
            task_id: The ID of the task
            mission_id: The ID of the mission
            module: The module instance

        Returns:
            TaskSession instance
        """
        session = TaskSession(
            task_id=task_id,
            mission_id=mission_id,
            module=module,
        )
        self.tasks_sessions[task_id] = session
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
            task_id: Unique identifier for the task
            mission_id: Mission identifier
            module: Module instance to execute
            coro: Coroutine to execute

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
            "Sending signal '%s' to task '%s'",
            signal_type,
            task_id,
            extra={"mission_id": mission_id, "task_id": task_id},
        )

        session = self.tasks_sessions[task_id]
        if session.signal_service is None:
            logger.warning(
                "Cannot send signal - task has no signal_service (config-setup session?): '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id, "signal_type": signal_type},
            )
            return False
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
            await self._cleanup_task(task_id, mission_id)
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
            await self.send_signal(task_id, mission_id, "cancel", {})
            await asyncio.wait_for(task, timeout=timeout)

            logger.info(
                "Task cancelled gracefully: '%s'", task_id, extra={"mission_id": mission_id, "task_id": task_id}
            )
        except asyncio.TimeoutError:
            if task_id in self.tasks_sessions:
                session = self.tasks_sessions[task_id]
                if session.cancellation_reason == CancellationReason.UNKNOWN:
                    session.cancellation_reason = CancellationReason.TIMEOUT

            logger.warning(
                "Graceful cancellation timed out for task '%s', forcing cancellation",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id},
            )

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
        """Force cleanup of task session, cancelling the task if still running.

        Args:
            task_id: The ID of the task
            mission_id: The ID of the mission

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

        if (task := self.tasks.get(task_id)) is not None and not task.done():
            await self.cancel_task(mission_id=mission_id, task_id=task_id)
        else:
            await self._cleanup_task(task_id, mission_id)

        logger.info("Cleaning up session for task: '%s'", task_id, extra={"mission_id": mission_id, "task_id": task_id})
        return True

    async def cancel_all_tasks(self, mission_id: str, timeout: float | None = None) -> dict[str, bool | BaseException]:
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
            mission_id: The ID of the mission
            timeout: Timeout for shutdown operations
        """
        logger.info(
            "TaskManager shutdown initiated, timeout: %.1fs",
            timeout,
            extra={"mission_id": mission_id, "timeout": timeout, "active_tasks": len(self.running_tasks)},
        )

        self._shutdown_event.set()

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
            Self for use in async with statements
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
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        await self.shutdown(mission_id="context_manager_cleanup")
