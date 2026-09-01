"""Local task manager for single-process execution."""

from collections.abc import Coroutine
from typing import Any

from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
from digitalkin.core.task_manager.task_executor import TaskExecutor
from digitalkin.logger import logger
from digitalkin.models.settings.task_manager import get_task_manager_settings
from digitalkin.modules._base_module import BaseModule


class LocalTaskManager(BaseTaskManager):
    """Task manager for local execution in the same process.

    Executes tasks locally using TaskExecutor with the supervisor pattern.
    Suitable for single-server deployments and development.
    """

    _executor: TaskExecutor

    def __init__(self, default_timeout: float = 10.0) -> None:
        """Initialize local task manager.

        Args:
            default_timeout: Default timeout for task operations in seconds
        """
        super().__init__(default_timeout)
        self._executor = TaskExecutor()

    async def create_task(
        self,
        task_id: str,
        mission_id: str,
        module: BaseModule,
        coro: Coroutine[Any, Any, None],
    ) -> None:
        """Create and execute a task locally using TaskExecutor.

        Args:
            task_id: Unique identifier for the task
            mission_id: Mission identifier
            module: Module instance to execute
            coro: Coroutine to execute

        Raises:
            ValueError: If task_id duplicated
            RuntimeError: If task overload
        """
        await self._acquire_task_slot(coro)
        registered = False
        try:  # noqa: PLW0717
            async with self._tasks_lock:
                await self._validate_task_creation(task_id, mission_id, coro)
                session = self._create_session(task_id, mission_id, module)
                registered = True

            logger.info(
                "Creating local task: '%s'",
                task_id,
                extra={
                    "mission_id": mission_id,
                    "task_id": task_id,
                },
            )

            async def _finalize() -> None:
                await self._cleanup_task(task_id, mission_id=mission_id)

            supervisor_task = await self._executor.execute_task(
                task_id,
                mission_id,
                coro,
                session,
                on_finalize=_finalize,
                stream_drain_timeout=get_task_manager_settings().stream_drain_timeout,
            )
            self.tasks[task_id] = supervisor_task

            logger.info(
                "Local task created and started: '%s' (total_tasks=%d)",
                task_id,
                len(self.tasks),
                extra={"mission_id": mission_id, "task_id": task_id},
            )

        except Exception:
            coro.close()
            if registered:
                await self._cleanup_task(task_id, mission_id=mission_id)
            else:
                # H2: this call never registered a session (e.g. duplicate task_id) —
                # undo only THIS call's admission; never touch the live task.
                self._release_admission()
            logger.exception(
                "Failed to create local task: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id},
            )
            raise
