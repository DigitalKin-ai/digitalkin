"""Local task manager for single-process execution."""

from collections.abc import Coroutine
from typing import Any

from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
from digitalkin.core.task_manager.task_executor import TaskExecutor
from digitalkin.logger import logger
from digitalkin.modules._base_module import BaseModule


class LocalTaskManager(BaseTaskManager):
    """Task manager for local execution in the same process.

    Executes tasks locally using TaskExecutor with the supervisor pattern.
    Suitable for single-server deployments and development.
    """

    _executor: TaskExecutor

    def __init__(
        self,
        default_timeout: float = 10.0,
        max_concurrent_tasks: int = 100,
    ) -> None:
        """Initialize local task manager.

        Args:
            default_timeout: Default timeout for task operations in seconds
            max_concurrent_tasks: Maximum number of concurrent tasks
        """
        super().__init__(default_timeout, max_concurrent_tasks)
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
        # Validate and register session atomically to prevent TOCTOU on max_concurrent_tasks
        async with self._tasks_lock:
            await self._validate_task_creation(task_id, mission_id, coro)
            session = self._create_session(task_id, mission_id, module)

        logger.info(
            "Creating local task: '%s'",
            task_id,
            extra={
                "mission_id": mission_id,
                "task_id": task_id,
            },
        )

        try:
            # Execute task using TaskExecutor
            supervisor_task = await self._executor.execute_task(
                task_id,
                mission_id,
                coro,
                session,
            )
            self.tasks[task_id] = supervisor_task

            logger.info(
                "Local task created and started: '%s'",
                task_id,
                extra={
                    "mission_id": mission_id,
                    "task_id": task_id,
                    "total_tasks": len(self.tasks),
                },
            )

        except Exception as e:
            coro.close()
            logger.error(
                "Failed to create local task: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id, "error": str(e)},
                exc_info=True,
            )
            # Cleanup on failure
            await self._cleanup_task(task_id, mission_id=mission_id)
            raise
