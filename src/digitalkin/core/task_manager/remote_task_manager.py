"""Remote task manager for distributed execution."""

from collections.abc import Coroutine
from typing import Any

from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
from digitalkin.logger import logger
from digitalkin.modules._base_module import BaseModule


class RemoteTaskManager(BaseTaskManager):
    """Task manager for distributed/remote execution.

    Only manages task metadata and signals - actual execution happens in remote workers.
    Suitable for horizontally scaled deployments with Taskiq/Celery workers.
    """

    def __init__(
        self,
        default_timeout: float = 10.0,
        max_concurrent_tasks: int = 100,
    ) -> None:
        """Initialize remote task manager.

        Args:
            default_timeout: Default timeout for task operations in seconds.
            max_concurrent_tasks: Maximum number of concurrent tasks.
        """
        super().__init__(default_timeout, max_concurrent_tasks)

    async def create_task(
        self,
        task_id: str,
        mission_id: str,
        module: BaseModule,
        coro: Coroutine[Any, Any, None],
    ) -> None:
        """Register task for remote execution (metadata only).

        Creates TaskSession for signal handling and monitoring, but doesn't execute the coroutine.
        The coroutine will be recreated and executed by a remote worker.

        Args:
            task_id: Unique identifier for the task.
            mission_id: Mission identifier.
            module: Module instance for metadata (not executed here).
            coro: Coroutine (will be closed - execution happens in worker).

        Raises:
            ValueError: If task_id duplicated.
            RuntimeError: If task overload.
        """
        await self._validate_task_creation(task_id, mission_id, coro)

        logger.info(
            "Registering remote task: '%s'",
            task_id,
            extra={"mission_id": mission_id, "task_id": task_id},
        )

        try:
            _ = await self._create_session(task_id, mission_id, module)

            # Close coroutine - worker will recreate and execute it
            coro.close()

            logger.info(
                "Remote task registered: '%s'",
                task_id,
                extra={
                    "mission_id": mission_id,
                    "task_id": task_id,
                    "total_sessions": len(self.tasks_sessions),
                },
            )

        except Exception as e:
            coro.close()
            logger.error(
                "Failed to register remote task: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id, "error": str(e)},
                exc_info=True,
            )
            await self._cleanup_task(task_id, mission_id=mission_id)
            raise
