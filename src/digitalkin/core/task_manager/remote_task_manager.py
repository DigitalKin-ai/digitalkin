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
            task_id: Unique identifier for the task
            mission_id: Mission identifier
            module: Module instance for metadata (not executed here)
            coro: Coroutine (will be closed - execution happens in worker)

        Raises:
            ValueError: If task_id duplicated
            RuntimeError: If task overload
        """
        await self._acquire_task_slot(coro)
        try:
            # Validate and register session atomically
            async with self._tasks_lock:
                await self._validate_task_creation(task_id, mission_id, coro)
                self._create_session(task_id, mission_id, module)

            logger.info(
                "Registering remote task: '%s'",
                task_id,
                extra={
                    "mission_id": mission_id,
                    "task_id": task_id,
                },
            )

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
            # Release semaphore if session was never registered (cleanup won't release it)
            if task_id not in self.tasks_sessions:
                self._task_slot.release()
            else:
                await self._cleanup_task(task_id, mission_id=mission_id)
            logger.error(
                "Failed to register remote task: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id, "error": str(e)},
                exc_info=True,
            )
            raise
