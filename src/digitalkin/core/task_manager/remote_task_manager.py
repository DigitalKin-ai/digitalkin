"""Remote task manager for distributed execution."""

from collections.abc import Coroutine
from typing import Any

from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
from digitalkin.logger import logger
from digitalkin.modules._base_module import BaseModule


class RemoteTaskManager(BaseTaskManager):
    """Task manager for distributed/remote execution.

    Only manages task metadata and signals - actual execution happens in remote workers.
    Suitable for horizontally scaled deployments with remote workers.
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
        registered = False
        try:  # noqa: PLW0717
            # Validate and register session atomically
            async with self._tasks_lock:
                await self._validate_task_creation(task_id, mission_id, coro)
                self._create_session(task_id, mission_id, module)
                registered = True

            logger.debug(
                "Registering remote task: '%s'",
                task_id,
                extra={
                    "mission_id": mission_id,
                    "task_id": task_id,
                },
            )

            # Close coroutine - worker will recreate and execute it
            coro.close()

            logger.debug(
                "Remote task registered: '%s' (total_sessions=%d)",
                task_id,
                len(self.tasks_sessions),
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
                "Failed to register remote task: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id},
            )
            raise
