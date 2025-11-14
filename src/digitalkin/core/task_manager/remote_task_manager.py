"""Remote task manager for distributed execution."""

import datetime
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
        heartbeat_interval: datetime.timedelta = datetime.timedelta(seconds=2),
        connection_timeout: datetime.timedelta = datetime.timedelta(seconds=5),
    ) -> None:
        """Register task for remote execution (metadata only).

        Creates TaskSession for signal handling and monitoring, but doesn't execute the coroutine.
        The coroutine will be recreated and executed by a remote worker.

        Args:
            task_id: Unique identifier for the task
            mission_id: Mission identifier
            module: Module instance for metadata (not executed here)
            coro: Coroutine (will be closed - execution happens in worker)
            heartbeat_interval: Interval between heartbeats
            connection_timeout: Connection timeout for SurrealDB

        Raises:
            ValueError: If task_id duplicated
            RuntimeError: If task overload
        """
        # Validation
        await self._validate_task_creation(task_id, mission_id, coro)

        logger.info(
            "Registering remote task: '%s'",
            task_id,
            extra={
                "mission_id": mission_id,
                "task_id": task_id,
                "heartbeat_interval": heartbeat_interval,
                "connection_timeout": connection_timeout,
            },
        )

        try:
            # Create session for metadata and signal handling
            _channel, _session = await self._create_session(
                task_id, mission_id, module, heartbeat_interval, connection_timeout
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
            logger.error(
                "Failed to register remote task: '%s'",
                task_id,
                extra={"mission_id": mission_id, "task_id": task_id, "error": str(e)},
                exc_info=True,
            )
            # Cleanup on failure
            await self._cleanup_task(task_id, mission_id=mission_id)
            raise
