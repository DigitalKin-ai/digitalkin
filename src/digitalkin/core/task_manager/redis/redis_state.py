"""Redis-backed lifecycle state manager.

Writes task status transitions to Redis before updating in-memory state,
enforcing the P1 invariant: if the process is killed after the Redis write
but before the memory update, the system is consistent.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from digitalkin.core.task_manager.redis.redis_client import RedisClient  # noqa: TC001
from digitalkin.logger import logger
from digitalkin.models.settings.redis import get_redis_settings


class RedisStateManager:
    """Persists task lifecycle state to Redis hashes.

    Each task's state is stored at ``task:{task_id}`` with fields:
    status, created_at, started_at, completed_at, cancellation_reason,
    error_message, exception_traceback.
    """

    _redis_client: RedisClient

    def __init__(self, redis_client: RedisClient) -> None:
        """Initialize state manager.

        TTL comes from ``RedisSettings.task_ttl`` (env ``DIGITALKIN_REDIS_TASK_TTL``).

        Args:
            redis_client: Shared Redis connection.
        """
        self._redis_client = redis_client

    async def set_status(
        self,
        task_id: str,
        status: str,
        **fields: Any,
    ) -> None:
        """Write a status transition to Redis.

        Writes atomically via HSET before the caller updates in-memory state.

        Args:
            task_id: Unique task identifier.
            status: New status value.
            **fields: Additional fields to write (started_at, completed_at, etc.).
        """
        key = f"task:{task_id}"
        mapping: dict[str, str] = {"status": status}
        for k, v in fields.items():
            if isinstance(v, datetime):
                mapping[k] = v.isoformat()
            elif v is not None:
                mapping[k] = str(v)
        # Pipeline: HSET + EXPIRE in 1 round-trip instead of 2
        pipe = self._redis_client.pipeline()
        pipe.hset(key, mapping=mapping)  # type: ignore[arg-type]
        pipe.expire(key, get_redis_settings().task_ttl)
        await pipe.execute()
        logger.debug("RedisStateManager.set_status: task_id=%s status=%s", task_id, status)

    async def get_status(self, task_id: str) -> dict[str, str]:
        """Read current task state from Redis.

        Args:
            task_id: Unique task identifier.

        Returns:
            Dict of field-value pairs, empty if task not found.
        """
        raw = await self._redis_client.hgetall(f"task:{task_id}")
        return {k.decode(): v.decode() for k, v in raw.items()}

    async def record_exception(
        self,
        task_id: str,
        error_message: str,
        exception_traceback: str | None = None,
    ) -> None:
        """Persist exception info alongside task state.

        Args:
            task_id: Unique task identifier.
            error_message: Error message.
            exception_traceback: Optional traceback string.
        """
        key = f"task:{task_id}"
        mapping: dict[str, str] = {"error_message": error_message}
        if exception_traceback is not None:
            mapping["exception_traceback"] = exception_traceback
        pipe = self._redis_client.pipeline()
        pipe.hset(key, mapping=mapping)  # type: ignore[arg-type]
        pipe.expire(key, get_redis_settings().task_ttl)
        await pipe.execute()

    async def register_task(
        self,
        task_id: str,
        mission_id: str,
        setup_id: str = "",
        setup_version_id: str = "",
    ) -> None:
        """Register a new task with initial pending status.

        Args:
            task_id: Unique task identifier.
            mission_id: Mission this task belongs to.
            setup_id: Setup configuration ID.
            setup_version_id: Setup version ID.
        """
        now = datetime.now(tz=timezone.utc).isoformat()
        await self.set_status(
            task_id,
            "pending",
            mission_id=mission_id,
            setup_id=setup_id,
            setup_version_id=setup_version_id,
            created_at=now,
        )
