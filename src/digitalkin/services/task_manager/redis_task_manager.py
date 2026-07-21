"""Redis pub/sub implementation of TaskManagerStrategy.

Uses direct PUBLISH for sending. Receiving is owned by
``SharedRedisListener`` (registered from ``TaskExecutor`` per task) —
this strategy only holds the listener ref so it's kept alive while the
process has at least one active task manager.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient


class RedisTaskManager(TaskManagerStrategy):
    """Redis pub/sub signal sender for embedded and standalone deployments.

    Gateway publishes signals to ``signal_ch:{task_id}`` via Redis PUBLISH;
    this class is the sender side. The receiver side is
    ``SharedRedisListener.dispatch_signal`` invoked from the listener
    loop — registration happens in ``TaskExecutor.execute_task``.

    Singleton-safe: ``SharedRedisListener`` is keyed by ``redis_url``, so
    multiple ``RedisTaskManager`` instances sharing the same
    ``RedisClient`` reuse one listener.
    """

    _redis_client: RedisClient
    _listener: SharedRedisListener
    _redis_url: str

    def __init__(self, redis_client: RedisClient, redis_url: str = "default") -> None:
        """Initialize Redis-backed signal service.

        Args:
            redis_client: Shared Redis connection pool.
            redis_url: Key for ``SharedRedisListener`` singleton lookup.
        """
        self._redis_client = redis_client
        self._redis_url = redis_url
        self._listener = SharedRedisListener.get_or_create(redis_url, redis_client)

    async def send_signal(self, task_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Publish a signal to Redis pub/sub.

        Args:
            task_id: Unique task identifier.
            data: Signal data (action, task_id, etc.).

        Returns:
            The signal data as sent.
        """
        payload = json.dumps(data, default=str)
        await self._redis_client.publish(f"signal_ch:{task_id}", payload)
        return data

    async def close(self) -> None:
        """Release the shared listener reference."""
        await SharedRedisListener.release(self._redis_url)
