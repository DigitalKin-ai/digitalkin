"""Redis pub/sub implementation of TaskManagerStrategy.

Uses SharedRedisListener for receiving signals and direct PUBLISH for sending.
Enables cross-process signal delivery between Gateway and Module via Redis.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from digitalkin.core.task_manager.redis.redis_client import RedisClient


class RedisTaskManager(TaskManagerStrategy):
    """Redis pub/sub signal delivery for embedded and standalone deployments.

    Gateway publishes signals to ``signal_ch:{task_id}`` via Redis PUBLISH.
    This strategy subscribes via SharedRedisListener and dispatches to the
    module's signal listener in TaskExecutor.

    Singleton-safe: SharedRedisListener is keyed by redis_url, so multiple
    RedisTaskManager instances sharing the same RedisClient reuse one listener.
    """

    _redis_client: RedisClient
    _listener: SharedRedisListener
    _redis_url: str

    def __init__(self, redis_client: RedisClient, redis_url: str = "default") -> None:
        """Initialize Redis-backed signal service.

        Args:
            redis_client: Shared Redis connection pool.
            redis_url: Key for SharedRedisListener singleton lookup.
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

    async def subscribe_signals(self, task_id: str) -> tuple[str, AsyncGenerator[dict[str, Any], None]]:
        """Subscribe to signals for a task via Redis pub/sub.

        Registers with SharedRedisListener which subscribes to
        ``signal_ch:{task_id}`` and dispatches to a per-task queue.

        Args:
            task_id: Unique task identifier.

        Returns:
            Tuple of (task_id as sub_id, async generator of signal dicts).
        """
        queue = await self._listener.register(task_id)

        async def _signal_generator() -> AsyncGenerator[dict[str, Any], None]:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item

        return task_id, _signal_generator()

    async def unsubscribe_signals(self, sub_id: str) -> None:
        """Unsubscribe from signals for a task.

        Args:
            sub_id: The task_id used as subscription identifier.
        """
        self._listener.unregister(sub_id)

    async def close(self) -> None:
        """Release the shared listener reference."""
        await SharedRedisListener.release(self._redis_url)
