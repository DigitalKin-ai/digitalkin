"""In-memory implementation of TaskManagerStrategy."""

import asyncio
import contextlib
import uuid
from collections.abc import AsyncGenerator
from typing import Any

from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy


class DefaultTaskManager(TaskManagerStrategy):
    """In-memory task signal service for single-process deployments."""

    _signals: dict[str, dict[str, Any]]
    _subscribers: dict[str, asyncio.Queue[dict[str, Any] | None]]
    _closed: bool

    def __init__(
        self,
        mission_id: str = "",  # noqa: ARG002
        setup_id: str = "",  # noqa: ARG002
        setup_version_id: str = "",  # noqa: ARG002
    ) -> None:
        """Initialize in-memory signal store.

        Args:
            mission_id: Mission identifier (unused, required by init_strategy convention).
            setup_id: Setup identifier (unused, required by init_strategy convention).
            setup_version_id: Setup version identifier (unused, required by init_strategy convention).
        """
        self._signals = {}
        self._subscribers = {}
        self._closed = False

    async def send_signal(self, task_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Create or update a signal record and broadcast to subscribers.

        Args:
            task_id: Unique task identifier.
            data: Signal data to upsert.

        Returns:
            The upserted record.
        """
        self._signals[task_id] = data
        for queue in self._subscribers.values():
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(data)
        return data

    async def subscribe_signals(self, task_id: str = "") -> tuple[str, AsyncGenerator[dict[str, Any], None]]:  # noqa: ARG002
        """Subscribe to signal updates via an in-memory queue.

        Args:
            task_id: Task identifier (unused in local mode, broadcasts all signals).

        Returns:
            Tuple of (subscription_id, async generator of signal dicts).
        """
        sub_id = str(uuid.uuid4())
        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=1000)
        self._subscribers[sub_id] = queue

        async def _generator() -> AsyncGenerator[dict[str, Any], None]:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item

        return sub_id, _generator()

    async def unsubscribe_signals(self, sub_id: str) -> None:
        """Unsubscribe by sending a poison pill and removing the subscriber.

        Args:
            sub_id: Subscription identifier.
        """
        if (queue := self._subscribers.pop(sub_id, None)) is not None:
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(None)

    async def close(self) -> None:
        """Poison all subscribers and clear state."""
        self._closed = True
        for sub_id in list(self._subscribers):
            await self.unsubscribe_signals(sub_id)
        self._signals.clear()
