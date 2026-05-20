"""Abstract interface for task manager signal management."""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Any


class TaskManagerStrategy(ABC):
    """Abstract strategy for task manager signal management.

    Defines the contract for upsert, subscribe, unsubscribe, and close
    operations used by TaskSession, TaskExecutor, and BaseTaskManager.
    """

    @abstractmethod
    async def send_signal(self, task_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Create or update a signal record for a task.

        Args:
            task_id: Unique task identifier.
            data: Signal data to upsert.

        Returns:
            The upserted record.
        """

    @abstractmethod
    async def subscribe_signals(self, task_id: str) -> tuple[str, AsyncGenerator[dict[str, Any], None]]:
        """Subscribe to signal updates for a specific task.

        Args:
            task_id: Unique task identifier to subscribe to.

        Returns:
            Tuple of (subscription_id, async generator of signal dicts).
        """

    @abstractmethod
    async def unsubscribe_signals(self, sub_id: str) -> None:
        """Unsubscribe from signal updates.

        Args:
            sub_id: Subscription identifier returned by subscribe_signals.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the signal service and release resources."""
