"""Abstract interface for task manager signal management."""

from abc import ABC, abstractmethod
from typing import Any


class TaskManagerStrategy(ABC):
    """Abstract strategy for task manager signal management.

    Defines the contract for sending signals and closing the transport.
    Receiving signals is handled directly by
    ``SharedRedisListener.dispatch_signal`` — no per-task subscription
    consumer is exposed through this interface.
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
    async def close(self) -> None:
        """Close the signal service and release resources."""
