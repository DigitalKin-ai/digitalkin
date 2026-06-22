"""In-memory implementation of TaskManagerStrategy."""

from collections import OrderedDict
from typing import Any

from digitalkin.logger import logger
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy


class DefaultTaskManager(TaskManagerStrategy):
    """In-memory task signal service for single-process deployments."""

    _signals: OrderedDict[str, dict[str, Any]]
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
        self._signals = OrderedDict()
        self._closed = False

    async def send_signal(self, task_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Store the latest signal record for a task.

        Args:
            task_id: Unique task identifier.
            data: Signal data to upsert.

        Returns:
            The upserted record.
        """
        self._signals[task_id] = data
        self._signals.move_to_end(task_id)
        if len(self._signals) > 10000:  # noqa: PLR2004
            evicted, _ = self._signals.popitem(last=False)
            logger.info(
                "[VALIDATE D5] evicted LRU signal record task_id=%s", evicted
            )  # TODO(validate): remove after prod validation
        return data

    async def close(self) -> None:
        """Clear in-memory state."""
        self._closed = True
        self._signals.clear()
