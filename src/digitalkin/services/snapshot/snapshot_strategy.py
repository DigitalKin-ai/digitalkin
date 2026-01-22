"""This module contains the abstract base class for snapshot strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy


class SnapshotStrategy(BaseStrategy, ABC):
    """Abstract base class for snapshot strategies."""

    # ════════════════════════════════ Overriding Methods ════════════════════════════════ #

    @abstractmethod
    def create(self, data: dict[str, Any]) -> str:
        """Create a new snapshot in the file system.

        Args:
            data: A dictionary containing the data needed to create the snapshot

        Returns:
            str: The ID of the new snapshot
        """

    @abstractmethod
    def list(self, data: dict[str, Any]) -> None:
        """Get snapshots from the file system.

        Args:
            data: A dictionary containing the data needed to list the snapshots

        """

    @abstractmethod
    def update(self, data: dict[str, Any]) -> int:
        """Update snapshots in the file system.

        Args:
            data: A dictionary containing the data needed to update the snapshots

        Returns:
            int: The number of snapshots updated

        """

    @abstractmethod
    def delete(self, data: dict[str, Any]) -> int:
        """Delete snapshots from the file system.

        Args:
            data: A dictionary containing the data needed to delete the snapshots

        Returns:
            int: The number of snapshots deleted

        """

    @abstractmethod
    def get_all(self) -> None:
        """Get all snapshots from the file system."""

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return super().get()

    def search(self, *args: Any, **kwargs: Any) -> Any:
        return super().search()

    def upload(self, *args: Any, **kwargs: Any) -> Any:
        return super().upload()
