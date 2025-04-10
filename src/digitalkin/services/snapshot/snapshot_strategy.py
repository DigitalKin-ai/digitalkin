"""This module contains the abstract base class for snapshot strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy


class SnapshotStrategy(BaseStrategy, ABC):
    """Abstract base class for snapshot strategies."""

    @abstractmethod
    def create(self, data: dict[str, Any]) -> str:
        """Create a new snapshot in the file system."""

    @abstractmethod
    def get(self, data: dict[str, Any]) -> None:
        """Get snapshots from the file system."""

    @abstractmethod
    def update(self, data: dict[str, Any]) -> int:
        """Update snapshots in the file system."""

    @abstractmethod
    def delete(self, data: dict[str, Any]) -> int:
        """Delete snapshots from the file system."""

    @abstractmethod
    def get_all(self) -> None:
        """Get all snapshots from the file system."""
