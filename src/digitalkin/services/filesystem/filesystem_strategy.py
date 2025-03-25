"""This module contains the abstract base class for filesystem strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy


class FilesystemStrategy(BaseStrategy, ABC):
    """Abstract base class for file system strategies."""

    @abstractmethod
    def create(self, data: dict[str, Any]) -> str:
        """Create a new file in the file system."""

    @abstractmethod
    def get(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Get files from the file system."""

    @abstractmethod
    def update(self, data: dict[str, Any]) -> int:
        """Update files in the file system."""

    @abstractmethod
    def delete(self, data: dict[str, Any]) -> int:
        """Delete files from the file system."""

    @abstractmethod
    def get_all(self) -> list[dict[str, Any]]:
        """Get all files from the file system."""
