"""This module contains the abstract base class for storage strategies."""

from abc import ABC, abstractmethod
from typing import Any


class StorageStrategy(ABC):
    """Abstract base class for storage strategies."""

    def __init__(self) -> None:
        """Initialize the storage strategy."""

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the database."""

    @abstractmethod
    def disconnect(self) -> bool:
        """Close connection to the database."""

    @abstractmethod
    def create(self, data: dict[str, Any]) -> str:
        """Create a new record in the database."""

    @abstractmethod
    def get(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Get records from the database."""

    @abstractmethod
    def update(self, data: dict[str, Any]) -> int:
        """Update records in the database."""

    @abstractmethod
    def delete(self, data: dict[str, Any]) -> int:
        """Delete records from the database."""

    @abstractmethod
    def get_all(self) -> list[dict[str, Any]]:
        """Get all records from the database."""
