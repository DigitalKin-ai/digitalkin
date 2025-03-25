"""This module contains the abstract base class for storage strategies."""

from abc import ABC, abstractmethod
from typing import Any


class StorageStrategy(ABC):
    """Abstract base class for storage strategies."""

    def __init__(self) -> None:
        """Initialize the storage strategy."""

    def __post_init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize the storage strategy."""

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the database."""

    @abstractmethod
    def disconnect(self) -> bool:
        """Close connection to the database."""

    @abstractmethod
    def create(self, table: str, data: dict[str, Any]) -> str:
        """Create a new record in the database."""

    @abstractmethod
    def get(self, table: str, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Get records from the database."""

    @abstractmethod
    def update(self, table: str, data: dict[str, Any]) -> int:
        """Update records in the database."""

    @abstractmethod
    def delete(self, table: str, data: dict[str, Any]) -> int:
        """Delete records from the database."""

    @abstractmethod
    def get_all(self) -> dict[str, list[dict[str, Any]]]:
        """Get all records from the database."""
