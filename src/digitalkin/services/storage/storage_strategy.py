"""This module contains the abstract base class for storage strategies."""

import datetime
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any

from pydantic import BaseModel


class DataType(Enum):
    """."""

    OUTPUT = auto()
    VIEW = auto()


class StorageData(BaseModel):
    """."""

    data: dict[str, Any]
    mission_id: str
    name: str
    timestamp: datetime.datetime
    type: DataType


class StorageStrategy(ABC):
    """Abstract base class for storage strategies."""

    def __init__(self) -> None:
        """Initialize the storage strategy."""

    def __post_init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize the storage strategy."""

    @abstractmethod
    def create(self, storage_dict: dict[str, Any]) -> str:
        """Create a new record in the database."""

    @abstractmethod
    def get(self, storage_dict: dict[str, Any]) -> list[StorageData]:
        """Get records from the database."""

    @abstractmethod
    def update(self, storage_dict: dict[str, Any]) -> int:
        """Update records in the database."""

    @abstractmethod
    def delete(self, storage_dict: dict[str, Any]) -> int:
        """Delete records from the database."""
