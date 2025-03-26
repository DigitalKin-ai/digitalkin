"""This module contains the abstract base class for cost strategies."""

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any

from pydantic import BaseModel


class CostType(Enum):
    """."""

    OTHER = auto()
    TOKEN_INPUT = auto()
    TOKEN_OUTPUT = auto()
    API_CALL = auto()
    STORAGE = auto()
    TIME = auto()


class CostData(BaseModel):
    """."""

    cost: float
    mission_id: str
    name: str
    type: CostType
    unit: str


class CostStrategy(ABC):
    """Abstract base class for cost strategies."""

    @abstractmethod
    def __init__(self) -> None:
        """Initialize the cost strategy."""

    def __post_init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Allow post init configuration."""

    @abstractmethod
    def add_cost(self, data: dict[str, Any]) -> str:
        """Register a new cost."""

    @abstractmethod
    def get(self, data: dict[str, Any]) -> list[CostData]:
        """Get a cost."""
