"""This module contains the abstract base class for cost strategies."""

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Literal

from pydantic import BaseModel

from digitalkin.services.base_strategy import BaseStrategy


class CostType(Enum):
    """Enum defining the types of costs that can be registered."""

    OTHER = auto()
    TOKEN_INPUT = auto()
    TOKEN_OUTPUT = auto()
    API_CALL = auto()
    STORAGE = auto()
    TIME = auto()


class CostData(BaseModel):
    """Data model for cost operations."""

    cost: float
    mission_id: str
    name: str
    cost_type: CostType
    unit: str


class CostServiceError(Exception):
    """Custom exception for CostService errors."""


class CostStrategy(BaseStrategy, ABC):
    """Abstract base class for cost strategies."""

    def __init__(self, mission_id: str) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
        """
        super().__init__(mission_id)

    @abstractmethod
    def add(
        self,
        name: str,
        cost: float,
        unit: str,
        cost_type: Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"],
    ) -> None:
        """Register a new cost."""

    def __post_init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Allow post init configuration."""

    @abstractmethod
    def get(
        self,
        names: list[str] | None,
        cost_type: Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"] | None,
    ) -> list[CostData]:
        """Get a cost."""

    @abstractmethod
    def get_all(self) -> list[CostData]:
        """Get all costs."""
