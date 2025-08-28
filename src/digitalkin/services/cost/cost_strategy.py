"""This module contains the abstract base class for cost strategies."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Literal

from pydantic import BaseModel

from digitalkin.services.base_strategy import BaseStrategy


class CostType(Enum):
    """Enum defining the types of costs that can be registered."""

    OTHER = "OTHER"
    TOKEN_INPUT = "TOKEN_INPUT"
    TOKEN_OUTPUT = "TOKEN_OUTPUT"
    API_CALL = "API_CALL"
    STORAGE = "STORAGE"
    TIME = "TIME"


class CostConfig(BaseModel):
    """Pydantic model that defines a cost configuration.

    :param cost_name: Name of the cost (unique identifier in the service).
    :param cost_type: The type/category of the cost.
    :param description: A short description of the cost.
    :param unit: The unit of measurement (e.g. token, call, MB).
    :param rate: The cost per unit (e.g. dollars per token).
    """

    cost_name: str
    cost_type: Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"]
    description: str | None = None
    unit: str
    rate: float


class CostData(BaseModel):
    """Data model for cost operations."""

    cost: float
    mission_id: str
    name: str
    cost_type: CostType
    unit: str
    rate: float
    setup_version_id: str
    quantity: float


class CostServiceError(Exception):
    """Custom exception for CostService errors."""


class CostStrategy(BaseStrategy, ABC):
    """Abstract base class for cost strategies."""

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, CostConfig],
    ) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version this strategy is associated with
            config: Configuration dictionary for the strategy
        """
        super().__init__(mission_id, setup_id, setup_version_id)
        self.config = config

    @abstractmethod
    def add(
        self,
        name: str,
        cost_config_name: str,
        quantity: float,
    ) -> None:
        """Register a new cost."""

    @abstractmethod
    def get(
        self,
        name: str,
    ) -> list[CostData]:
        """Get a cost."""

    @abstractmethod
    def get_filtered(
        self,
        names: list[str] | None = None,
        cost_types: list[Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"]] | None = None,
    ) -> list[CostData]:
        """Get filtered costs."""
