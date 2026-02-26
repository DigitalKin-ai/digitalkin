"""This module contains the abstract base class for cost strategies."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Literal

from pydantic import BaseModel

from digitalkin.models.services.cost import AmountLimit, QuantityLimit
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

    def __init__(self, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with.
            setup_id: The ID of the setup.
            setup_version_id: The ID of the setup version.
        """
        super().__init__()
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id

    @abstractmethod
    async def set_limits(self, limits: list[QuantityLimit | AmountLimit]) -> None:
        """Set cost limits for this session.

        Args:
            limits: List of CostLimit objects to enforce.
        """

    @abstractmethod
    async def check_limit(self, cost_config_name: str, quantity: float) -> bool:
        """Check if adding this cost would exceed any limits.

        Args:
            cost_config_name: Name of the cost config.
            quantity: Quantity to add.

        Returns:
            True if within limits, False if would exceed.
        """

    @abstractmethod
    async def add(
        self,
        name: str,
        cost_config_name: str,
        quantity: float,
    ) -> None:
        """Register a new cost."""

    @abstractmethod
    async def get(
        self,
        name: str,
    ) -> list[CostData]:
        """Get a cost."""

    @abstractmethod
    async def get_filtered(
        self,
        names: list[str] | None = None,
        cost_types: list[Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"]] | None = None,
    ) -> list[CostData]:
        """Get filtered costs."""

    @abstractmethod
    async def get_cost_config(self) -> list[CostConfig]:
        """Get cost configuration for the current setup version.

        Returns:
            List of CostConfig objects from the database.
        """

    @abstractmethod
    async def set_cost_config(self, configs: list[CostConfig]) -> bool:
        """Store cost configuration for the current setup version.

        Args:
            configs: List of CostConfig objects to store.

        Returns:
            True if successfully stored.
        """
