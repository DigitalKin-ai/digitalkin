"""This module contains the abstract base class for cost strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy
from digitalkin.services.cost.cost_models import CostConfig, CostData, CostType


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

    # ════════════════════════════════ Overriding Methods ════════════════════════════════ #

    @abstractmethod
    def create(
        self,
        name: str,
        cost_config_name: str,
        quantity: float,
    ) -> None:
        """Create a new record in the cost database.

        Args:
            name: The name of the cost
            cost_config_name: The name of the cost config
            quantity: The quantity of the cost

        Raises:
            CostServiceError: If the cost data is invalid or if the cost already exists
        """
        return super().create()

    @abstractmethod
    def list(
        self,
        names: list[str] | None = None,
            cost_types: list[CostType] | None = None,
    ) -> list[CostData]:
        """Get records from the database.

        Args:
            names: The names of the costs
            cost_types: The types of the costs

        Returns:
            list[CostData]: The list of records

        Raises:
            CostServiceError: If the cost data is invalid or if the cost does not exist
        """
        return super().list()

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return super().get()

    def search(self, *args: Any, **kwargs: Any) -> Any:
        return super().search()

    def delete(self, *args: Any, **kwargs: Any) -> Any:
        return super().delete()

    def update(self, *args: Any, **kwargs: Any) -> Any:
        return super().update()

    def upload(self, *args: Any, **kwargs: Any) -> Any:
        return super().upload()
