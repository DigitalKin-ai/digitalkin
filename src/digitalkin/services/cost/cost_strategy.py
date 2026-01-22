"""This module contains the abstract base class for cost strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.base_strategy import BaseStrategy
from digitalkin.models.services.cost import AmountLimit, CostConfig, CostData, CostType, QuantityLimit


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
    async def create(
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
        return await super().create()

    @abstractmethod
    async def list(
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
        return await super().list()

    # ════════════════════════════════ Public Methods ════════════════════════════════ #

    @abstractmethod
    async def list_config(self) -> list[CostConfig]:
        """Get cost configuration from the database.

        Returns:
            List of CostConfig objects from the database.
        """
        msg = "List cost config method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def set_config(self, configs: list[CostConfig]) -> bool:
        """Store cost configuration in the database.

        Args:
            configs: List of CostConfig objects to store.

        Returns:
            True if successfully stored.
        """
        msg = "Set cost config method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def set_limits(self, limits: list[QuantityLimit | AmountLimit]) -> None:
        """Set cost limits for this session.

        Args:
            limits: List of CostLimit objects to enforce.
        """
        msg = "Set limits method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def check_limit(self, cost_config_name: str, quantity: float) -> bool:
        """Check if adding this cost would exceed any limits.

        Args:
            cost_config_name: Name of the cost config.
            quantity: Quantity to add.

        Returns:
            True if within limits, False if would exceed.
        """
        msg = "Check limit method not implemented yet."
        raise NotImplementedError(msg)

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        return await super().get()

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        return await super().search()

    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        return await super().delete()

    async def update(self, *args: Any, **kwargs: Any) -> Any:
        return await super().update()

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        return await super().upload()
