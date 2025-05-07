"""Default cost."""

import logging
from typing import Literal

from digitalkin.services.cost.cost_strategy import CostConfig, CostData, CostServiceError, CostStrategy, CostType

logger = logging.getLogger(__name__)


class DefaultCost(CostStrategy):
    """Default cost strategy."""

    def __init__(self, mission_id: str, setup_version_id: str, config: dict[str, CostConfig]) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_version_id: The ID of the setup version this strategy is associated with
            config: The configuration dictionary for the cost
        """
        super().__init__(mission_id=mission_id, setup_version_id=setup_version_id, config=config)
        self.db: dict[str, list[CostData]] = {}

    def add(
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
        cost_config = self.config.get(cost_config_name)
        if cost_config is None:
            msg = f"Cost config {cost_config_name} not found in the configuration."
            logger.error(msg)
            raise CostServiceError(msg)
        cost_data = CostData.model_validate({
            "name": name,
            "cost": cost_config.rate * quantity,
            "unit": cost_config.unit,
            "cost_type": getattr(CostType, cost_config.cost_type),
            "mission_id": self.mission_id,
            "rate": cost_config.rate,
            "quantity": quantity,
            "setup_version_id": self.setup_version_id,
        })
        if cost_data.mission_id not in self.db:
            self.db[cost_data.mission_id] = []
        if cost_data.name in [cost.name for cost in self.db[cost_data.mission_id]]:
            msg = f"Cost with name {cost_data.name} already exists in mission {cost_data.mission_id}"
            logger.error(msg)
            raise CostServiceError(msg)
        self.db[cost_data.mission_id].append(cost_data)

    def get(self, name: str) -> list[CostData]:
        """Get a record from the database.

        Args:
            name: The name of the cost

        Returns:
            list[CostData]: The cost data

        Raises:
            CostServiceError: If the cost data is invalid or if the cost does not exist
        """
        if self.mission_id not in self.db:
            msg = f"Mission {self.mission_id} not found in the database."
            logger.warning(msg)
            raise CostServiceError(msg)

        return [cost for cost in self.db[self.mission_id] if cost.name == name] or []

    def get_filtered(
        self,
        names: list[str] | None = None,
        cost_types: list[Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"]] | None = None,
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
        if self.mission_id not in self.db:
            msg = f"Mission {self.mission_id} not found in the database."
            logger.warning(msg)
            raise CostServiceError(msg)

        return [
            cost
            for cost in self.db[self.mission_id]
            if (names and cost.name in names) or (cost_types and cost.cost_type in cost_types)
        ]
