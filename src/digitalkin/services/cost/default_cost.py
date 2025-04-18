"""Default cost."""

import logging
from typing import Literal

from digitalkin.services.cost.cost_strategy import CostData, CostServiceError, CostStrategy

logger = logging.getLogger(__name__)


class DefaultCost(CostStrategy):
    """Default cost strategy."""

    def __init__(self, mission_id: str) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
        """
        super().__init__(mission_id)
        self.db: dict[str, list[CostData]] = {}

    def add(
        self,
        name: str,
        cost: float,
        unit: str,
        cost_type: Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"],
    ) -> None:
        """Create a new record in the cost database.

        Args:
            name: The name of the cost
            cost: The cost value
            unit: The unit of the cost
            cost_type: The type of the cost

        Raises:
            CostServiceError: If the cost data is invalid or if the cost already exists
        """
        cost_data = CostData.model_validate({
            "name": name,
            "cost": cost,
            "unit": unit,
            "cost_type": cost_type,
            "mission_id": self.mission_id,
        })
        if cost_data.mission_id not in self.db:
            self.db[cost_data.mission_id] = []
        if cost_data.name in [cost.name for cost in self.db[cost_data.mission_id]]:
            msg = f"Cost with name {cost_data.name} already exists in mission {cost_data.mission_id}"
            logger.error(msg)
            raise CostServiceError(msg)
        self.db[cost_data.mission_id].append(cost_data)

    def get(self, names: list[str] | None = None, cost_type: str | None = None) -> list[CostData]:
        """Get records from the database.

        Args:
            names: The names of the costs
            cost_type: The type of the cost

        Returns:
            list[CostData]: The list of records

        Raises:
            CostServiceError: If neither names nor cost_type is provided or if the mission doesn't exist
        """
        if self.mission_id not in self.db:
            msg = f"Mission {self.mission_id} not found in the database."
            logger.warning(msg)
            return []

        if names:
            return [cost for cost in self.db[self.mission_id] if cost.name in names]

        if cost_type:
            return [cost for cost in self.db[self.mission_id] if cost.cost_type == cost_type]

        msg = "At least one of 'names' or 'cost_type' must be provided."
        logger.error(msg)
        raise CostServiceError(msg)

    def get_all(self) -> list[CostData]:
        """Get all records from the database.

        Returns:
            list[CostData]: The list of all records
        """
        if self.mission_id not in self.db:
            msg = f"Mission {self.mission_id} not found in the database."
            logger.warning(msg)
            return []

        return self.db[self.mission_id]
