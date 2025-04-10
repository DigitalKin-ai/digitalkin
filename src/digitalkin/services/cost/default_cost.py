"""Default cost."""

import logging
from typing import Any

from digitalkin.services.cost.cost_strategy import CostData, CostStrategy

logger = logging.getLogger(__name__)


class DefaultCost(CostStrategy):
    """Default cost strategy."""

    def add_cost(self, cost_dict: dict[str, Any]) -> str:  # noqa: PLR6301
        """Create a new record in the cost database.

        Returns:
            str: The ID of the new record
        """
        logger.info("Cost added with cost_dict: %s", cost_dict)
        return ""

    def get(self, cost_dict: dict[str, Any]) -> list[CostData]:  # noqa: PLR6301
        """Get records from the database.

        Returns:
            list[CostData]: The list of records
        """
        logger.info("Costs querried with cost_dict: %s", cost_dict)
        return []
