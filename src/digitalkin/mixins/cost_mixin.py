"""Cost Mixin to ease trigger deveolpment."""

from typing import Literal

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.services.cost.cost_strategy import CostData


class CostMixin:
    """Mixin providing cost tracking operations through the cost strategy.

    This mixin wraps cost strategy calls to provide a cleaner API
    for cost tracking in trigger handlers.
    """

    @staticmethod
    def add_cost(context: ModuleContext, name: str, cost_config_name: str, quantity: float) -> None:
        """Add a cost entry using the cost strategy.

        Args:
            context: Module context containing the cost strategy
            name: Name/identifier for this cost entry
            cost_config_name: Name of the cost configuration to use
            quantity: Quantity of units consumed

        Raises:
            CostServiceError: If cost addition fails
        """
        return context.cost.add(name, cost_config_name, quantity)

    @staticmethod
    def get_cost(context: ModuleContext, name: str) -> list[CostData]:
        """Get cost entries for a specific name.

        Args:
            context: Module context containing the cost strategy
            name: Name/identifier to get costs for

        Returns:
            List of cost data entries

        Raises:
            CostServiceError: If cost retrieval fails
        """
        return context.cost.get(name)

    @staticmethod
    def get_costs(
        context: ModuleContext,
        names: list[str] | None = None,
        cost_types: list[
            Literal[
                "TOKEN_INPUT",
                "TOKEN_OUTPUT",
                "API_CALL",
                "STORAGE",
                "TIME",
                "OTHER",
            ]
        ]
        | None = None,
    ) -> list[CostData]:
        """Get filtered cost entries.

        Args:
            context: Module context containing the cost strategy
            names: Optional list of names to filter by
            cost_types: Optional list of cost types to filter by

        Returns:
            List of filtered cost data entries

        Raises:
            CostServiceError: If cost retrieval fails
        """
        return context.cost.get_filtered(names, cost_types)
