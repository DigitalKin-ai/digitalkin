"""Cost Mixin to ease trigger deveolpment."""

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.services.cost import CostData


class CostMixin:
    """Mixin providing cost tracking operations through the cost strategy.

    This mixin wraps cost strategy calls to provide a cleaner API
    for cost tracking in trigger handlers.
    """

    @staticmethod
    async def create_cost(context: ModuleContext, name: str, cost_config_name: str, quantity: float) -> None:
        """Add a cost entry using the cost strategy.

        Args:
            context: Module context containing the cost strategy
            name: Name/identifier for this cost entry
            cost_config_name: Name of the cost configuration to use
            quantity: Quantity of units consumed

        Raises:
            CostServiceError: If cost addition fails
        """
        return await context.cost.create(name, cost_config_name, quantity)

    @staticmethod
    async def list_cost(context: ModuleContext, name: str) -> list[CostData]:
        """Get cost entries for a specific name.

        Args:
            context: Module context containing the cost strategy
            name: Name/identifier to get costs for

        Returns:
            List of cost data entries

        Raises:
            CostServiceError: If cost retrieval fails
        """
        return await context.cost.list(name)
