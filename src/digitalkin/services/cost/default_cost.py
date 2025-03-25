"""Default cost."""

from .cost_strategy import CostStrategy


class DefaultCost(CostStrategy):
    """Default cost strategy."""

    def __init__(self) -> None:
        """Initialize the cost strategy."""

    def register_cost(self, data: dict[str, str]) -> None:
        """Register a new cost."""
