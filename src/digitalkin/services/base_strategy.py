"""This module contains the abstract base class for storage strategies."""

from abc import ABC


class BaseStrategy(ABC):
    """Abstract base class for all strategies.

    This class defines the interface for all strategies.
    """

    def __init__(self, mission_id: str) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
        """
        self.mission_id: str = mission_id
