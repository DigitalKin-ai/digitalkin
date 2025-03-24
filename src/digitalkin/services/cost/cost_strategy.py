"""This module contains the abstract base class for cost strategies."""

from abc import ABC, abstractmethod


class CostStrategy(ABC):
    """Abstract base class for cost strategies."""

    @abstractmethod
    def __init__(self) -> None:
        """Initialize the cost strategy."""

    @abstractmethod
    def register_cost(self, data: dict[str, str]) -> None:
        """Register a new cost."""
