"""This module contains the abstract base class for agent strategies."""

from abc import ABC, abstractmethod

from digitalkin.services.base_strategy import BaseStrategy


class AgentStrategy(BaseStrategy, ABC):
    """Abstract base class for agent strategies."""

    @abstractmethod
    def start(self) -> None:
        """Start the agent."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stop the agent."""
        raise NotImplementedError
