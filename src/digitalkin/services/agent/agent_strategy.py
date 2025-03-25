"""This module contains the abstract base class for agent strategies."""

from abc import ABC, abstractmethod


class AgentStrategy(ABC):
    """Abstract base class for agent strategies."""

    @abstractmethod
    def __init__(self) -> None:
        """Initialize the agent strategy."""
        raise NotImplementedError

    @abstractmethod
    def start(self) -> None:
        """Start the agent."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stop the agent."""
        raise NotImplementedError
