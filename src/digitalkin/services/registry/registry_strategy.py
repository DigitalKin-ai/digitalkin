"""This module contains the abstract base class for registry strategies."""

from abc import ABC, abstractmethod


class RegistryStrategy(ABC):
    """Abstract base class for registry strategies."""

    @abstractmethod
    def __init__(self) -> None:
        """Initialize the registry strategy."""
        raise NotImplementedError

    @abstractmethod
    def get_by_id(self, module_id: str) -> None:
        """Get services from the registry."""
        raise NotImplementedError
