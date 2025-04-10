"""This module contains the abstract base class for registry strategies."""

from abc import ABC, abstractmethod

from digitalkin.services.base_strategy import BaseStrategy


class RegistryStrategy(BaseStrategy, ABC):
    """Abstract base class for registry strategies."""

    @abstractmethod
    def get_by_id(self, module_id: str) -> None:
        """Get services from the registry."""
        raise NotImplementedError
