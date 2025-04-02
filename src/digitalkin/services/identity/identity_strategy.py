"""This module contains the abstract base class for identity strategies."""

from abc import ABC, abstractmethod

from digitalkin.services.base_strategy import BaseStrategy


class IdentityStrategy(BaseStrategy, ABC):
    """IdentityStrategy is the abstract base class for all identity strategies."""

    @abstractmethod
    async def get_identity(self) -> str:
        """Get the identity."""
        raise NotImplementedError
