"""This module contains the abstract base class for Secret strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy


class SecretStrategy(BaseStrategy, ABC):
    """Abstract base class for Secret strategies."""

    @abstractmethod
    async def get_secret(self) -> dict[str, Any] | None:
        """Resolve the secret object attached to this setup.

        Returns:
            The secret values (matching the module's secret_schema), or None if not found.

        Raises:
            SecretServiceError: If the service call fails.
        """
