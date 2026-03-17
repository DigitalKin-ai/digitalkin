"""This module contains the abstract base class for UserProfile strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy


class UserProfileServiceError(Exception):
    """Base exception for UserProfile service errors."""


class UserProfileStrategy(BaseStrategy, ABC):
    """Abstract base class for UserProfile strategies."""

    @abstractmethod
    async def get_user_profile(self) -> dict[str, Any] | None:
        """Get user profile data.

        Returns:
            User profile data, or None if not found.

        Raises:
            UserProfileServiceError: If the service call fails (not for missing profiles).
        """
