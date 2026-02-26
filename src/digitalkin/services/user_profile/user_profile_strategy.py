"""This module contains the abstract base class for UserProfile strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy, RequestContext


class UserProfileServiceError(Exception):
    """Base exception for UserProfile service errors."""


class UserProfileStrategy(BaseStrategy, ABC):
    """Abstract base class for UserProfile strategies."""

    @abstractmethod
    async def get_user_profile(self, ctx: RequestContext) -> dict[str, Any]:
        """Get user profile data.

        Args:
            ctx: Request context carrying mission/setup IDs.

        Returns:
            dict[str, Any]: User profile data

        Raises:
            UserProfileServiceError: If the user profile cannot be retrieved
        """
