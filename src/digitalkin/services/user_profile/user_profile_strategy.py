"""This module contains the abstract base class for UserProfile strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy


class UserProfileStrategy(BaseStrategy, ABC):
    """Abstract base class for UserProfile strategies."""

    @abstractmethod
    async def get_user_profile(self) -> dict[str, Any] | None:
        """Get user profile data.

        The returned dict carries the profile fields plus ``mission_cost``: the total
        the mission has accumulated so far, in the same unit as the cost service.

        Returns:
            User profile data, or None if not found.

        Raises:
            UserProfileServiceError: If the service call fails (not for missing profiles).
        """

    @abstractmethod
    async def check_resource_access(self, resource_type: int, resource_id: str) -> bool:
        """Check whether the caller may access a resource.

        Args:
            resource_type: The ResourceType enum value (e.g. RESOURCE_TYPE_SETUP).
            resource_id: The resource identifier (e.g. the setup_id).

        Returns:
            True if access is granted, False otherwise.

        Raises:
            UserProfileServiceError: If the service call fails.
        """
