"""Default user profile implementation."""

from typing import Any

from digitalkin.logger import logger
from digitalkin.services.base_strategy import RequestContext
from digitalkin.services.user_profile.user_profile_strategy import (
    UserProfileServiceError,
    UserProfileStrategy,
)


class DefaultUserProfile(UserProfileStrategy):
    """Default user profile strategy with in-memory storage."""

    def __init__(self) -> None:
        """Initialize the strategy."""
        super().__init__()
        self.db: dict[str, dict[str, Any]] = {}

    async def get_user_profile(self, ctx: RequestContext) -> dict[str, Any]:
        """Get user profile from in-memory storage.

        Args:
            ctx: Request context carrying mission/setup IDs.

        Returns:
            dict[str, Any]: User profile data

        Raises:
            UserProfileServiceError: If the user profile is not found
        """
        if ctx.mission_id not in self.db:
            msg = f"User profile for mission {ctx.mission_id} not found in the database."
            logger.warning(msg)
            raise UserProfileServiceError(msg)

        logger.debug(f"Retrieved user profile for mission_id: {ctx.mission_id}")
        return self.db[ctx.mission_id]

    def add_user_profile(self, ctx: RequestContext, user_profile_data: dict[str, Any]) -> None:
        """Add a user profile to the in-memory database (helper for testing).

        Args:
            ctx: Request context carrying mission/setup IDs.
            user_profile_data: Dictionary containing user profile data
        """
        self.db[ctx.mission_id] = user_profile_data
        logger.debug(f"Added user profile for mission_id: {ctx.mission_id}")
