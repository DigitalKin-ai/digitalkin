"""Default user profile implementation."""

from typing import Any

from digitalkin.logger import logger
from digitalkin.services.user_profile.user_profile_strategy import UserProfileStrategy


class DefaultUserProfile(UserProfileStrategy):
    """Default user profile strategy with in-memory storage."""

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
    ) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version
        """
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id)
        self.db: dict[str, dict[str, Any]] = {}

    async def get_user_profile(self) -> dict[str, Any] | None:
        """Get user profile from in-memory storage.

        Returns:
            User profile data, or None if not found.
        """
        if self.mission_id not in self.db:
            logger.warning("No user profile found for mission_id: %s", self.mission_id)
            return None

        logger.debug("Retrieved user profile for mission_id: %s", self.mission_id)
        return self.db[self.mission_id]

    def add_user_profile(self, user_profile_data: dict[str, Any]) -> None:
        """Add a user profile to the in-memory database (helper for testing).

        Args:
            user_profile_data: Dictionary containing user profile data
        """
        self.db[self.mission_id] = user_profile_data
        logger.debug("Added user profile for mission_id: %s", self.mission_id)
