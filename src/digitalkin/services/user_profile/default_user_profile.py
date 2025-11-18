"""Default user profile implementation."""

from typing import Any

from digitalkin.logger import logger
from digitalkin.services.user_profile.user_profile_strategy import (
    UserProfileServiceError,
    UserProfileStrategy,
)


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

    def get_user_profile(self) -> dict[str, Any]:
        """Get user profile from in-memory storage.

        Returns:
            dict[str, Any]: User profile data

        Raises:
            UserProfileServiceError: If the user profile is not found
        """
        if self.mission_id not in self.db:
            msg = f"User profile for mission {self.mission_id} not found in the database."
            logger.warning(msg)
            raise UserProfileServiceError(msg)

        logger.debug(f"Retrieved user profile for mission_id: {self.mission_id}")
        return self.db[self.mission_id]

    def add_user_profile(self, user_profile_data: dict[str, Any]) -> None:
        """Add a user profile to the in-memory database (helper for testing).

        Args:
            user_profile_data: Dictionary containing user profile data
        """
        self.db[self.mission_id] = user_profile_data
        logger.debug(f"Added user profile for mission_id: {self.mission_id}")
