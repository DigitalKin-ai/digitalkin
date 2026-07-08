"""Default secret implementation."""

from typing import Any

from digitalkin.logger import logger
from digitalkin.services.secret.secret_strategy import SecretStrategy


class DefaultSecret(SecretStrategy):
    """Default secret strategy with in-memory storage."""

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

    async def get_secret(self) -> dict[str, Any] | None:
        """Get the secret for this setup from in-memory storage.

        Returns:
            Secret values, or None if not found.
        """
        if self.setup_id not in self.db:
            logger.warning("No secret found for setup_id: %s", self.setup_id)
            return None
        return self.db[self.setup_id]

    def add_secret(self, secret_data: dict[str, Any]) -> None:
        """Add a secret to the in-memory database (helper for testing).

        Args:
            secret_data: Dictionary containing secret values.
        """
        self.db[self.setup_id] = secret_data
