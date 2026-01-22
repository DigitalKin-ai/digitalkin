"""This module contains the abstract base class for setup strategies."""

import secrets
import string
from typing import Any

from pydantic import ValidationError

from digitalkin.exception.setup import SetupServiceError
from digitalkin.logger import logger
from digitalkin.models.services.setup import SetupData, SetupVersionData
from digitalkin.services.setup.setup_strategy import SetupStrategy


class DefaultSetup(SetupStrategy):
    """Abstract base class for setup strategies."""

    setups: dict[str, SetupData]
    setup_versions: dict[str, dict[str, SetupVersionData]]

    def __init__(
        self, mission_id: str | None = None, setup_id: str | None = None, setup_version_id: str | None = None
    ) -> None:
        """Initialize the default setup strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version this strategy is associated with
        """
        super().__init__(mission_id, setup_id, setup_version_id)
        self.setups = {}
        self.setup_versions = {}

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    async def create(self, setup_dict: dict[str, Any]) -> str:
        """Create a setup in local storage.

        Returns:
            The setup ID.
        """
        try:
            valid_data = SetupData.model_validate(setup_dict["data"])  # Revalidates instance
        except ValidationError:
            logger.exception("Validation failed for model SetupData")
            return ""

        setup_id = setup_dict.get(
            "setup_id", "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))
        )
        valid_data.id = setup_id
        self.setups[setup_id] = valid_data
        logger.debug("CREATE SETUP DATA %s:%s successful", setup_id, valid_data)
        return setup_id

    async def get(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by ID from local storage.

        Returns:
            The setup data.

        Raises:
            SetupServiceError: If setup not found.
        """
        logger.debug("GET setup_id = %s", setup_dict["setup_id"])
        if setup_dict["setup_id"] not in self.setups:
            msg = f"GET setup_id = {setup_dict['setup_id']}: setup_id DOESN'T EXIST"
            logger.error(msg)
            raise SetupServiceError(msg)
        return self.setups[setup_dict["setup_id"]]

    async def update(self, setup_dict: dict[str, Any]) -> bool:
        """Update a setup in local storage.

        Returns:
            The updated setup data.
        """
        if setup_dict["setup_id"] not in self.setups:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_dict["setup_id"])
            return False

        try:
            valid_data = SetupData.model_validate(setup_dict["data"])  # Revalidates instance
        except ValidationError:
            logger.exception("Validation failed for model SetupData")
            return False

        self.setups[setup_dict["update_id"]] = valid_data
        return True

    async def delete(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup from local storage.

        Returns:
            True if setup was deleted.
        """
        if setup_dict["setup_id"] not in self.setups:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_dict["setup_id"])
            return False
        del self.setups[setup_dict["setup_id"]]
        return True

    async def list(self, list_dict: dict[str, Any]) -> dict[str, Any]:
        """List setups with optional pagination.

        Returns:
            List of setup data.
        """
        setups = list(self.setups.values())
        offset = list_dict.get("offset", 0)
        limit = list_dict.get("limit", 0)
        setups = setups[offset : offset + limit] if limit > 0 else setups[offset:]
        return {"setups": [s.model_dump() for s in setups], "total_count": len(self.setups)}
