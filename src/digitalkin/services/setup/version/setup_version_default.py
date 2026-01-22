"""This module contains the abstract base class for setup strategies."""

from typing import Any

from pydantic import ValidationError

from digitalkin.exception.setup import SetupVersionServiceError
from digitalkin.logger import logger
from digitalkin.models.services.setup import SetupData, SetupVersionData
from digitalkin.services.setup.version.setup_version_strategy import SetupVersionStrategy


class DefaultSetupVersion(SetupVersionStrategy):
    """Abstract base class for setup strategies."""

    setups: dict[str, SetupData]
    setup_versions: dict[str, dict[str, SetupVersionData]]

    def __init__(self, mission_id: str, setup_id: str, setup_version_id: str) -> None:
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

    async def create(self, setup_version_dict: dict[str, Any]) -> str:
        """Create a setup version in local storage.

        Returns:
            The setup version ID.

        Raises:
            SetupVersionServiceError: If version already exists.
        """
        try:
            valid_data = SetupVersionData.model_validate(setup_version_dict["data"])  # Revalidates instance
        except ValidationError:
            msg = "Validation failed for model SetupVersionData"
            logger.exception(msg)
            raise SetupVersionServiceError(msg)

        if setup_version_dict["setup_id"] not in self.setup_versions:
            self.setup_versions[setup_version_dict["setup_id"]] = {}
        self.setup_versions[setup_version_dict["setup_id"]][valid_data.version] = valid_data
        logger.debug("CREATE SETUP VERSION DATA %s:%s successful", setup_version_dict["setup_id"], valid_data)
        return valid_data.version

    async def get(self, setup_version_dict: dict[str, Any]) -> SetupVersionData:
        """Retrieve a setup version from local storage.

        Returns:
            The setup version data.

        Raises:
            SetupVersionServiceError: If version not found.
        """
        logger.debug("GET setup_id = %s: version = %s", setup_version_dict["setup_id"], setup_version_dict["version"])
        if setup_version_dict["setup_id"] not in self.setup_versions:
            msg = f"GET setup_id = {setup_version_dict['setup_id']}: setup_id DOESN'T EXIST"
            logger.error(msg)
            raise SetupVersionServiceError(msg)

        return self.setup_versions[setup_version_dict["setup_id"]][setup_version_dict["version"]]

    async def search(self, setup_version_dict: dict[str, Any]) -> list[SetupVersionData]:
        """Search setup versions by query string.

        Returns:
            The updated setup version.

        Raises:
            SetupVersionServiceError: If version not found.
        """
        if setup_version_dict["setup_id"] not in self.setup_versions:
            msg = f"GET setup_id = {setup_version_dict['setup_id']}: setup_id DOESN'T EXIST"
            logger.error(msg)
            raise SetupVersionServiceError(msg)

        return [
            value
            for value in self.setup_versions[setup_version_dict["setup_id"]].values()
            if setup_version_dict["query_versions"] in value.version
        ]

    async def update(self, setup_version_dict: dict[str, Any]) -> bool:
        """Update a setup version in local storage.

        Returns:
            True if version was deleted.
        """
        if setup_version_dict["setup_id"] not in self.setup_versions:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_version_dict["setup_id"])
            return False

        if setup_version_dict["version"] not in self.setup_versions[setup_version_dict["setup_id"]]:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_version_dict["setup_id"])
            return False

        try:
            valid_data = SetupVersionData.model_validate(setup_version_dict["data"])
        except ValidationError:
            logger.exception("Validation failed for model SetupVersionData")
            return False

        self.setup_versions[setup_version_dict["setup_id"]][setup_version_dict["version"]] = valid_data
        return True

    async def delete(self, setup_version_dict: dict[str, Any]) -> bool:
        """Delete a setup version from local storage.

        Returns:
            The setup version data dict.
        """
        if setup_version_dict["setup_id"] not in self.setup_versions:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_version_dict["setup_id"])
            return False

        del self.setup_versions[setup_version_dict["setup_id"]][setup_version_dict["version"]]
        return True
