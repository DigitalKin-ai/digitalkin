"""This module contains the abstract base class for setup strategies."""

import secrets
import string
from typing import Any

from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.services.setup.grpc_setup import SetupData, SetupVersionData
from digitalkin.services.setup.setup_strategy import SetupServiceError, SetupStrategy


class DefaultSetup(SetupStrategy):
    """Abstract base class for setup strategies."""

    setups: dict[str, SetupData]
    setup_versions: dict[str, dict[str, SetupVersionData]]

    def __init__(self) -> None:
        """Initialize the default setup strategy."""
        super().__init__()
        self.setups = {}
        self.setup_versions = {}

    def create_setup(self, setup_dict: dict[str, Any]) -> str:
        """Create a new setup with comprehensive validation.

        Args:
            setup_dict: Dictionary containing setup details.

        Returns:
            bool: Success status of setup creation.

        Raises:
            ValidationError: If setup data is invalid.
            GrpcOperationError: If gRPC operation fails.
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
        logger.debug("CREATE SETUP DATA %s:%s succesfull", setup_id, valid_data)
        return setup_id

    def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with 'name' and optional 'version'.

        Raises:
            SetupServiceError: setup_id does not exist.

        Returns:
            Dict[str, Any]: Setup details including optional setup version.
        """
        logger.debug("GET setup_id = %s", setup_dict["setup_id"])
        if setup_dict["setup_id"] not in self.setups:
            msg = f"GET setup_id = {setup_dict['setup_id']}: setup_id DOESN'T EXIST"
            logger.error(msg)
            raise SetupServiceError(msg)
        return self.setups[setup_dict["setup_id"]]

    def update_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Update an existing setup.

        Args:
            setup_dict: Dictionary with setup update details.

        Raises:
            ValidationError: setup object failed validation.

        Returns:
            bool: Success status of the update operation.
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

    def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the setup 'name'.

        Returns:
            bool: Success status of deletion.
        """
        if setup_dict["setup_id"] not in self.setups:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_dict["setup_id"])
            return False
        del self.setups[setup_dict["setup_id"]]
        return True

    def create_setup_version(self, setup_version_dict: dict[str, Any]) -> str:
        """Create a new setup version.

        Args:
            setup_version_dict: Dictionary with setup version details.

        Raises:
            SetupServiceError: setup object failed validation.

        Returns:
            str: version of setup version creation.
        """
        try:
            valid_data = SetupVersionData.model_validate(setup_version_dict["data"])  # Revalidates instance
        except ValidationError:
            msg = "Validation failed for model SetupVersionData"
            logger.exception(msg)
            raise SetupServiceError(msg)

        if setup_version_dict["setup_id"] not in self.setup_versions:
            self.setup_versions[setup_version_dict["setup_id"]] = {}
        self.setup_versions[setup_version_dict["setup_id"]][valid_data.version] = valid_data
        logger.debug("CREATE SETUP VERSION DATA %s:%s succesfull", setup_version_dict["setup_id"], valid_data)
        return valid_data.version

    def get_setup_version(self, setup_version_dict: dict[str, Any]) -> SetupVersionData:
        """Retrieve a setup version by its unique identifier.

        Args:
            setup_version_dict: Dictionary with the setup version 'name'.

        Raises:
            SetupServiceError: setup_id does not exist.

        Returns:
            Dict[str, Any]: Setup version details.
        """
        logger.debug("GET setup_id = %s: version = %s", setup_version_dict["setup_id"], setup_version_dict["version"])
        if setup_version_dict["setup_id"] not in self.setup_versions:
            msg = f"GET setup_id = {setup_version_dict['setup_id']}: setup_id DOESN'T EXIST"
            logger.error(msg)
            raise SetupServiceError(msg)

        return self.setup_versions[setup_version_dict["setup_id"]][setup_version_dict["version"]]

    def search_setup_versions(self, setup_version_dict: dict[str, Any]) -> list[SetupVersionData]:
        """Search for setup versions based on filters.

        Args:
            setup_version_dict: Dictionary with optional 'name' or 'query_versions' filters.

        Raises:
            SetupServiceError: setup_id does not exist.

        Returns:
            List[SetupVersionData]: A list of matching setup version details.
        """
        if setup_version_dict["setup_id"] not in self.setup_versions:
            msg = f"GET setup_id = {setup_version_dict['setup_id']}: setup_id DOESN'T EXIST"
            logger.error(msg)
            raise SetupServiceError(msg)

        return [
            value
            for value in setup_version_dict["setup_id"].values()
            if setup_version_dict["query_versions"] in value.version or setup_version_dict["name"] in value.name
        ]

    def update_setup_version(self, setup_version_dict: dict[str, Any]) -> bool:
        """Update an existing setup version.

        Args:
            setup_version_dict: Dictionary with setup version update details.

        Returns:
            bool: Success status of the update operation.
        """
        if setup_version_dict["setup_id"] not in self.setup_versions:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_version_dict["setup_id"])
            return False

        if setup_version_dict["version"] not in self.setup_versions["setup_id"]:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_version_dict["setup_id"])
            return False

        try:
            valid_data = SetupVersionData.model_validate(setup_version_dict["data"])
        except ValidationError:
            logger.exception("Validation failed for model SetupVersionData")
            return False

        self.setup_versions[setup_version_dict["setup_id"]][setup_version_dict["version"]] = valid_data
        return True

    def delete_setup_version(self, setup_version_dict: dict[str, Any]) -> bool:
        """Delete a setup version by its unique identifier.

        Args:
            setup_version_dict: Dictionary with the setup version 'name'.

        Returns:
            bool: Success status of version deletion.
        """
        if setup_version_dict["setup_id"] not in self.setup_versions:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_version_dict["setup_id"])
            return False

        del self.setup_versions[setup_version_dict["setup_id"]][setup_version_dict["version"]]
        return True
