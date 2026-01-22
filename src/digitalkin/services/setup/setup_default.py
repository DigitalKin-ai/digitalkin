"""This module contains the abstract base class for setup strategies."""

import secrets
import string
from typing import Any

from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.services.setup.setup_models import SetupData, SetupVersionData
from digitalkin.services.setup.setup_strategy import SetupServiceError, SetupStrategy


class DefaultSetup(SetupStrategy):
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

    def create(self, setup_dict: dict[str, Any]) -> str:
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

    def get(self, setup_dict: dict[str, Any]) -> SetupData:
        logger.debug("GET setup_id = %s", setup_dict["setup_id"])
        if setup_dict["setup_id"] not in self.setups:
            msg = f"GET setup_id = {setup_dict['setup_id']}: setup_id DOESN'T EXIST"
            logger.error(msg)
            raise SetupServiceError(msg)
        return self.setups[setup_dict["setup_id"]]

    def update(self, setup_dict: dict[str, Any]) -> bool:
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

    def delete(self, setup_dict: dict[str, Any]) -> bool:
        if setup_dict["setup_id"] not in self.setups:
            logger.debug("UPDATE setup_id = %s: setup_id DOESN'T EXIST", setup_dict["setup_id"])
            return False
        del self.setups[setup_dict["setup_id"]]
        return True

    def list(self, list_dict: dict[str, Any]) -> dict[str, Any]:
        return super().list(list_dict)
