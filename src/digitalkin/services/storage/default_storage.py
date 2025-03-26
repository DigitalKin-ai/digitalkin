"""This module implements the default storage strategy."""

import logging
from typing import Any

from pydantic import ValidationError

from .storage_strategy import StorageData, StorageStrategy

logger = logging.getLogger(__name__)


class DefaultStorage(StorageStrategy):
    """This class implements the default storage strategy."""

    storage: dict[str, list[StorageData]]

    def __init__(self) -> None:
        """Initialize the default storage strategy."""
        super().__init__()
        self.storage = {}

    def create(self, storage_dict: dict[str, Any]) -> str:
        """Create a new record in the database.

        Returns:
            str: The ID of the new record
        """
        try:
            valid_data = StorageData.model_validate(storage_dict["data"])  # Revalidates instance
        except ValidationError:
            logger.exception("Validation failed for model StorageData")
            return ""

        if storage_dict["table"] not in self.storage:
            self.storage[storage_dict["table"]] = []
        self.storage[storage_dict["table"]].append(valid_data)
        logger.info("CREATE %s:%s succesfull", storage_dict["table"], valid_data)
        return f"{len(self.storage[storage_dict['table']]) - 1}"

    def get(self, storage_dict: dict[str, Any]) -> list[StorageData]:
        """Get records from the database.

        Returns:
            list[StorageData]: The list of records
        """
        logger.info("GET table = %s: keys = %s", storage_dict["table"], storage_dict["keys"])
        if storage_dict["table"] not in self.storage:
            logger.info("GET table = %s: TABLE DOESN'T EXIST", storage_dict["table"])
            return []
        return [self.storage[storage_dict["table"]][int(key)] for key in storage_dict["keys"]]

    def update(self, storage_dict: dict[str, Any]) -> int:
        """Update records in the database.

        Returns:
            int: The number of records updated
        """
        if storage_dict["table"] not in self.storage:
            logger.info("UPDATE table = %s: TABLE DOESN'T EXIST", storage_dict["table"])
            return 0
        self.storage[storage_dict["table"]][storage_dict["update_id"]] = storage_dict["update_value"]
        return 1

    def delete(self, storage_dict: dict[str, Any]) -> int:
        """Delete records from the database.

        Returns:
            int: The number of records deleted
        """
        if storage_dict["table"] not in self.storage:
            logger.info("UPDATE table = %s: TABLE DOESN'T EXIST", storage_dict["table"])
            return 0
        del self.storage[storage_dict["table"]][storage_dict["delete_id"]]
        return 1
