"""This module implements the default storage strategy."""

import logging
from typing import Any

from .storage_strategy import StorageStrategy

logger = logging.getLogger(__name__)


class DefaultStorage(StorageStrategy):
    """This class implements the default storage strategy."""

    storage: dict[str, list[dict[str, Any]]]

    def __init__(self) -> None:
        """Initialize the default storage strategy."""
        super().__init__()
        self.storage = {"setups": []}

    def connect(self) -> bool:  # noqa: PLR6301
        """Establish connection to the database.

        Returns:
            bool: True if the connection is successful, False otherwise
        """
        return True

    def disconnect(self) -> bool:  # noqa: PLR6301
        """Close connection to the database.

        Returns:
            bool: True if the connection is closed, False otherwise
        """
        return True

    def create(self, data: dict[str, Any]) -> str:
        """Create a new record in the database.

        Returns:
            str: The ID of the new record
        """
        self.storage[data["table"]].append(data["insert"])
        logger.info("CREATE %s:%s succesfull", data["table"], data["insert"])
        return f"{len(self.storage[data['table']])}"

    def get(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Get records from the database.

        Returns:
            list[dict[str, Any]]: The list of records
        """
        logger.info("GET table = %s: keys = %s", data["table"], data["keys"])
        return [self.storage[data["table"]][int(key)] for key in data["keys"]]

    def update(self, data: dict[str, Any]) -> int:
        """Update records in the database.

        Returns:
            int: The number of records updated
        """
        self.storage[data["table"]][data["update_id"]] = data["update_value"]
        return 1

    def delete(self, data: dict[str, Any]) -> int:
        """Delete records from the database.

        Returns:
            int: The number of records deleted
        """
        del self.storage[data["table"]][data["delete_id"]]
        return 1

    def get_all(self) -> list[dict[str, Any]]:
        """Get all records from the database.

        Returns:
            list[dict[str, Any]]: The list of records
        """
        return self.storage["setups"]
