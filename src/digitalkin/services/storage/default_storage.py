"""This module implements the default storage strategy."""

import datetime
import logging

from pydantic import BaseModel

from .storage_strategy import StorageRecord, StorageStrategy

logger = logging.getLogger(__name__)


class DefaultStorage(StorageStrategy):
    """This class implements the default storage strategy."""

    storage: dict[str, StorageRecord]

    def __init__(
        self,
        mission_id: str,
        config: dict[str, type[BaseModel]],
        **kwargs,  # noqa: ANN003, ARG002
    ) -> None:
        """Initialize the storage."""
        super().__init__(mission_id=mission_id, config=config)
        self.storage = {}

    def _store(self, record: StorageRecord) -> str:
        """Store a new record in the database.

        Args:
            record: The record to store

        Returns:
            str: The ID of the new record

        Raises:
            ValueError: If the record already exists
        """
        name = record.name
        if name in self.storage:
            msg = f"Record with name {name} already exists"
            raise ValueError(msg)
        self.storage[name] = record
        self.storage[name].creation_date = datetime.datetime.now(datetime.timezone.utc)
        self.storage[name].update_date = datetime.datetime.now(datetime.timezone.utc)
        logger.info("CREATE %s:%s succesfull", name, record)
        return name

    def _read(self, name: str) -> StorageRecord | None:
        """Get records from the database.

        Args:
            name: The unique name to retrieve data for

        Returns:
            StorageRecord: The corresponding record
        """
        logger.info("GET record link to the key = %s", name)
        if name not in self.storage:
            logger.info("GET key = %s: DOESN'T EXIST", name)
            return None
        return self.storage[name]

    def _modify(self, name: str, data: BaseModel) -> StorageRecord | None:
        """Update records in the database.

        Args:
            name: The unique name to store the data under
            data: The data to modify

        Returns:
            StorageRecord: The modified
        """
        if name not in self.storage:
            logger.info("UPDATE key = %s: DOESN'T EXIST", name)
            return None
        self.storage[name].data = data
        self.storage[name].update_date = datetime.datetime.now(datetime.timezone.utc)
        return self.storage[name]

    def _remove(self, name: str) -> bool:
        """Delete records from the database.

        Args:
            name: The unique name to remove a record

        Returns:
            bool: True if the record was removed, False otherwise
        """
        if name not in self.storage:
            logger.info("UPDATE key = %s: DOESN'T EXIST", name)
            return False
        del self.storage[name]
        return True
