"""This module contains the abstract base class for storage strategies."""

import asyncio
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

from digitalkin.models.base_strategy import BaseStrategy
from digitalkin.models.services.storage import DataType, StorageRecord


class StorageStrategy(BaseStrategy, ABC):
    """Define CRUD + list/remove-collection against a collection/record store."""

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, type[BaseModel]],
    ) -> None:
        """Initialize the storage strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version
            config: A dictionary mapping names to Pydantic model classes
        """
        super().__init__(mission_id, setup_id, setup_version_id)
        # Schema configuration mapping keys to model classes
        self.config: dict[str, type[BaseModel]] = config
        self._record_locks: dict[str, asyncio.Lock] = {}

    # ═════════════════════════════════ Private Methods ══════════════════════════════════ #

    def _create_storage_record(
        self,
        collection: str,
        record_id: str,
        validated_data: BaseModel,
        data_type: DataType,
    ) -> StorageRecord:
        """Create a storage record with metadata.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record
            validated_data: The validated data model
            data_type: The type of data

        Returns:
            A complete storage record with metadata
        """
        return StorageRecord(
            mission_id=self.mission_id,
            collection=collection,
            record_id=record_id,
            data=validated_data,
            data_type=data_type,
        )

    def _record_lock(self, collection: str, record_id: str) -> asyncio.Lock:
        """Get or create an asyncio.Lock for a specific record.

        Args:
            collection: The collection name
            record_id: The record ID

        Returns:
            An asyncio.Lock scoped to the given collection:record_id pair.
        """
        return self._record_locks.setdefault(f"{collection}:{record_id}", asyncio.Lock())

    # ════════════════════════════════ Protected Methods ═════════════════════════════════ #

    def _validate_data(self, collection: str, data: dict[str, Any]) -> BaseModel:
        """Validate data against the model schema for the given key.

        Args:
            collection: The unique name for the record type
            data: The data to validate

        Returns:
            A validated model instance

        Raises:
            ValueError: If the key has no associated model or validation fails
        """
        model_cls = self.config.get(collection)
        if not model_cls:
            msg = f"No schema registered for collection '{collection}'"
            raise ValueError(msg)

        try:
            return model_cls.model_validate(data)
        except Exception as e:
            msg = f"Validation failed for '{collection}': {e!s}"
            raise ValueError(msg) from e

    # ════════════════════════════════ Overriding Methods ════════════════════════════════ #

    @abstractmethod
    async def update(self, collection: str, record_id: str, data: BaseModel) -> StorageRecord | None:
        """Validate & overwrite an existing record.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            data: The new data to store

        Returns:
            StorageRecord: The modified record
        """
        return await super().update()

    @abstractmethod
    async def delete(self, collection: str, record_id: str) -> bool:
        """Delete a record from the storage.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record

        Returns:
            True if the deletion was successful, False otherwise
        """
        return await super().delete()

    @abstractmethod
    async def get(self, collection: str, record_id: str) -> StorageRecord | None:
        """Get records from storage by key.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record

        Returns:
            A storage record with validated data
        """
        return await super().get()

    @abstractmethod
    async def list(self, collection: str) -> list[StorageRecord]:
        """Get all records within a collection.

        Args:
            collection: The unique name for the record type

        Returns:
            A list of storage records
        """
        return await super().list()

    @abstractmethod
    async def create(
        self,
        collection: str,
        record_id: str | None,
        data: BaseModel,
        data_type: DataType = DataType.OUTPUT,
    ) -> StorageRecord:
        """Create a new record in the storage.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record (optional)
            data: The data to store
            data_type: The type of data being stored (default: OUTPUT)

        Returns:
            The ID of the created record

        Raises:
            ValueError: If the data type is invalid or if validation fails
        """
        return await super().create()

    # ═══════════════════════════════ Abstract Methods ═══════════════════════════════ #

    @abstractmethod
    async def delete_collection(self, collection: str) -> bool:
        """Delete all records in a collection.

        Args:
            collection: The unique name for the record type

        Returns:
            True if the deletion was successful, False otherwise
        """
        msg = "Delete collection method not implemented yet."
        raise NotImplementedError(msg)

    # ════════════════════════════ Unimplemented Methods ═════════════════════════════ #

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().search(args, kwargs)

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().upload(args, kwargs)
