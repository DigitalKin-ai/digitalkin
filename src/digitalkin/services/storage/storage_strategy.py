"""This module contains the abstract base class for storage strategies."""

import datetime
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Literal, TypeGuard
from uuid import uuid4

from pydantic import BaseModel, Field

from digitalkin.services.base_strategy import BaseStrategy


class StorageServiceError(Exception):
    """Base exception for Setup service errors."""


class DataType(Enum):
    """Enum defining the types of data that can be stored."""

    OUTPUT = "OUTPUT"
    VIEW = "VIEW"
    LOGS = "LOGS"
    OTHER = "OTHER"


class StorageRecord(BaseModel):
    """A single record stored in a collection, with metadata."""

    mission_id: str = Field(..., description="ID of the mission (bucket) this doc belongs to")
    collection: str = Field(..., description="Logical collection name")
    record_id: str = Field(..., description="Unique ID of this record in its collection")
    data_type: DataType = Field(default=DataType.OUTPUT, description="Category of the data of this record")
    data: BaseModel = Field(..., description="The typed payload of this record")
    creation_date: datetime.datetime | None = Field(default=None, description="When this record was first created")
    update_date: datetime.datetime | None = Field(default=None, description="When this record was last modified")


class StorageStrategy(BaseStrategy, ABC):
    """Define CRUD + list/remove-collection against a collection/record store."""

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

    @staticmethod
    def _is_valid_data_type_name(value: str) -> TypeGuard[str]:
        return value in DataType.__members__

    @abstractmethod
    def _store(self, record: StorageRecord) -> StorageRecord:
        """Store a new record in the storage.

        Args:
            record: The record to store

        Returns:
            The ID of the created record
        """

    @abstractmethod
    def _read(self, collection: str, record_id: str) -> StorageRecord | None:
        """Get records from storage by key.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record

        Returns:
            A storage record with validated data
        """

    @abstractmethod
    def _update(self, collection: str, record_id: str, data: BaseModel) -> StorageRecord | None:
        """Overwrite an existing record's payload.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            data: The new data to store

        Returns:
            StorageRecord: The modified record
        """

    @abstractmethod
    def _remove(self, collection: str, record_id: str) -> bool:
        """Delete a record from the storage.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record

        Returns:
            True if the deletion was successful, False otherwise
        """

    @abstractmethod
    def _list(self, collection: str) -> list[StorageRecord]:
        """List all records in a collection.

        Args:
            collection: The unique name for the record type

        Returns:
            A list of storage records
        """

    @abstractmethod
    def _remove_collection(self, collection: str) -> bool:
        """Delete all records in a collection.

        Args:
            collection: The unique name for the record type

        Returns:
            True if the deletion was successful, False otherwise
        """

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

    def store(
        self,
        collection: str,
        record_id: str | None,
        data: dict[str, Any],
        data_type: Literal["OUTPUT", "VIEW", "LOGS", "OTHER"] = "OUTPUT",
    ) -> StorageRecord:
        """Store a new record in the storage.

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
        if not self._is_valid_data_type_name(data_type):
            msg = f"Invalid data type '{data_type}'. Must be one of {list(DataType.__members__.keys())}"
            raise ValueError(msg)
        record_id = record_id or uuid4().hex
        data_type_enum = DataType[data_type]
        validated_data = self._validate_data(collection, {**data, "mission_id": self.mission_id})
        record = self._create_storage_record(collection, record_id, validated_data, data_type_enum)
        return self._store(record)

    def read(self, collection: str, record_id: str) -> StorageRecord | None:
        """Get records from storage by key.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record

        Returns:
            A storage record with validated data
        """
        return self._read(collection, record_id)

    def update(self, collection: str, record_id: str, data: dict[str, Any]) -> StorageRecord | None:
        """Validate & overwrite an existing record.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            data: The new data to store

        Returns:
            StorageRecord: The modified record
        """
        validated_data = self._validate_data(collection, data)
        return self._update(collection, record_id, validated_data)

    def remove(self, collection: str, record_id: str) -> bool:
        """Delete a record from the storage.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record

        Returns:
            True if the deletion was successful, False otherwise
        """
        return self._remove(collection, record_id)

    def list(self, collection: str) -> list[StorageRecord]:
        """Get all records within a collection.

        Args:
            collection: The unique name for the record type

        Returns:
            A list of storage records
        """
        return self._list(collection)

    def remove_collection(self, collection: str) -> bool:
        """Wipe a record clean.

        Args:
            collection: The unique name for the record type

        Returns:
            True if the deletion was successful, False otherwise
        """
        return self._remove_collection(collection)
