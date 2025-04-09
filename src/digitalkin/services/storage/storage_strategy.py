"""This module contains the abstract base class for storage strategies."""

import datetime
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Literal, TypeGuard

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
    """Container for stored records with metadata."""

    # Metadata
    mission_id: str = Field(description="The ID of the mission this record is associated with")
    name: str = Field(description="The name of the record")
    creation_date: datetime.datetime | None = Field(default=None, description="The date the record was created")
    update_date: datetime.datetime | None = Field(default=None, description="The date the record was last updated")
    data_type: DataType = Field(default=DataType.OUTPUT, description="The type of data stored")
    # Actual data payload
    data: BaseModel = Field(description="The data stored in the record")


class StorageStrategy(BaseStrategy, ABC):
    """Abstract base class for storage strategies.

    This strategy defines how data is stored and retrieved, with
    type validation through registered Pydantic models.
    """

    def __init__(self, mission_id: str, config: dict[str, type[BaseModel]]) -> None:
        """Initialize the storage strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            config: A dictionary mapping names to Pydantic model classes
        """
        super().__init__(mission_id)
        # Schema configuration mapping keys to model classes
        self.config: dict[str, type[BaseModel]] = config

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

    def store(
        self,
        name: str,
        data: dict[str, Any],
        data_type: Literal["OUTPUT", "VIEW", "LOGS", "OTHER"] = "OUTPUT",
    ) -> StorageRecord:
        """Store a new record in the storage.

        Args:
            name: The unique name to store the data under
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
        data_type_enum = DataType[data_type]
        validated_data = self._validate_data(name, {**data, "mission_id": self.mission_id})
        record = self._create_storage_record(name, validated_data, data_type_enum)
        return self._store(record)

    @abstractmethod
    def _read(self, name: str) -> StorageRecord | None:
        """Get records from storage by key.

        Args:
            name: The unique name to retrieve data for

        Returns:
            A storage record with validated data
        """

    def read(self, name: str) -> StorageRecord | None:
        """Get records from storage by key.

        Args:
            name: The unique name to retrieve data for

        Returns:
            A storage record with validated data
        """
        return self._read(name)

    @abstractmethod
    def _modify(self, name: str, data: BaseModel) -> StorageRecord | None:
        """Update a record in the storage.

        Args:
            name: The unique name for the record type
            data: The new data to store

        Returns:
            StorageRecord: The modified record
        """

    def modify(self, name: str, data: dict[str, Any]) -> StorageRecord | None:
        """Update a record in the storage (overwrite all the data).

        Args:
            name: The unique name for the record type
            data: The new data to store

        Returns:
            StorageRecord: The modified record
        """
        validated_data = self._validate_data(name, data)
        return self._modify(name, validated_data)

    @abstractmethod
    def _remove(self, name: str) -> bool:
        """Delete a record from the storage.

        Args:
            name: The unique name for the record type

        Returns:
            True if the deletion was successful, False otherwise
        """

    def remove(self, name: str) -> bool:
        """Delete a record from the storage.

        Args:
            name: The unique name for the record type

        Returns:
            True if the deletion was successful, False otherwise
        """
        return self._remove(name)

    def _validate_data(self, name: str, data: dict[str, Any]) -> BaseModel:
        """Validate data against the model schema for the given key.

        Args:
            name: The unique name to get the model type for
            data: The data to validate

        Returns:
            A validated model instance

        Raises:
            ValueError: If the key has no associated model or validation fails
        """
        model_cls = self.config.get(name)
        if not model_cls:
            msg = f"No model schema defined for name: {name}"
            raise ValueError(msg)

        try:
            return model_cls.model_validate(data)
        except Exception as e:
            msg = f"Data validation failed for key '{name}': {e!s}"
            raise ValueError(msg) from e

    def _create_storage_record(
        self,
        name: str,
        validated_data: BaseModel,
        data_type: DataType,
    ) -> StorageRecord:
        """Create a storage record with metadata.

        Args:
            name: The unique name for the record
            validated_data: The validated data model
            data_type: The type of data

        Returns:
            A complete storage record with metadata
        """
        return StorageRecord(
            mission_id=self.mission_id,
            name=name,
            data=validated_data,
            data_type=data_type,
        )
