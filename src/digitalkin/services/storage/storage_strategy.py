"""This module contains the abstract base class for storage strategies."""

import asyncio
import datetime
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Literal, TypeGuard
from uuid import uuid4

from pydantic import BaseModel, Field

from digitalkin.logger import logger
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

    context: str = Field(..., description="Owner context (`missions:<id>` or `setup_versions:<id>`)")
    collection: str = Field(..., description="Logical collection name")
    record_id: str = Field(..., description="Unique ID of this record in its collection")
    data_type: DataType = Field(default=DataType.OUTPUT, description="Category of the data of this record")
    data: BaseModel = Field(..., description="The typed payload of this record")
    creation_date: datetime.datetime | None = Field(default=None, description="When this record was first created")
    update_date: datetime.datetime | None = Field(default=None, description="When this record was last modified")


Scope = Literal["mission", "setup"]


class StorageStrategy(BaseStrategy, ABC):
    """Define CRUD + list/remove-collection against a collection/record store.

    Records are scoped by a `context` string (the proto field), which is either
    `self.mission_id` (mission scope, the default) or `self.setup_version_id`
    (setup-version scope). Both attributes are expected to already contain the
    full prefix (`missions:<id>` / `setup_versions:<id>`).

    Public methods accept `scope: Literal["mission", "setup"]` (default
    `"mission"`); internally we resolve it to the matching context string and
    pass that to the abstract `_store/_read/_update/_remove/_list/_remove_collection`.
    """

    def _resolve_context(self, scope: Scope) -> str:
        """Return the context string for the given scope."""
        return self.mission_id if scope == "mission" else self.setup_version_id

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

    @staticmethod
    def _create_storage_record(
        collection: str,
        record_id: str,
        validated_data: BaseModel,
        data_type: DataType,
        context: str,
    ) -> StorageRecord:
        """Create a storage record stamped with the given context.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record
            validated_data: The validated data model
            data_type: The type of data
            context: Owner context to stamp on the record (mission or setup-version).

        Returns:
            A complete storage record with metadata
        """
        return StorageRecord(
            context=context,
            collection=collection,
            record_id=record_id,
            data=validated_data,
            data_type=data_type,
        )

    @staticmethod
    def _is_valid_data_type_name(value: str) -> TypeGuard[str]:
        return value in DataType.__members__

    @abstractmethod
    async def _store(self, record: StorageRecord) -> StorageRecord:
        """Store a new record in the storage.

        Args:
            record: The record to store (context is encoded in record.context)

        Returns:
            The ID of the created record
        """

    @abstractmethod
    async def _read(self, collection: str, record_id: str, context: str) -> StorageRecord | None:
        """Get records from storage scoped to a specific context.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record
            context: Owner context (e.g. `missions:<mission_id>` or `setup_versions:<setup_version_id>`).

        Returns:
            A storage record with validated data
        """

    @abstractmethod
    async def _update(self, collection: str, record_id: str, data: BaseModel, context: str) -> StorageRecord | None:
        """Overwrite an existing record's payload scoped to a specific context.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            data: The new data to store
            context: Owner context for the record being updated.

        Returns:
            StorageRecord: The modified record
        """

    @abstractmethod
    async def _remove(self, collection: str, record_id: str, context: str) -> bool:
        """Delete a record from the storage scoped to a specific context.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            context: Owner context for the record being deleted.

        Returns:
            True if the deletion was successful, False otherwise
        """

    @abstractmethod
    async def _list(self, collection: str, context: str) -> list[StorageRecord]:
        """List all records in a collection scoped to a specific context.

        Args:
            collection: The unique name for the record type
            context: Owner context filter.

        Returns:
            A list of storage records
        """

    @abstractmethod
    async def _remove_collection(self, collection: str, context: str) -> bool:
        """Delete all records in a collection scoped to a specific context.

        Args:
            collection: The unique name for the record type
            context: Owner context for which to wipe records.

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
            mission_id: Already-prefixed mission context (`missions:<id>`).
            setup_id: The ID of the setup
            setup_version_id: Already-prefixed setup-version context (`setup_versions:<id>`).
            config: A dictionary mapping names to Pydantic model classes
        """
        super().__init__(mission_id, setup_id, setup_version_id)
        # Schema configuration mapping keys to model classes
        self.config: dict[str, type[BaseModel]] = config
        self._record_locks: dict[str, asyncio.Lock] = {}

    def _record_lock(self, context: str, collection: str, record_id: str) -> asyncio.Lock:
        """Get or create an asyncio.Lock for a specific record under a given context.

        Args:
            context: Owner context the record lives under
            collection: The collection name
            record_id: The record ID

        Returns:
            An asyncio.Lock scoped to the given context:collection:record_id triple.
        """
        return self._record_locks.setdefault(f"{context}|{collection}:{record_id}", asyncio.Lock())

    async def store(
        self,
        collection: str,
        record_id: str | None,
        data: dict[str, Any],
        data_type: Literal["OUTPUT", "VIEW", "LOGS", "OTHER"] = "OUTPUT",
        scope: Scope = "mission",
    ) -> StorageRecord:
        """Store a new record in the storage.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record (optional)
            data: The data to store
            data_type: The type of data being stored (default: OUTPUT)
            scope: "mission" (default) writes under the current mission context;
                "setup" writes under the setup-version context.

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
        context = self._resolve_context(scope)
        validated_data = self._validate_data(collection, data)
        record = self._create_storage_record(collection, record_id, validated_data, data_type_enum, context)
        async with self._record_lock(context, collection, record_id):
            return await self._store(record)

    async def read(self, collection: str, record_id: str, scope: Scope = "mission") -> StorageRecord | None:
        """Get a record by key under the given scope.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record
            scope: Which context to read from (default: "mission").

        Returns:
            The matching record if it exists, otherwise None.
        """
        context = self._resolve_context(scope)
        async with self._record_lock(context, collection, record_id):
            return await self._read(collection, record_id, context)

    async def update(
        self,
        collection: str,
        record_id: str,
        data: dict[str, Any],
        scope: Scope = "mission",
    ) -> StorageRecord | None:
        """Validate & overwrite an existing record under the given scope.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            data: The new data to store
            scope: Which context the record lives under (default: "mission").

        Returns:
            StorageRecord: The modified record
        """
        validated_data = self._validate_data(collection, data)
        context = self._resolve_context(scope)
        async with self._record_lock(context, collection, record_id):
            return await self._update(collection, record_id, validated_data, context)

    async def remove(self, collection: str, record_id: str, scope: Scope = "mission") -> bool:
        """Delete a record from the storage under the given scope.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            scope: Which context the record lives under (default: "mission").

        Returns:
            True if the deletion was successful, False otherwise
        """
        context = self._resolve_context(scope)
        async with self._record_lock(context, collection, record_id):
            result = await self._remove(collection, record_id, context)
        if result:
            self._record_locks.pop(f"{context}|{collection}:{record_id}", None)
        return result

    async def list(self, collection: str, scope: Scope = "mission") -> list[StorageRecord]:
        """Get all records in a collection under the given scope.

        Args:
            collection: The unique name for the record type
            scope: Which context to list (default: "mission").

        Returns:
            A list of storage records under the resolved context.
        """
        return await self._list(collection, self._resolve_context(scope))

    async def remove_collection(self, collection: str, scope: Scope = "mission") -> bool:
        """Wipe a collection clean under the given scope.

        Args:
            collection: The unique name for the record type
            scope: Which context the records live under (default: "mission").

        Returns:
            True if the deletion was successful, False otherwise
        """
        context = self._resolve_context(scope)
        result = await self._remove_collection(collection, context)
        if result:
            prefix = f"{context}|{collection}:"
            for key in [k for k in self._record_locks if k.startswith(prefix)]:
                self._record_locks.pop(key, None)
        return result

    async def upsert(
        self,
        collection: str,
        record_id: str,
        data: dict[str, Any],
        data_type: Literal["OUTPUT", "VIEW", "LOGS", "OTHER"] = "OUTPUT",
        scope: Scope = "mission",
    ) -> StorageRecord:
        """Insert or update a record atomically under the given scope.

        If a record with the given collection/record_id exists under that
        context it is updated; otherwise a new record is created. The operation
        is protected by a per-record lock to prevent races.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record
            data: The data to store
            data_type: The type of data being stored (default: OUTPUT)
            scope: Which context to upsert under (default: "mission").

        Returns:
            The created or updated storage record

        Raises:
            ValueError: If the data type is invalid or if validation fails
            StorageServiceError: If update of an existing record fails unexpectedly
        """
        if not self._is_valid_data_type_name(data_type):
            msg = f"Invalid data type '{data_type}'. Must be one of {list(DataType.__members__.keys())}"
            raise ValueError(msg)
        data_type_enum = DataType[data_type]
        context = self._resolve_context(scope)
        validated_data = self._validate_data(collection, data)
        async with self._record_lock(context, collection, record_id):
            if await self._read(collection, record_id, context):
                updated = await self._update(collection, record_id, validated_data, context)
                if updated is None:
                    msg = f"Update failed for existing record '{collection}:{record_id}'"
                    raise StorageServiceError(msg)
                return updated
            record = self._create_storage_record(collection, record_id, validated_data, data_type_enum, context)
            return await self._store(record)
