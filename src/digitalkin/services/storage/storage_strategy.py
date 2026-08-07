"""This module contains the abstract base class for storage strategies."""

import asyncio
import datetime
from abc import ABC, abstractmethod
from typing import Any, Literal, TypeGuard
from uuid import uuid4

from pydantic import BaseModel, Field

from digitalkin.models.services.storage import ContextStorage, DataType, Visibility
from digitalkin.services.base_strategy import BaseStrategy
from digitalkin.services.storage.exceptions import StorageServiceError


class StorageRecord(BaseModel):
    """A single record stored in a collection, with metadata."""

    context: str = Field(..., description="Owner context (`missions:<id>` or `setup_versions:<id>`)")
    collection: str = Field(..., description="Logical collection name")
    record_id: str = Field(..., description="Unique ID of this record in its collection")
    data_type: DataType = Field(default=DataType.OUTPUT, description="Category of the data of this record")
    visibility: Visibility = Field(
        default=Visibility.UNSPECIFIED,
        description="Read-access scope of this record (UNSPECIFIED = storage-service default)",
    )
    data: BaseModel = Field(..., description="The typed payload of this record")
    creation_date: datetime.datetime | None = Field(default=None, description="When this record was first created")
    update_date: datetime.datetime | None = Field(default=None, description="When this record was last modified")


class StorageStrategy(BaseStrategy, ABC):
    """Define CRUD + list/remove-collection against a collection/record store.

    Records are scoped by a `context` string (the proto field), which is either
    `self.mission_id` (mission scope, the default) or `self.setup_version_id`
    (setup-version scope). Both attributes are expected to already contain the
    full prefix (`missions:<id>` / `setup_versions:<id>`).

    Public methods accept `scope: Literal["mission", "setup", "user", "organization"]`
    (default `"mission"`); internally we resolve it to the matching context string and
    pass that to the abstract `_store/_read/_update/_remove/_list/_remove_collection`.
    `user`/`organization` are read-only cross-owner scopes usable only for listing.
    """

    def _resolve_context(self, context: ContextStorage) -> str:
        """Resolve a context kind to its storage context string.

        MISSIONS/SETUP_VERSIONS map to the owner contexts this strategy was built
        with. USERS/ORGANIZATIONS are read-only cross-owner scopes (list only); the
        strategy holds no user/org id, so it returns a kind-only marker
        (`users:` / `organizations:`). The storage service resolves the concrete id
        server-side from the request metadata stamped by the client interceptor.

        Args:
            context: The context kind to resolve.

        Returns:
            The context string: `missions:<id>`, `setup_versions:<id>`, or the
            kind marker `users:` / `organizations:`.
        """
        match context:
            case ContextStorage.MISSIONS:
                return self.mission_id
            case ContextStorage.SETUP_VERSIONS:
                return self.setup_version_id
            case ContextStorage.USERS | ContextStorage.ORGANIZATIONS:
                return f"{context.value}:"
            case _:
                return self.mission_id

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
        visibility: Visibility,
    ) -> StorageRecord:
        """Create a storage record stamped with the given context.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record
            validated_data: The validated data model
            data_type: The type of data
            context: Owner context to stamp on the record (mission or setup-version).
            visibility: Read-access scope for the record.

        Returns:
            A complete storage record with metadata
        """
        return StorageRecord(
            context=context,
            collection=collection,
            record_id=record_id,
            data=validated_data,
            data_type=data_type,
            visibility=visibility,
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
            context: Resolved owner context (e.g. `missions:<mission_id>` or `setup_versions:<setup_version_id>`).

        Returns:
            A storage record with validated data
        """

    @abstractmethod
    async def _update(
        self,
        collection: str,
        record_id: str,
        data: BaseModel,
        context: str,
        visibility: Visibility = Visibility.UNSPECIFIED,
    ) -> StorageRecord | None:
        """Overwrite an existing record's payload scoped to a specific context.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            data: The new data to store
            context: Owner context for the record being updated.
            visibility: New read-access scope; UNSPECIFIED leaves it unchanged.

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
    async def _list(
        self, collection: str, context: str, visibilities: list[Visibility] | None = None
    ) -> list[StorageRecord]:
        """List all records in a collection scoped to a specific context.

        Args:
            collection: The unique name for the record type
            context: Owner context filter.
            visibilities: Optional read-access scopes to filter by (None = no filter).

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
            context: Resolved owner context string the record lives under.
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
        data_type: DataType = DataType.OUTPUT,
        context: ContextStorage = ContextStorage.MISSIONS,
        visibility: Visibility = Visibility.UNSPECIFIED,
    ) -> StorageRecord:
        """Store a new record in the storage.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record (optional)
            data: The data to store
            data_type: The type of data being stored (default: OUTPUT)
            context: "mission" (default) writes under the current mission context;
                "setup" writes under the setup-version context.
            visibility: Read-access scope for the record (UNSPECIFIED = server default).

        Returns:
            The ID of the created record

        Raises:
            ValueError: If the data type is invalid or if validation fails
        """
        if not self._is_valid_data_type_name(data_type.value):
            msg = f"Invalid data type '{data_type}'. Must be one of {list(DataType.__members__.keys())}"
            raise ValueError(msg)
        record_id = record_id or uuid4().hex
        validated_data = self._validate_data(collection, data)
        record = self._create_storage_record(
            collection, record_id, validated_data, data_type, self._resolve_context(context), visibility
        )
        async with self._record_lock(record.context, collection, record_id):
            return await self._store(record)

    async def read(
        self, collection: str, record_id: str, context: ContextStorage = ContextStorage.MISSIONS
    ) -> StorageRecord | None:
        """Get a record by key under the given scope.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record
            context: Which context to read from (default: "mission").

        Returns:
            The matching record if it exists, otherwise None.
        """
        ctx = self._resolve_context(context)
        async with self._record_lock(ctx, collection, record_id):
            return await self._read(collection, record_id, ctx)

    async def update(
        self,
        collection: str,
        record_id: str,
        data: dict[str, Any],
        context: ContextStorage = ContextStorage.MISSIONS,
        visibility: Visibility = Visibility.UNSPECIFIED,
    ) -> StorageRecord | None:
        """Validate & overwrite an existing record under the given scope.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            data: The new data to store
            context: Which context the record lives under (default: "mission").
            visibility: New read-access scope; UNSPECIFIED leaves it unchanged.

        Returns:
            StorageRecord: The modified record
        """
        validated_data = self._validate_data(collection, data)
        ctx = self._resolve_context(context)
        async with self._record_lock(ctx, collection, record_id):
            return await self._update(collection, record_id, validated_data, ctx, visibility)

    async def remove(self, collection: str, record_id: str, context: ContextStorage = ContextStorage.MISSIONS) -> bool:
        """Delete a record from the storage under the given scope.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID of the record
            context: Which context the record lives under (default: "mission").

        Returns:
            True if the deletion was successful, False otherwise
        """
        ctx = self._resolve_context(context)
        async with self._record_lock(ctx, collection, record_id):
            result = await self._remove(collection, record_id, ctx)
        if result:
            self._record_locks.pop(f"{ctx}|{collection}:{record_id}", None)
        return result

    async def list(
        self,
        collection: str,
        context: ContextStorage = ContextStorage.MISSIONS,
        visibilities: list[Visibility] | None = None,
    ) -> list[StorageRecord]:
        """Get all records in a collection under the given scope.

        Args:
            collection: The unique name for the record type
            context: Which context to list (default: "mission"). "user"/"organization"
                list across an owner and require `owner_id`.
            visibilities: Optional read-access scopes to filter by (None = no filter).

        Returns:
            A list of storage records under the resolved context.
        """
        return await self._list(collection, self._resolve_context(context), visibilities)

    async def remove_collection(self, collection: str, context: ContextStorage = ContextStorage.MISSIONS) -> bool:
        """Wipe a collection clean under the given scope.

        Args:
            collection: The unique name for the record type
            context: Which context the records live under (default: "mission").

        Returns:
            True if the deletion was successful, False otherwise
        """
        ctx = self._resolve_context(context)
        result = await self._remove_collection(collection, ctx)
        if result:
            prefix = f"{ctx}|{collection}:"
            for key in [k for k in self._record_locks if k.startswith(prefix)]:
                self._record_locks.pop(key, None)
        return result

    async def upsert(
        self,
        collection: str,
        record_id: str,
        data: dict[str, Any],
        data_type: DataType = DataType.OUTPUT,
        context: ContextStorage = ContextStorage.MISSIONS,
        visibility: Visibility = Visibility.UNSPECIFIED,
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
            context: Which context to upsert under (default: "mission").
            visibility: Read-access scope for the record (UNSPECIFIED = server default).

        Returns:
            The created or updated storage record

        Raises:
            ValueError: If the data type is invalid or if validation fails
            StorageServiceError: If update of an existing record fails unexpectedly
        """
        if not self._is_valid_data_type_name(data_type.value):
            msg = f"Invalid data type '{data_type}'. Must be one of {list(DataType.__members__.keys())}"
            raise ValueError(msg)
        validated_data = self._validate_data(collection, data)
        ctx = self._resolve_context(context)
        async with self._record_lock(ctx, collection, record_id):
            if await self._read(collection, record_id, ctx):
                updated = await self._update(collection, record_id, validated_data, ctx, visibility)
                if updated is None:
                    msg = f"Update failed for existing record '{collection}:{record_id}'"
                    raise StorageServiceError(msg)
                return updated
            record = self._create_storage_record(collection, record_id, validated_data, data_type, ctx, visibility)
            return await self._store(record)
