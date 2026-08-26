"""Storage Mixin to ease storage access in Triggers."""

from typing import Any, Literal

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.services.storage import DataType
from digitalkin.services.storage.storage_strategy import StorageRecord


class StorageMixin:
    """Mixin providing storage operations through the storage strategy.

    This mixin wraps storage strategy calls to provide a cleaner API
    for trigger handlers.
    """

    @staticmethod
    async def store_storage(
        context: ModuleContext,
        collection: str,
        record_id: str | None,
        data: dict[str, Any],
        data_type: Literal["OUTPUT", "VIEW", "LOGS", "OTHER"] = "OUTPUT",
    ) -> StorageRecord:
        """Store data using the storage strategy.

        Args:
            context: Module context containing the storage strategy
            collection: Collection name for the data
            record_id: Optional record identifier
            data: Data to store
            data_type: Type of data being stored

        Returns:
            Result from the storage strategy

        Raises:
            StorageServiceError: If storage operation fails
        """
        return await context.storage.store(collection, record_id, data, data_type=DataType[data_type])

    @staticmethod
    async def read_storage(context: ModuleContext, collection: str, record_id: str) -> StorageRecord | None:
        """Read data from storage.

        Args:
            context: Module context containing the storage strategy
            collection: Collection name
            record_id: Record identifier

        Returns:
            Retrieved data

        Raises:
            StorageServiceError: If read operation fails
        """
        return await context.storage.read(collection, record_id)

    @staticmethod
    async def update_storage(
        context: ModuleContext,
        collection: str,
        record_id: str,
        data: dict[str, Any],
    ) -> StorageRecord | None:
        """Update existing data in storage.

        Args:
            context: Module context containing the storage strategy
            collection: Collection name
            record_id: Record identifier
            data: Updated data

        Returns:
            Result from the storage strategy

        Raises:
            StorageServiceError: If update operation fails
        """
        return await context.storage.update(collection, record_id, data)

    @staticmethod
    async def upsert_storage(
        context: ModuleContext,
        collection: str,
        record_id: str,
        data: dict[str, Any],
        data_type: Literal["OUTPUT", "VIEW", "LOGS", "OTHER"] = "OUTPUT",
    ) -> StorageRecord:
        """Insert or update data in storage atomically.

        Args:
            context: Module context containing the storage strategy
            collection: Collection name
            record_id: Record identifier
            data: Data to store or update
            data_type: Type of data being stored

        Returns:
            The created or updated storage record

        Raises:
            StorageServiceError: If upsert operation fails
        """
        return await context.storage.upsert(collection, record_id, data, data_type=DataType[data_type])
