"""Bound strategy wrappers that auto-inject RequestContext.

These wrappers bind a shared singleton service to a specific request context,
preserving the public API so downstream code (mixins, triggers) doesn't need
to pass ctx explicitly.
"""

from typing import Any, Literal

from digitalkin.services.base_strategy import RequestContext
from digitalkin.services.filesystem.filesystem_strategy import (
    FileFilter,
    FilesystemRecord,
    FilesystemStrategy,
    UploadFileData,
)
from digitalkin.services.storage.storage_strategy import StorageRecord, StorageStrategy
from digitalkin.services.user_profile.user_profile_strategy import UserProfileStrategy


class BoundStorageStrategy:
    """Binds a shared StorageStrategy to a specific RequestContext."""

    __slots__ = ("_ctx", "_service")

    def __init__(self, service: StorageStrategy, ctx: RequestContext) -> None:
        """Initialize the bound storage strategy.

        Args:
            service: The shared storage strategy singleton.
            ctx: The request context to inject.
        """
        self._service = service
        self._ctx = ctx

    async def store(
        self,
        collection: str,
        record_id: str | None,
        data: dict[str, Any],
        data_type: Literal["OUTPUT", "VIEW", "LOGS", "OTHER"] = "OUTPUT",
    ) -> StorageRecord:
        """Store a new record.

        Args:
            collection: The collection name.
            record_id: The record ID (optional).
            data: The data to store.
            data_type: The type of data being stored.

        Returns:
            The created storage record.
        """
        return await self._service.store(self._ctx, collection, record_id, data, data_type=data_type)

    async def read(self, collection: str, record_id: str) -> StorageRecord | None:
        """Read a record.

        Args:
            collection: The collection name.
            record_id: The record ID.

        Returns:
            The storage record, or None if not found.
        """
        return await self._service.read(self._ctx, collection, record_id)

    async def update(self, collection: str, record_id: str, data: dict[str, Any]) -> StorageRecord | None:
        """Update a record.

        Args:
            collection: The collection name.
            record_id: The record ID.
            data: The updated data.

        Returns:
            The updated storage record, or None if not found.
        """
        return await self._service.update(self._ctx, collection, record_id, data)

    async def remove(self, collection: str, record_id: str) -> bool:
        """Remove a record.

        Args:
            collection: The collection name.
            record_id: The record ID.

        Returns:
            True if the record was removed.
        """
        return await self._service.remove(self._ctx, collection, record_id)

    async def list(self, collection: str) -> list[StorageRecord]:
        """List all records in a collection.

        Args:
            collection: The collection name.

        Returns:
            A list of storage records.
        """
        return await self._service.list(self._ctx, collection)

    async def remove_collection(self, collection: str) -> bool:
        """Remove an entire collection.

        Args:
            collection: The collection name.

        Returns:
            True if the collection was removed.
        """
        return await self._service.remove_collection(self._ctx, collection)

    async def upsert(
        self,
        collection: str,
        record_id: str,
        data: dict[str, Any],
        data_type: Literal["OUTPUT", "VIEW", "LOGS", "OTHER"] = "OUTPUT",
    ) -> StorageRecord:
        """Insert or update a record atomically.

        Args:
            collection: The collection name.
            record_id: The record ID.
            data: The data to store.
            data_type: The type of data being stored.

        Returns:
            The created or updated storage record.
        """
        return await self._service.upsert(self._ctx, collection, record_id, data, data_type=data_type)


class BoundFilesystemStrategy:
    """Binds a shared FilesystemStrategy to a specific RequestContext."""

    __slots__ = ("_ctx", "_service")

    def __init__(self, service: FilesystemStrategy, ctx: RequestContext) -> None:
        """Initialize the bound filesystem strategy.

        Args:
            service: The shared filesystem strategy singleton.
            ctx: The request context to inject.
        """
        self._service = service
        self._ctx = ctx

    async def upload_files(
        self,
        files: list[UploadFileData],
    ) -> tuple[list[FilesystemRecord], int, int]:
        """Upload multiple files.

        Args:
            files: List of files to upload.

        Returns:
            Tuple of (uploaded files, total uploaded, total failed).
        """
        return await self._service.upload_files(self._ctx, files)

    async def get_file(
        self,
        file_id: str,
        context: Literal["mission", "setup"] = "mission",
        *,
        include_content: bool = False,
    ) -> FilesystemRecord:
        """Get a file by ID.

        Args:
            file_id: The file ID.
            context: The context (mission or setup).
            include_content: Whether to include file content.

        Returns:
            The file record.
        """
        return await self._service.get_file(self._ctx, file_id, context, include_content=include_content)

    async def get_files(
        self,
        filters: FileFilter,
        *,
        list_size: int = 100,
        offset: int = 0,
        order: str | None = None,
        include_content: bool = False,
    ) -> tuple[list[FilesystemRecord], int]:
        """Get multiple files.

        Args:
            filters: Filter criteria.
            list_size: Number of files per page.
            offset: Offset to start from.
            order: Field to order by.
            include_content: Whether to include file content.

        Returns:
            Tuple of (files, total count).
        """
        return await self._service.get_files(
            self._ctx, filters, list_size=list_size, offset=offset, order=order, include_content=include_content
        )

    async def update_file(
        self,
        file_id: str,
        content: bytes | None = None,
        file_type: Literal[
            "UNSPECIFIED",
            "DOCUMENT",
            "IMAGE",
            "VIDEO",
            "AUDIO",
            "ARCHIVE",
            "CODE",
            "OTHER",
        ]
        | None = None,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        new_name: str | None = None,
        status: str | None = None,
    ) -> FilesystemRecord:
        """Update a file.

        Args:
            file_id: The file ID.
            content: Optional new content.
            file_type: Optional new file type.
            content_type: Optional new content type.
            metadata: Optional new metadata.
            new_name: Optional new name.
            status: Optional new status.

        Returns:
            The updated file record.
        """
        return await self._service.update_file(
            self._ctx, file_id, content, file_type, content_type, metadata, new_name, status
        )

    async def delete_files(
        self,
        filters: FileFilter,
        *,
        permanent: bool = False,
        force: bool = False,
    ) -> tuple[dict[str, bool], int, int]:
        """Delete files.

        Args:
            filters: Filter criteria.
            permanent: Whether to permanently delete.
            force: Whether to force delete.

        Returns:
            Tuple of (results per file, total deleted, total failed).
        """
        return await self._service.delete_files(self._ctx, filters, permanent=permanent, force=force)


class BoundUserProfileStrategy:
    """Binds a shared UserProfileStrategy to a specific RequestContext."""

    __slots__ = ("_ctx", "_service")

    def __init__(self, service: UserProfileStrategy, ctx: RequestContext) -> None:
        """Initialize the bound user profile strategy.

        Args:
            service: The shared user profile strategy singleton.
            ctx: The request context to inject.
        """
        self._service = service
        self._ctx = ctx

    async def get_user_profile(self) -> dict[str, Any]:
        """Get user profile data.

        Returns:
            User profile data dictionary.
        """
        return await self._service.get_user_profile(self._ctx)
