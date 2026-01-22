"""This module contains the abstract base class for filesystem strategies."""

from abc import ABC, abstractmethod
from typing import Any, Literal

from agentic_mesh_protocol.pagination.v1.pagination_pb2 import PaginationRequest

from digitalkin.models.base_strategy import BaseStrategy
from digitalkin.models.services.filesystem import FileFilter, FileStatus, FilesystemRecord, FileType, UploadFileData


class FilesystemStrategy(BaseStrategy, ABC):
    """Abstract base class for filesystem strategies.

    This strategy provides comprehensive file management capabilities including
    upload, retrieval, update, and deletion operations with rich metadata support,
    filtering, and pagination.
    """

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the gRPC filesystem strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version this strategy is associated with
            config: Configuration for the filesystem strategy
        """
        super().__init__(mission_id, setup_id, setup_version_id)
        self.config = config

    # ════════════════════════════════ Overriding Methods ════════════════════════════════ #

    @abstractmethod
    async def upload(
        self,
        files: list[UploadFileData],
    ) -> tuple[list[FilesystemRecord], int, int]:
        """Upload multiple files to the system.

        This method allows batch uploading of files with validation and
        error handling for each individual file. Files are processed
        atomically - if one fails, others may still succeed.

        Args:
            files: List of tuples containing (content, name, file_type, content_type, metadata, replace_if_exists)

        Returns:
            tuple[list[FilesystemRecord], int, int]: List of uploaded files, total uploaded count, total failed count
        """
        return await super().upload()

    @abstractmethod
    async def get(
        self,
        file_id: str,
        context: Literal["mission", "setup"] = "mission",
        *,
        include_content: bool = False,
    ) -> FilesystemRecord:
        """Get a specific file by ID or name.

        This method fetches detailed information about a single file,
        with optional content inclusion. Supports lookup by either
        unique ID or name within a context.

        Args:
            file_id: The ID of the file to be retrieved
            context: The context of the files (mission or setup)
            include_content: Whether to include file content in response

        Returns:
            FilesystemRecord: Metadata about the retrieved file
        """
        return await super().get()

    @abstractmethod
    async def list(
        self,
        filters: FileFilter,
        *,
            pagination=PaginationRequest(limit=100, offset=0, order=None),
        include_content: bool = False,
    ) -> tuple[list[FilesystemRecord], int]:
        """Get multiple files by various criteria.

        This method provides efficient retrieval of multiple files using:
        - File IDs
        - File names
        - Path prefix
        With support for:
        - Pagination for large result sets
        - Optional content inclusion
        - Total count of matching files

        Args:
            filters: Filter criteria for the files
            include_content: Whether to include file content in response
            pagination: Pagination settings for result set

        Returns:
            tuple[list[FilesystemRecord], int]: List of files and total count
        """
        return await super().list()

    @abstractmethod
    async def delete(
            self,
            filters: FileFilter,
            *,
            permanent: bool = False,
            force: bool = False,
    ) -> tuple[dict[str, bool], int, int]:
        """Delete multiple files.

        This method supports batch deletion of files with options for:
        - Soft deletion (marking as deleted)
        - Permanent deletion
        - Force deletion of files in use
        - Individual error reporting per file

        Args:
            filters: Filter criteria for the files
            permanent: Whether to permanently delete the files
            force: Whether to force delete even if files are in use

        Returns:
            tuple[dict[str, bool], int, int]: Results per file, total deleted count, total failed count
        """
        return await super().delete()

    @abstractmethod
    async def update(
        self,
        file_id: str,
        content: bytes | None = None,
            type: FileType | None = None,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        new_name: str | None = None,
            status: FileStatus | None = None,
    ) -> FilesystemRecord:
        """Update file metadata, content, or both.

        This method allows updating various aspects of a file:
        - Rename files
        - Update content and content type
        - Modify metadata
        - Create new versions

        Args:
            file_id: The ID of the file to be updated
            content: Optional new content of the file
            type: Optional new type of data
            content_type: Optional new MIME type
            metadata: Optional new metadata (will merge with existing)
            new_name: Optional new name for the file
            status: Optional new status for the file

        Returns:
            FilesystemRecord: Metadata about the updated file
        """
        return await super().update()

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        return await super().create()

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        return await super().search()
