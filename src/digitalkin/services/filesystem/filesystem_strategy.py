"""This module contains the abstract base class for filesystem strategies."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from digitalkin.models.services.filesystem import FileMetadata, FileType, FileUploadMetadata
from digitalkin.models.services.services import Context
from digitalkin.models.services.storage import Visibility
from digitalkin.services.base_strategy import BaseStrategy
from digitalkin.services.filesystem.exceptions import FilesystemServiceError


class FilesystemRecord(FileMetadata):
    """A stored file: the canonical `FileMetadata` plus the service-only fields."""

    context: str = Field(description="The context of the file in the filesystem")
    checksum: str = Field(default="", description="SHA-256 checksum of the file content")
    metadata: dict[str, Any] | None = Field(default=None, description="Additional metadata for the file")
    storage_uri: str = Field(default="", description="Internal URI for accessing the file content")
    status: str = Field(default="UNSPECIFIED", description="Current status of the file")
    content: bytes | None = Field(default=None, description="The content of the file")
    visibility: Visibility = Field(default=Visibility.UNSPECIFIED, description="Read-access scope of the file")


class FileFilter(BaseModel):
    """Filter criteria for querying files."""

    context: Context = Field(
        default=Context.MISSIONS,
        description="The context of the files: mission/setup (owner) or user/organization (read-only cross-owner)",
    )
    names: list[str] | None = Field(default=None, description="Filter by file names (exact matches)")
    file_ids: list[str] | None = Field(default=None, description="Filter by file IDs")
    file_types: list[FileType] | None = Field(default=None, description="Filter by file types")
    created_after: datetime | None = Field(default=None, description="Filter files created after this timestamp")
    created_before: datetime | None = Field(default=None, description="Filter files created before this timestamp")
    updated_after: datetime | None = Field(default=None, description="Filter files updated after this timestamp")
    updated_before: datetime | None = Field(default=None, description="Filter files updated before this timestamp")
    status: str | None = Field(default=None, description="Filter by file status")
    content_type_prefix: str | None = Field(default=None, description="Filter by content type prefix (e.g., 'image/')")
    min_size_bytes: int | None = Field(default=None, description="Filter files with minimum size")
    max_size_bytes: int | None = Field(default=None, description="Filter files with maximum size")
    prefix: str | None = Field(default=None, description="Filter by path prefix (e.g., 'folder1/')")
    content_type: str | None = Field(default=None, description="Filter by content type")
    visibilities: list[Visibility] | None = Field(
        default=None, description="Filter by read-access scope; None or empty means no filter"
    )


class UploadFileData(BaseModel):
    """Data model for uploading a file."""

    content: bytes = Field(description="The content of the file")
    name: str = Field(description="The name of the file")
    type: FileType = Field(description="The type of the file")
    content_type: str | None = Field(default=None, description="The content type of the file")
    metadata: dict[str, Any] | BaseModel | None = Field(default=None, description="The metadata of the file")
    replace_if_exists: bool = Field(default=False, description="Whether to replace the file if it already exists")
    visibility: Visibility = Field(
        default=Visibility.UNSPECIFIED, description="Read-access scope; UNSPECIFIED lets the service default it"
    )

    @field_validator("metadata", mode="before")
    @classmethod
    def _accept_model(cls, value: object) -> object:
        """Accept either a pydantic model or a plain mapping.

        Returns:
            The value as a mapping.
        """
        return value.model_dump() if isinstance(value, BaseModel) else value


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
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version this strategy is associated with
            config: Configuration for the filesystem strategy
        """
        super().__init__(mission_id, setup_id, setup_version_id)
        self.config = config

    def _validate_metadata(self, file: "UploadFileData") -> dict[str, Any]:
        """Validate an upload's metadata against the registered schema.

        The model is registered per module as
        ``services_config_params["filesystem"]["config"]["metadata_model"]``,
        mirroring how storage collections register theirs; ``FileUploadMetadata``
        applies when none is.

        Args:
            file: The upload whose metadata to validate.

        Returns:
            The validated metadata as a plain dict, ready for the wire. Only keys the
            caller actually set are kept, so validating adds nothing to the payload.

        Raises:
            FilesystemServiceError: If the metadata does not match the schema.
        """
        model_cls = (self.config or {}).get("metadata_model") or FileUploadMetadata
        try:
            return model_cls.model_validate(file.metadata or {}).model_dump(mode="json", exclude_unset=True)
        except Exception as e:
            msg = f"Invalid metadata for file '{file.name}': {e!s}"
            raise FilesystemServiceError(msg) from e

    @abstractmethod
    async def upload_files(
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

    @abstractmethod
    async def get_file(
        self,
        file_id: str,
        context: Context = Context.MISSIONS,
        *,
        include_content: bool = False,
    ) -> FilesystemRecord:
        """Get a specific file by ID or name.

        This method fetches detailed information about a single file,
        with optional content inclusion. Supports lookup by either
        unique ID or name within a context.

        Args:
            file_id: The ID of the file to be retrieved
            context: The context of the file (mission/setup, or user/organization for cross-owner reads)
            include_content: Whether to include file content in response

        Returns:
            tuple[FilesystemRecord, bytes | None]: Metadata about the retrieved file and optional content
        """

    @abstractmethod
    async def get_files(
        self,
        filters: FileFilter,
        *,
        list_size: int = 100,
        offset: int = 0,
        order: str | None = None,
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
            list_size: Number of files to return per page
            offset: Offset to start listing files from
            order: Field to order results by
            include_content: Whether to include file content in response

        Returns:
            tuple[list[FilesystemRecord], int]: List of files and total count
        """

    @abstractmethod
    async def update_file(
        self,
        file_id: str,
        content: bytes | None = None,
        type: FileType | None = None,  # Matches the canonical field name # noqa: A002
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        new_name: str | None = None,
        status: str | None = None,
        visibility: Visibility = Visibility.UNSPECIFIED,
        context: Context = Context.MISSIONS,
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
            visibility: Optional new read-access scope; UNSPECIFIED leaves it unchanged
            context: The owner context of the file

        Returns:
            FilesystemRecord: Metadata about the updated file
        """

    @abstractmethod
    async def delete_files(
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
