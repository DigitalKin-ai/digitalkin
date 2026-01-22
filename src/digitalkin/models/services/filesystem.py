"""This module contains objects for filesystem strategies."""

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from agentic_mesh_protocol.filesystem.v1.filesystem_enums_pb2 import (
    FileStatus as FileStatusProto,
)
from agentic_mesh_protocol.filesystem.v1.filesystem_enums_pb2 import (
    FileType as FileTypeProto,
)
from pydantic import BaseModel, Field

from digitalkin.models.base_enum import BaseEnum


class FileType(BaseEnum[FileTypeProto], Enum):
    """Enumeration of file types."""

    UNSPECIFIED = "UNSPECIFIED"
    DOCUMENT = "DOCUMENT"
    IMAGE = "IMAGE"
    AUDIO = "AUDIO"
    VIDEO = "VIDEO"
    ARCHIVE = "ARCHIVE"
    CODE = "CODE"
    OTHER = "OTHER"


class FileStatus(BaseEnum[FileStatusProto], Enum):
    """Enumeration of file statuses."""

    UNSPECIFIED = "UNSPECIFIED"
    UPLOADING = "UPLOADING"
    ACTIVE = "ACTIVE"
    PROCESSING = "PROCESSING"
    ARCHIVED = "ARCHIVED"
    DELETED = "DELETED"


class FilesystemRecord(BaseModel):
    """Data model for filesystem operations."""

    id: str = Field(description="Unique identifier for the file (UUID)")
    context: str = Field(description="The context of the file in the filesystem")
    name: str = Field(description="The name of the file")
    type: FileType = Field(default=FileType.UNSPECIFIED, description="The type of data stored")
    content_type: str = Field(default="application/octet-stream", description="The MIME type of the file")
    size_bytes: int = Field(default=0, description="Size of the file in bytes")
    checksum: str = Field(default="", description="SHA-256 checksum of the file content")
    metadata: dict[str, Any] | None = Field(default=None, description="Additional metadata for the file")
    storage_uri: str = Field(description="Internal URI for accessing the file content")
    url: str = Field(description="Public URL for accessing the file content")
    status: FileStatus = Field(default=FileStatus.UNSPECIFIED, description="Current status of the file")
    content: bytes | None = Field(default=None, description="The content of the file")


class FileFilter(BaseModel):
    """Filter criteria for querying files."""

    context: Literal["mission", "setup"] = Field(
        default="mission", description="The context of the files (mission or setup)"
    )
    names: list[str] | None = Field(default=None, description="Filter by file names (exact matches)")
    ids: list[str] | None = Field(default=None, description="Filter by file IDs")
    types: list[FileType] | None = Field(default=None, description="Filter by file types")
    created_after: datetime | None = Field(default=None, description="Filter files created after this timestamp")
    created_before: datetime | None = Field(default=None, description="Filter files created before this timestamp")
    updated_after: datetime | None = Field(default=None, description="Filter files updated after this timestamp")
    updated_before: datetime | None = Field(default=None, description="Filter files updated before this timestamp")
    status: FileStatus | None = Field(default=None, description="Filter by file status")
    content_type_prefix: str | None = Field(default=None, description="Filter by content type prefix (e.g., 'image/')")
    min_size_bytes: int | None = Field(default=None, description="Filter files with minimum size")
    max_size_bytes: int | None = Field(default=None, description="Filter files with maximum size")
    prefix: str | None = Field(default=None, description="Filter by path prefix (e.g., 'folder1/')")
    content_type: str | None = Field(default=None, description="Filter by content type")


class UploadFileData(BaseModel):
    """Data model for uploading a file."""

    content: bytes = Field(description="The content of the file")
    name: str = Field(description="The name of the file")
    type: FileType = Field(description="The type of the file")
    content_type: str | None = Field(default=None, description="The content type of the file")
    metadata: dict[str, Any] | None = Field(default=None, description="The metadata of the file")
    replace_if_exists: bool = Field(default=False, description="Whether to replace the file if it already exists")
