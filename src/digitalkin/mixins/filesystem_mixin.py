"""Filesystem Mixin to ease filesystem use."""

from typing import Any

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.services.filesystem.filesystem_strategy import FilesystemRecord


class FilesystemMixin:
    """Mixin providing filesystem operations through the filesystem strategy.

    This mixin wraps filesystem strategy calls to provide a cleaner API
    for file operations in trigger handlers.
    """

    @staticmethod
    def upload_files(context: ModuleContext, files: list[Any]) -> tuple[list[FilesystemRecord], int, int]:
        """Upload files using the filesystem strategy.

        Args:
            context: Module context containing the filesystem strategy
            files: List of files to upload

        Returns:
            Tuple of (all_files, succeeded_files, failed_files)

        Raises:
            FilesystemServiceError: If upload operation fails
        """
        return context.filesystem.upload_files(files)

    @staticmethod
    def get_file(context: ModuleContext, file_id: str) -> tuple[FilesystemRecord, bytes | None]:
        """Retrieve a file by ID with the content.

        Args:
            context: Module context containing the filesystem strategy
            file_id: Unique identifier for the file
            include_content: Whether to include file content in response

        Returns:
            File object with metadata and optionally content

        Raises:
            FilesystemServiceError: If file retrieval fails
        """
        return context.filesystem.get_file(file_id, include_content=True)
