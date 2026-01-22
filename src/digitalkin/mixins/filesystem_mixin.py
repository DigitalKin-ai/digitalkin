"""Filesystem Mixin to ease filesystem use."""

from typing import Any

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.services.filesystem import FilesystemRecord


class FilesystemMixin:
    """Mixin providing filesystem operations through the filesystem strategy.

    This mixin wraps filesystem strategy calls to provide a cleaner API
    for file operations in trigger handlers.
    """

    @staticmethod
    async def create_files(context: ModuleContext, files: list[Any]) -> tuple[list[FilesystemRecord], int, int]:
        """Upload files using the filesystem strategy.

        Args:
            context: Module context containing the filesystem strategy
            files: List of files to upload

        Returns:
            Tuple of (all_files, succeeded_files, failed_files)

        Raises:
            FilesystemServiceError: If upload operation fails
        """
        return context.filesystem.create(files)

    @staticmethod
    async def list_file(context: ModuleContext, file_id: str) -> FilesystemRecord:
        """Retrieve a file by ID with the content.

        Args:
            context: Module context containing the filesystem strategy
            file_id: Unique identifier for the file

        Returns:
            File object with metadata and optionally content

        Raises:
            FilesystemServiceError: If file retrieval fails
        """
        return context.filesystem.list(file_id, include_content=True)
