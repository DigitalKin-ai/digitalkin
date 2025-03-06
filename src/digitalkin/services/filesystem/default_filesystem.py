"""Default filesystem."""

from typing import Any

from .filesystem_strategy import FilesystemStrategy


class DefaultFilesystem(FilesystemStrategy):
    """Default state filesystem strategy."""

    def create(self, data: dict[str, Any]) -> str:
        """Create a new file in the file system."""
        raise NotImplementedError

    def get(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Get files from the file system."""
        raise NotImplementedError

    def update(self, data: dict[str, Any]) -> int:
        """Update files in the file system."""
        raise NotImplementedError

    def delete(self, data: dict[str, Any]) -> int:
        """Delete files from the file system."""
        raise NotImplementedError

    def get_all(self) -> list[dict[str, Any]]:
        """Get all files from the file system."""
        raise NotImplementedError
