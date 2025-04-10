"""Default snapshot."""

from typing import Any

from digitalkin.services.snapshot.snapshot_strategy import SnapshotStrategy


class DefaultSnapshot(SnapshotStrategy):
    """Default snapshot strategy."""

    def create(self, data: dict[str, Any]) -> str:  # noqa: ARG002, PLR6301
        """Create a new snapshot in the file system.

        Returns:
            str: The ID of the new snapshot
        """
        return "1"

    def get(self, data: dict[str, Any]) -> None:
        """Get snapshots from the file system."""

    def update(self, data: dict[str, Any]) -> int:  # noqa: ARG002, PLR6301
        """Update snapshots in the file system.

        Returns:
            int: The number of snapshots updated
        """
        return 1

    def delete(self, data: dict[str, Any]) -> int:  # noqa: ARG002, PLR6301
        """Delete snapshots from the file system.

        Returns:
            int: The number of snapshots deleted
        """
        return 1

    def get_all(self) -> None:
        """Get all snapshots from the file system."""
