"""Default snapshot."""

from typing import Any

from digitalkin.services.snapshot.snapshot_strategy import SnapshotStrategy


class DefaultSnapshot(SnapshotStrategy):
    """Default snapshot strategy."""

    def create(self, _data: dict[str, Any]) -> str:
        """Create a snapshot (stub).

        Returns:
            The snapshot ID.
        """
        return "1"

    def list(self, _data: dict[str, Any]) -> None:
        """List snapshots (stub)."""
        return

    def update(self, _data: dict[str, Any]) -> int:
        """Update a snapshot (stub).

        Returns:
            Update count.
        """
        return 1

    def delete(self, _data: dict[str, Any]) -> int:
        """Delete a snapshot (stub).

        Returns:
            Deletion count.
        """
        return 1

    def get_all(self) -> None:
        """Get all snapshots (stub)."""
        return
