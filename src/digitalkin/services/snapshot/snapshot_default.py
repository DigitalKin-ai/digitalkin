"""Default snapshot."""

from typing import Any

from digitalkin.services.snapshot.snapshot_strategy import SnapshotStrategy


class DefaultSnapshot(SnapshotStrategy):
    """Default snapshot strategy."""

    def create(self, data: dict[str, Any]) -> str:  # noqa: ARG002, PLR6301
        return "1"

    def list(self, data: dict[str, Any]) -> None:
        return

    def update(self, data: dict[str, Any]) -> int:  # noqa: ARG002, PLR6301
        return 1

    def delete(self, data: dict[str, Any]) -> int:  # noqa: ARG002, PLR6301
        return 1

    def get_all(self) -> None:
        return
