"""This module contains the abstract base class for snapshot strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.base_strategy import BaseStrategy


class SnapshotStrategy(BaseStrategy, ABC):
    """Abstract base class for snapshot strategies."""

    # ════════════════════════════════ Overriding Methods ════════════════════════════════ #

    @abstractmethod
    async def create(self, data: dict[str, Any]) -> str:
        """Create a new snapshot in the file system.

        Args:
            data: A dictionary containing the data needed to create the snapshot

        Returns:
            str: The ID of the new snapshot
        """
        return await super().create()

    @abstractmethod
    async def list(self, data: dict[str, Any]) -> None:
        """Get snapshots from the file system.

        Args:
            data: A dictionary containing the data needed to list the snapshots

        """
        return await super().list()

    @abstractmethod
    async def update(self, data: dict[str, Any]) -> int:
        """Update snapshots in the file system.

        Args:
            data: A dictionary containing the data needed to update the snapshots

        Returns:
            int: The number of snapshots updated

        """
        return await super().update()

    @abstractmethod
    async def delete(self, data: dict[str, Any]) -> int:
        """Delete snapshots from the file system.

        Args:
            data: A dictionary containing the data needed to delete the snapshots

        Returns:
            int: The number of snapshots deleted

        """
        return await super().delete()

    @abstractmethod
    async def get_all(self) -> None:
        """Get all snapshots from the file system."""
        msg = "Get all snapshots is not implemented yet."
        raise NotImplementedError(msg)

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().get(args, kwargs)

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().search(args, kwargs)

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().upload(args, kwargs)
