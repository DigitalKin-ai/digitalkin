"""This module contains the abstract base class for setup strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.base_strategy import BaseStrategy
from digitalkin.models.services.setup import SetupVersionData


class SetupVersionStrategy(BaseStrategy, ABC):
    """Abstract base class for setup strategies."""

    def __init__(
            self,
            mission_id: str,
            setup_id: str,
            setup_version_id: str,
            config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the strategy."""
        super().__init__(mission_id, setup_id, setup_version_id)
        self.config = config

    # ═════════════════════════════════ Private Methods ══════════════════════════════════ #

    def __post_init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize the setup strategy."""

    # ═════════════════════════════════ Overrinding Methods ═════════════════════════════════ #

    @abstractmethod
    async def create(self, setup_version_dict: dict[str, Any]) -> str:
        """Create a new setup version.

        Args:
            setup_version_dict: Dictionary with setup version details.

        Returns:
            str: name of setup version creation.
        """
        return await super().create()

    @abstractmethod
    async def get(self, setup_version_dict: dict[str, Any]) -> SetupVersionData:
        """Retrieve a setup version by its unique identifier.

        Args:
            setup_version_dict: Dictionary with the setup version 'name'.

        Returns:
            Dict[str, Any]: Setup version details.
        """
        return await super().get()

    @abstractmethod
    async def search(self, setup_version_dict: dict[str, Any]) -> list[SetupVersionData]:
        """Search for setup versions based on filters.

        Args:
            setup_version_dict: Dictionary with optional 'name' and 'version' filters.

        Returns:
            List[Dict[str, Any]]: A list of matching setup version details.
        """
        return await super().search()

    @abstractmethod
    async def update(self, setup_version_dict: dict[str, Any]) -> bool:
        """Update an existing setup version.

        Args:
            setup_version_dict: Dictionary with setup version update details.

        Returns:
            bool: Success status of the update operation.
        """
        return await super().update()

    @abstractmethod
    async def delete(self, setup_version_dict: dict[str, Any]) -> bool:
        """Delete a setup version by its unique identifier.

        Args:
            setup_version_dict: Dictionary with the setup version 'name'.

        Returns:
            bool: Success status of version deletion.
        """
        return await super().delete()

    # ════════════════════════════ Unimplemented Methods ═════════════════════════════ #

    async def list(self, *args: Any, **kwargs: Any) -> Any:
        return await super().list()

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        return await super().upload()
