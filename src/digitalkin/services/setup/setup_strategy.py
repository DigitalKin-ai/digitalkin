"""This module contains the abstract base class for setup strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.base_strategy import BaseStrategy
from digitalkin.models.services.setup import SetupData


class SetupStrategy(BaseStrategy, ABC):
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

    def __post_init__(self, *args: Any, **kwargs: Any) -> None:
        """Lifecycle hook for post-initialization. Subclasses override with specific params."""

    # ═══════════════════════════════ Overriding Merthods ════════════════════════════════ #

    @abstractmethod
    async def create(self, setup_dict: dict[str, Any]) -> str:
        """Create a new setup with comprehensive validation.

        Args:
            setup_dict: Dictionary containing setup details.

        Returns:
            bool: Success status of setup creation.

        Raises:
            ValidationError: If setup data is invalid.
            GrpcOperationError: If gRPC operation fails.
        """
        return await super().create()

    @abstractmethod
    async def get(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with 'name' and optional 'version'.

        Returns:
            Dict[str, Any]: Setup details including optional setup version.
        """
        return await super().get()

    @abstractmethod
    async def list(self, list_dict: dict[str, Any]) -> dict[str, Any]:
        """List setups with optional filtering and pagination.

        Args:
           list_dict: Dictionary with optional filters:
               - organization_id: Filter by organization
               - owner_id: Filter by owner
               - limit: Maximum number of results
               - offset: Number of results to skip

        Returns:
           dict[str, Any]: Dictionary with 'setups' list and 'total_count'.

        Raises:
           ServerError: If gRPC operation fails.
           SetupServiceError: For any unexpected internal error.
        """
        return await super().list()

    @abstractmethod
    async def update(self, setup_dict: dict[str, Any]) -> bool:
        """Update an existing setup.

        Args:
            setup_dict: Dictionary with setup update details.

        Returns:
            bool: Success status of the update operation.
        """
        return await super().update()

    @abstractmethod
    async def delete(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the setup 'name'.

        Returns:
            bool: Success status of deletion.
        """
        return await super().delete()

    # ════════════════════════════ Unimplemented Methods ═════════════════════════════ #

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        return await super().search()

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        return await super().upload()
