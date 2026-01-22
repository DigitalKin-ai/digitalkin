"""This module contains the abstract base class for UserProfile strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.base_strategy import BaseStrategy
from digitalkin.models.grpc_servers.models import ClientConfig


class UserProfileServiceError(Exception):
    """Base exception for UserProfile service errors."""


class UserProfileStrategy(BaseStrategy, ABC):
    """Abstract base class for UserProfile strategies."""

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        client_config: ClientConfig,
    ) -> None:
        """Initialize the user profile strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version
            client_config: Client configuration for connecting to the user profile service
        """
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id)
        self.client_config = client_config

    # ════════════════════════════════ Overriting Methods ════════════════════════════════ #

    @abstractmethod
    async def get(self) -> dict[str, Any]:
        """Get user profile data.

        Returns:
            User profile data, or None if not found.

        Raises:
            UserProfileServiceError: If the service call fails (not for missing profiles).
        """
        return await super().get()

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().create(args, kwargs)

    async def list(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().list(args, kwargs)

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().search(args, kwargs)

    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().delete(args, kwargs)

    async def update(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().update(args, kwargs)

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().upload(args, kwargs)
