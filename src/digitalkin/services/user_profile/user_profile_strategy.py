"""This module contains the abstract base class for UserProfile strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.base_strategy import BaseStrategy


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
    def get(self) -> dict[str, Any]:
        """Get user profile data.

        Returns:
            dict[str, Any]: User profile data

        Raises:
            UserProfileServiceError: If the user profile cannot be retrieved
        """
        return super().get()

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    def create(self, *args: Any, **kwargs: Any) -> Any:
        return super().create()

    def list(self, *args: Any, **kwargs: Any) -> Any:
        return super().list()

    def search(self, *args: Any, **kwargs: Any) -> Any:
        return super().search()

    def delete(self, *args: Any, **kwargs: Any) -> Any:
        return super().delete()

    def update(self, *args: Any, **kwargs: Any) -> Any:
        return super().update()

    def upload(self, *args: Any, **kwargs: Any) -> Any:
        return super().upload()
