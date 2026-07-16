"""This module contains the abstract base class for setup strategies."""

import datetime
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class SetupVersionData(BaseModel):
    """Pydantic model for SetupVersion data validation."""

    id: str
    setup_id: str
    version: str
    content: dict[str, Any]
    creation_date: datetime.datetime


class SetupData(BaseModel):
    """Pydantic model for Setup data validation.

    ``status``/``visibility`` carry the proto enum names (e.g. ``READY``,
    ``VISIBILITY_PRIVATE``); empty when the backend predates them.
    """

    id: str
    name: str
    organisation_id: str
    owner_id: str
    module_id: str
    current_setup_version: SetupVersionData
    status: str = ""
    visibility: str = ""


class SetupStrategy(ABC):
    """Abstract base class for setup strategies.

    Mirrors the SetupService protocol: setup-level CRUD plus visibility change.
    The version lifecycle is platform-owned — content flows through the setup's
    ``current_setup_version``, never through standalone version RPCs.
    """

    def __init__(self) -> None:
        """Initialize the setup strategy."""

    def __post_init__(self, *args: Any, **kwargs: Any) -> None:
        """Lifecycle hook for post-initialization. Subclasses override with specific params."""

    @abstractmethod
    async def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with 'setup_id' and optional 'version'.

        Returns:
            The setup with its current version populated.
        """

    @abstractmethod
    async def create_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Create a new setup; owner/organisation/module derive from the request context.

        Args:
            setup_dict: Dictionary with 'name' and 'content'.

        Returns:
            The created setup with its initial version.
        """

    @abstractmethod
    async def update_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Update a setup's name and current version content.

        Args:
            setup_dict: Dictionary with 'setup_id', 'name' and 'content'.

        Returns:
            The updated setup with its current version.
        """

    @abstractmethod
    async def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_id'.

        Returns:
            bool: Success status of deletion.
        """

    @abstractmethod
    async def change_visibility(self, setup_dict: dict[str, Any]) -> SetupData:
        """Change a setup's visibility scope.

        Args:
            setup_dict: Dictionary with 'setup_id' and 'visibility'
                (``public`` | ``private`` | ``internal``).

        Returns:
            The setup with its updated visibility.
        """
