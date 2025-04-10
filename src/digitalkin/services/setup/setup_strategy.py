"""This module contains the abstract base class for setup strategies."""

import datetime
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class SetupServiceError(Exception):
    """Base exception for Setup service errors."""


class SetupVersionData(BaseModel):
    """Pydantic model for SetupVersion data validation."""

    id: str
    setup_id: str
    version: str
    content: dict[str, Any]
    creation_date: datetime.datetime


class SetupData(BaseModel):
    """Pydantic model for Setup data validation."""

    id: str
    name: str
    organisation_id: str
    owner_id: str
    module_id: str
    current_setup_version: SetupVersionData


class SetupStrategy(ABC):
    """Abstract base class for setup strategies."""

    def __init__(self) -> None:
        """Initialize the setup strategy."""

    def __post_init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize the setup strategy."""

    @abstractmethod
    def create_setup(self, setup_dict: dict[str, Any]) -> str:
        """Create a new setup with comprehensive validation.

        Args:
            setup_dict: Dictionary containing setup details.

        Returns:
            bool: Success status of setup creation.

        Raises:
            ValidationError: If setup data is invalid.
            GrpcOperationError: If gRPC operation fails.
        """

    @abstractmethod
    def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with 'name' and optional 'version'.

        Returns:
            Dict[str, Any]: Setup details including optional setup version.
        """

    @abstractmethod
    def update_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Update an existing setup.

        Args:
            setup_dict: Dictionary with setup update details.

        Returns:
            bool: Success status of the update operation.
        """

    @abstractmethod
    def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the setup 'name'.

        Returns:
            bool: Success status of deletion.
        """

    @abstractmethod
    def create_setup_version(self, setup_version_dict: dict[str, Any]) -> str:
        """Create a new setup version.

        Args:
            setup_version_dict: Dictionary with setup version details.

        Returns:
            str: name of setup version creation.
        """

    @abstractmethod
    def get_setup_version(self, setup_version_dict: dict[str, Any]) -> SetupVersionData:
        """Retrieve a setup version by its unique identifier.

        Args:
            setup_version_dict: Dictionary with the setup version 'name'.

        Returns:
            Dict[str, Any]: Setup version details.
        """

    @abstractmethod
    def search_setup_versions(self, setup_version_dict: dict[str, Any]) -> list[SetupVersionData]:
        """Search for setup versions based on filters.

        Args:
            setup_version_dict: Dictionary with optional 'name' and 'version' filters.

        Returns:
            List[Dict[str, Any]]: A list of matching setup version details.
        """

    @abstractmethod
    def update_setup_version(self, setup_version_dict: dict[str, Any]) -> bool:
        """Update an existing setup version.

        Args:
            setup_version_dict: Dictionary with setup version update details.

        Returns:
            bool: Success status of the update operation.
        """

    @abstractmethod
    def delete_setup_version(self, setup_version_dict: dict[str, Any]) -> bool:
        """Delete a setup version by its unique identifier.

        Args:
            setup_version_dict: Dictionary with the setup version 'name'.

        Returns:
            bool: Success status of version deletion.
        """
