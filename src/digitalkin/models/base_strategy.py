"""This module contains the abstract base class for storage strategies."""

from abc import ABC, abstractmethod
from typing import Any


class BaseStrategy(ABC):
    """Abstract base class for all strategies.

    This class defines the interface for all strategies.
    """

    def __init__(self, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup this strategy is associated with
            setup_version_id: The ID of the setup version this strategy is associated with
        """
        self.mission_id: str = mission_id
        self.setup_id: str = setup_id
        self.setup_version_id: str = setup_version_id

    @abstractmethod
    async def create(self, *args: Any, **kwargs: Any) -> Any:
        """Add a new resource.

        This method must be implemented by subclasses with their specific signature.

        Raises:
            NotImplementedError: This function is not implemented yet.
        """
        msg = "Create method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def get(self, *args: Any, **kwargs: Any) -> Any:
        """Get one resources.

        This method must be implemented by subclasses with their specific signature.

        Raises:
            NotImplementedError: This function is not implemented yet.
        """
        msg = "Get method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def list(self, *args: Any, **kwargs: Any) -> Any:
        """List one or more resources.

        This method must be implemented by subclasses with their specific signature.

        Raises:
            NotImplementedError: This function is not implemented yet.
        """
        msg = "List method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def search(self, *args: Any, **kwargs: Any) -> Any:
        """Search resources.

        This method must be implemented by subclasses with their specific signature.

        Raises:
            NotImplementedError: This function is not implemented yet.
        """
        msg = "Search method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        """Delete one or more resources.

        This method must be implemented by subclasses with their specific signature.

        Raises:
            NotImplementedError: This function is not implemented yet.
        """
        msg = "Delete method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def update(self, *args: Any, **kwargs: Any) -> Any:
        """Update a resource.

        This method must be implemented by subclasses with their specific signature.

        Raises:
            NotImplementedError: This function is not implemented yet.
        """
        msg = "Update method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        """Upload one or more resources.

        This method must be implemented by subclasses with their specific signature.

        Raises:
            NotImplementedError: This function is not implemented yet.
        """
        msg = "Upload method not implemented yet."
        raise NotImplementedError(msg)
