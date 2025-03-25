"""This module contains the strategy models for the services."""

from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel

from digitalkin.logger import logger
from digitalkin.services.agent import AgentStrategy
from digitalkin.services.cost import CostStrategy
from digitalkin.services.filesystem import FilesystemStrategy
from digitalkin.services.identity import IdentityStrategy
from digitalkin.services.registry import RegistryStrategy
from digitalkin.services.snapshot import SnapshotStrategy
from digitalkin.services.storage import StorageStrategy

# Define type variables
T = TypeVar(
    "T",
    bound=AgentStrategy
    | CostStrategy
    | FilesystemStrategy
    | IdentityStrategy
    | RegistryStrategy
    | SnapshotStrategy
    | StorageStrategy,
)


class ServicesMode(str, Enum):
    """Mode for strategy execution."""

    LOCAL = "local"
    REMOTE = "remote"


class ServicesStrategy(BaseModel, Generic[T]):
    """Service class describing the available services in a Module with local and remote attributes.

    Attributes:
        local: type
        remote: type
    """

    local: type[T]
    remote: type[T]

    def __getitem__(self, mode: str) -> type[T]:
        """Get the service strategy based on the mode.

        Args:
            mode (str): The mode to get the strategy for.

        Returns:
            The strategy based on the mode.
        """
        try:
            return getattr(self, mode)
        except AttributeError:
            logger.exception("Unknown mode: %s, available modes are: %s", mode, ServicesMode.__members__)
            return getattr(self, ServicesMode.LOCAL.value)
