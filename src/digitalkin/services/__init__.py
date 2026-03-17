"""This package contains the abstract base class for all services."""

from digitalkin.services.agent import AgentStrategy, DefaultAgent
from digitalkin.services.communication import CommunicationStrategy, DefaultCommunication, GrpcCommunication
from digitalkin.services.cost import CostStrategy, DefaultCost
from digitalkin.services.filesystem import DefaultFilesystem, FilesystemStrategy
from digitalkin.services.identity import DefaultIdentity, IdentityStrategy
from digitalkin.services.registry import DefaultRegistry, RegistryStrategy
from digitalkin.services.snapshot import DefaultSnapshot, SnapshotStrategy
from digitalkin.services.storage import DefaultStorage, StorageStrategy

__all__ = [
    "AgentStrategy",
    "CommunicationStrategy",
    "CostStrategy",
    "DefaultAgent",
    "DefaultCommunication",
    "DefaultCost",
    "DefaultFilesystem",
    "DefaultIdentity",
    "DefaultRegistry",
    "DefaultSnapshot",
    "DefaultStorage",
    "FilesystemStrategy",
    "GrpcCommunication",
    "IdentityStrategy",
    "RegistryStrategy",
    "SnapshotStrategy",
    "StorageStrategy",
]
