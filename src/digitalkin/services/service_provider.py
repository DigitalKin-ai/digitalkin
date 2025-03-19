"""Service Provider definitions."""

from typing import ClassVar

from pydantic import BaseModel

from digitalkin.services.agent.agent_strategy import AgentStrategy
from digitalkin.services.cost.cost_strategy import CostStrategy
from digitalkin.services.filesystem.filesystem_strategy import FilesystemStrategy
from digitalkin.services.identity.identity_strategy import IdentityStrategy
from digitalkin.services.registry.registry_strategy import RegistryStrategy
from digitalkin.services.snapshot.snapshot_strategy import SnapshotStrategy
from digitalkin.services.storage.default_storage import DefaultStorage
from digitalkin.services.storage.storage_strategy import StorageStrategy


class ServiceProvider(BaseModel):
    """Service class describing the available services in a Module."""

    storage: ClassVar[StorageStrategy]
    cost: ClassVar[CostStrategy]
    snapshot: ClassVar[SnapshotStrategy]
    registry: ClassVar[RegistryStrategy]
    filesystem: ClassVar[FilesystemStrategy]
    agent: ClassVar[AgentStrategy]
    identity: ClassVar[IdentityStrategy]

    model_config = {"arbitrary_types_allowed": True}


class DefaultServiceProvider(ServiceProvider):
    """Service Instance used as default service in a Module.

    Currently only allow the default (local) database service.
    """

    storage = DefaultStorage()


class DevelopmentServiceProvider(ServiceProvider):
    """Service Instance used as a development service in a Module.

    Not implemented
    """
