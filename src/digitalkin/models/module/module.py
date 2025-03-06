"""Module model."""

from enum import Enum, auto

from pydantic import BaseModel

from digitalkin.services import (
    StorageStrategy,
)


class ModuleStatus(Enum):
    """États possibles d'un module."""

    CREATED = auto()  # Module créé mais pas encore démarré
    STARTING = auto()  # Module en cours de démarrage
    RUNNING = auto()  # Module en cours d'exécution
    STOPPING = auto()  # Module en cours d'arrêt
    STOPPED = auto()  # Module arrêté normalement
    FAILED = auto()  # Module arrêté suite à une erreur
    NOT_FOUND = auto()


class Module(BaseModel):
    """Module model."""

    name: str
    cost_schema: str
    input_schema: str
    output_schema: str
    setup_schema: str
    secret_schema: str
    type: str
    version: str
    description: str


class StrategyConfig(BaseModel):
    """Module config model."""

    storage_strategy: StorageStrategy
    """
    cost_strategy: CostStrategy
    snapshot_strategy: SnapshotStrategy
    registry_strategy: RegistryStrategy
    filesystem_strategy: FilesystemStrategy
    agent_strategy: AgentStrategy
    identity_strategy: IdentityStrategy
    """
    model_config = {"arbitrary_types_allowed": True}
