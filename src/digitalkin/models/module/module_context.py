"""Define the module context used in the triggers."""

from types import SimpleNamespace
from typing import Any

from digitalkin.services.agent.agent_strategy import AgentStrategy
from digitalkin.services.cost.cost_strategy import CostStrategy
from digitalkin.services.filesystem.filesystem_strategy import FilesystemStrategy
from digitalkin.services.identity.identity_strategy import IdentityStrategy
from digitalkin.services.registry.registry_strategy import RegistryStrategy
from digitalkin.services.snapshot.snapshot_strategy import SnapshotStrategy
from digitalkin.services.storage.storage_strategy import StorageStrategy


class Session(SimpleNamespace):
    """Session data container with mandatory setup_id and mission_id."""

    mission_id: str
    setup_version_id: str

    def __init__(self, mission_id: str, setup_version_id: str, **kwargs: dict[str, Any]) -> None:
        """Init Module Session.

        Args:
            mission_id: current mission_id.
            setup_version_id: used setup config.
            kwargs: user defined session variables.

        Raises:
            ValueError: If mandatory args are missing
        """
        if not setup_version_id:
            msg = "setup_version_id is mandatory and cannot be empty"
            raise ValueError(msg)
        if not mission_id:
            msg = "mission_id is mandatory and cannot be empty"
            raise ValueError(msg)

        self.mission_id = mission_id
        self.setup_version_id = setup_version_id

        super().__init__(**kwargs)


class ModuleContext:
    """ModuleContext provides a container for strategies and resources used by a module.

    This context object is designed to be passed to module components, providing them with
    access to shared strategies and resources. Additional attributes may be set dynamically.
    """

    # services list
    agent: AgentStrategy
    cost: CostStrategy
    filesystem: FilesystemStrategy
    identity: IdentityStrategy
    registry: RegistryStrategy
    snapshot: SnapshotStrategy
    storage: StorageStrategy

    session: Session
    callbacks: SimpleNamespace
    metadata: SimpleNamespace
    helpers: SimpleNamespace
    state: SimpleNamespace = SimpleNamespace()

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        agent: AgentStrategy,
        cost: CostStrategy,
        filesystem: FilesystemStrategy,
        identity: IdentityStrategy,
        registry: RegistryStrategy,
        snapshot: SnapshotStrategy,
        storage: StorageStrategy,
        session: dict[str, Any],
        metadata: dict[str, Any] = {},
        helpers: dict[str, Any] = {},
        callbacks: dict[str, Any] = {},
    ) -> None:
        """Register mandatory services, session, metadata and callbacks.

        Args:
            agent: AgentStrategy.
            cost: CostStrategy.
            filesystem: FilesystemStrategy.
            identity: IdentityStrategy.
            registry: RegistryStrategy.
            snapshot: SnapshotStrategy.
            storage: StorageStrategy.
            metadata: dict defining differents Module metadata.
            helpers: dict different user defined helpers.
            session: dict referring the session IDs or informations.
            callbacks: Functions allowing user to agent interaction.
        """
        # Core services
        self.agent = agent
        self.cost = cost
        self.filesystem = filesystem
        self.identity = identity
        self.registry = registry
        self.snapshot = snapshot
        self.storage = storage

        self.metadata = SimpleNamespace(**metadata)
        self.session = Session(**session)
        self.helpers = SimpleNamespace(**helpers)
        self.callbacks = SimpleNamespace(**callbacks)
