"""Define the module context used in the triggers."""

import os
from datetime import tzinfo
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from digitalkin.services.agent.agent_strategy import AgentStrategy
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.cost.cost_strategy import CostStrategy
from digitalkin.services.filesystem.filesystem_strategy import FilesystemStrategy
from digitalkin.services.identity.identity_strategy import IdentityStrategy
from digitalkin.services.registry.registry_strategy import RegistryStrategy
from digitalkin.services.snapshot.snapshot_strategy import SnapshotStrategy
from digitalkin.services.storage.storage_strategy import StorageStrategy
from digitalkin.services.user_profile.user_profile_strategy import UserProfileStrategy


class Session(SimpleNamespace):
    """Session data container with mandatory setup_id and mission_id."""

    job_id: str
    mission_id: str
    setup_id: str
    setup_version_id: str
    timezone: tzinfo

    def __init__(
        self,
        job_id: str,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        timezone: tzinfo | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        """Init Module Session.

        Raises:
            ValueError: If mandatory args are missing.
        """
        if not setup_id:
            msg = "setup_id is mandatory"
            raise ValueError(msg)
        if not setup_version_id:
            msg = "setup_version_id is mandatory"
            raise ValueError(msg)
        if not mission_id:
            msg = "mission_id is mandatory"
            raise ValueError(msg)
        if not job_id:
            msg = "job_id is mandatory"
            raise ValueError(msg)

        self.job_id = job_id
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id
        self.timezone = timezone or ZoneInfo(os.environ.get("DIGITALKIN_TIMEZONE", "Europe/Paris"))

        super().__init__(**kwargs)

    def current_ids(self) -> dict[str, str]:
        """Return current session ids as a dictionary.

        Returns:
            A dictionary containing the current session ids.
        """
        return {
            "job_id": self.job_id,
            "mission_id": self.mission_id,
            "setup_id": self.setup_id,
            "setup_version_id": self.setup_version_id,
        }


class ModuleContext:
    """ModuleContext provides a container for strategies and resources used by a module.

    This context object is designed to be passed to module components, providing them with
    access to shared strategies and resources. Additional attributes may be set dynamically.
    """

    # services list
    agent: AgentStrategy
    communication: CommunicationStrategy
    cost: CostStrategy
    filesystem: FilesystemStrategy
    identity: IdentityStrategy
    registry: RegistryStrategy
    snapshot: SnapshotStrategy
    storage: StorageStrategy
    user_profile: UserProfileStrategy

    session: Session
    callbacks: SimpleNamespace
    metadata: SimpleNamespace
    helpers: SimpleNamespace
    state: SimpleNamespace = SimpleNamespace()

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        agent: AgentStrategy,
        communication: CommunicationStrategy,
        cost: CostStrategy,
        filesystem: FilesystemStrategy,
        identity: IdentityStrategy,
        registry: RegistryStrategy,
        snapshot: SnapshotStrategy,
        storage: StorageStrategy,
        user_profile: UserProfileStrategy,
        session: dict[str, Any],
        metadata: dict[str, Any] = {},
        helpers: dict[str, Any] = {},
        callbacks: dict[str, Any] = {},
    ) -> None:
        """Register mandatory services, session, metadata and callbacks.

        Args:
            agent: AgentStrategy.
            communication: CommunicationStrategy.
            cost: CostStrategy.
            filesystem: FilesystemStrategy.
            identity: IdentityStrategy.
            registry: RegistryStrategy.
            snapshot: SnapshotStrategy.
            storage: StorageStrategy.
            user_profile: UserProfileStrategy.
            metadata: dict defining differents Module metadata.
            helpers: dict different user defined helpers.
            session: dict referring the session IDs or informations.
            callbacks: Functions allowing user to agent interaction.
        """
        # Core services
        self.agent = agent
        self.communication = communication
        self.cost = cost
        self.filesystem = filesystem
        self.identity = identity
        self.registry = registry
        self.snapshot = snapshot
        self.storage = storage
        self.user_profile = user_profile

        self.metadata = SimpleNamespace(**metadata)
        self.session = Session(**session)
        self.helpers = SimpleNamespace(**helpers)
        self.callbacks = SimpleNamespace(**callbacks)
