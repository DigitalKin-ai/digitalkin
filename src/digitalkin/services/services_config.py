"""Service Provider definitions."""

from typing import Any, ClassVar

from pydantic import BaseModel, Field, PrivateAttr

from digitalkin.services.agent import AgentStrategy, DefaultAgent
from digitalkin.services.cost import CostStrategy, DefaultCost, GrpcCost
from digitalkin.services.filesystem import DefaultFilesystem, FilesystemStrategy, GrpcFilesystem
from digitalkin.services.identity import DefaultIdentity, IdentityStrategy
from digitalkin.services.registry import DefaultRegistry, RegistryStrategy
from digitalkin.services.services_models import ServicesMode, ServicesStrategy
from digitalkin.services.snapshot import DefaultSnapshot, SnapshotStrategy
from digitalkin.services.storage import DefaultStorage, GrpcStorage, StorageStrategy


class ServicesConfig(BaseModel):
    """Service class describing the available services in a Module.

    This class manages the strategy implementations for various services,
    allowing them to be switched between local and remote modes.
    """

    # Mode setting for all strategies
    mode: ServicesMode = Field(default=ServicesMode.LOCAL, description="The mode of the services (local or remote)")

    # Strategy definitions with proper type annotations
    _storage: ServicesStrategy[StorageStrategy] = PrivateAttr(
        default_factory=lambda: ServicesStrategy(local=DefaultStorage, remote=GrpcStorage)
    )
    _config_storage: dict[str, Any | None] = PrivateAttr(default_factory=dict)
    _cost: ServicesStrategy[CostStrategy] = PrivateAttr(
        default_factory=lambda: ServicesStrategy(local=DefaultCost, remote=GrpcCost)
    )
    _config_cost: dict[str, Any | None] = PrivateAttr(default_factory=dict)
    _snapshot: ServicesStrategy[SnapshotStrategy] = PrivateAttr(
        default_factory=lambda: ServicesStrategy(local=DefaultSnapshot, remote=DefaultSnapshot)
    )
    _config_snapshot: dict[str, Any | None] = PrivateAttr(default_factory=dict)
    _registry: ServicesStrategy[RegistryStrategy] = PrivateAttr(
        default_factory=lambda: ServicesStrategy(local=DefaultRegistry, remote=DefaultRegistry)
    )
    _config__registry: dict[str, Any | None] = PrivateAttr(default_factory=dict)
    _filesystem: ServicesStrategy[FilesystemStrategy] = PrivateAttr(
        default_factory=lambda: ServicesStrategy(local=DefaultFilesystem, remote=GrpcFilesystem)
    )
    _config_filesystem: dict[str, Any | None] = PrivateAttr(default_factory=dict)
    _agent: ServicesStrategy[AgentStrategy] = PrivateAttr(
        default_factory=lambda: ServicesStrategy(local=DefaultAgent, remote=DefaultAgent)
    )
    _config_agent: dict[str, Any | None] = PrivateAttr(default_factory=dict)
    _identity: ServicesStrategy[IdentityStrategy] = PrivateAttr(
        default_factory=lambda: ServicesStrategy(local=DefaultIdentity, remote=DefaultIdentity)
    )
    _config_identity: dict[str, Any | None] = PrivateAttr(default_factory=dict)

    # List of valid strategy names for validation
    _valid_strategy_names: ClassVar[set[str]] = {
        "storage",
        "cost",
        "snapshot",
        "registry",
        "filesystem",
        "agent",
        "identity",
    }

    def __init__(
        self,
        services_config_strategies: dict[str, ServicesStrategy | None] = {},
        services_config_params: dict[str, dict[str, Any | None] | None] = {},
        mode: ServicesMode = ServicesMode.LOCAL,
        **kwargs: dict[str, Any],
    ) -> None:
        """Initialize the service configuration with optional strategy overrides.

        Args:
            services_config_strategies: Dictionary mapping service names to strategy implementations
            services_config_params: Dictionary mapping service names to configuration parameters
            mode: The mode of the services (local or remote)
            **kwargs: Additional keyword arguments passed to the parent class constructor
        """
        super().__init__(**kwargs)
        self.mode = mode
        # Apply any strategy overrides
        if services_config_strategies:
            for name, strategy in services_config_strategies.items():
                if strategy is not None and name in self._valid_strategy_names:
                    setattr(self, f"_{name}", strategy)

        for name in self.valid_strategy_names():
            setattr(self, f"_config_{name}", services_config_params.get(name, {}))

    @classmethod
    def valid_strategy_names(cls) -> set[str]:
        """Get the list of valid strategy names.

        Returns:
            The set of valid strategy names.
        """
        return cls._valid_strategy_names

    def get_strategy_config(self, name: str) -> dict[str, Any]:
        """Get the configuration for a specific strategy.

        Args:
            name: The name of the strategy to retrieve the configuration for

        Returns:
            The configuration for the specified strategy, or None if not found
        """
        return getattr(self, f"_config_{name}", {})

    def init_strategy(self, name: str, mission_id: str, setup_id: str, setup_version_id: str) -> ServicesStrategy:
        """Initialize a specific strategy.

        Args:
            name: The name of the strategy to initialize
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The setup ID for the strategy
            setup_version_id: The setup version ID for the strategy

        Returns:
            The initialized strategy instance

        Raises:
            ValueError: If the strategy is not found
        """
        strategy_type = getattr(self, name, None)
        if strategy_type is None:
            msg = f"Strategy {name} not found in ServicesConfig."
            raise ValueError(msg)

        # Instantiate the strategy with the mission ID, setup version ID, and configuration
        return strategy_type(mission_id, setup_id, setup_version_id, **self.get_strategy_config(name) or {})

    @property
    def storage(self) -> type[StorageStrategy]:
        """Get the storage service strategy class based on the current mode."""
        return self._storage[self.mode.value]

    @property
    def cost(self) -> type[CostStrategy]:
        """Get the cost service strategy class based on the current mode."""
        return self._cost[self.mode.value]

    @property
    def snapshot(self) -> type[SnapshotStrategy]:
        """Get the snapshot service strategy class based on the current mode."""
        return self._snapshot[self.mode.value]

    @property
    def registry(self) -> type[RegistryStrategy]:
        """Get the registry service strategy class based on the current mode."""
        return self._registry[self.mode.value]

    @property
    def filesystem(self) -> type[FilesystemStrategy]:
        """Get the filesystem service strategy class based on the current mode."""
        return self._filesystem[self.mode.value]

    @property
    def agent(self) -> type[AgentStrategy]:
        """Get the agent service strategy class based on the current mode."""
        return self._agent[self.mode.value]

    @property
    def identity(self) -> type[IdentityStrategy]:
        """Get the identity service strategy class based on the current mode."""
        return self._identity[self.mode.value]

    def update_mode(self, mode: ServicesMode) -> None:
        """Update the strategy mode.

        Parameters:
            mode: The new mode to use for all strategies
        """
        self.mode = mode
