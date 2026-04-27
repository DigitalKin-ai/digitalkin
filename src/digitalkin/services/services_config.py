"""Service Provider definitions."""

from typing import Any, ClassVar

from pydantic import BaseModel, Field, PrivateAttr

from digitalkin.services.communication import CommunicationStrategy, DefaultCommunication, GrpcCommunication
from digitalkin.services.cost import CostStrategy, DefaultCost, GrpcCost
from digitalkin.services.filesystem import DefaultFilesystem, FilesystemStrategy, GrpcFilesystem
from digitalkin.services.identity import DefaultIdentity, IdentityStrategy
from digitalkin.services.registry import DefaultRegistry, GrpcRegistry, RegistryStrategy
from digitalkin.services.services_models import ServicesMode, ServicesStrategy
from digitalkin.services.storage import DefaultStorage, GrpcStorage, StorageStrategy
from digitalkin.services.user_profile import DefaultUserProfile, GrpcUserProfile, UserProfileStrategy


class ServicesConfig(BaseModel):
    """Service class describing the available services in a Module.

    This class manages the strategy implementations for various services,
    allowing them to be switched between local and remote modes.
    """

    # Mode setting for all strategies
    mode: ServicesMode = Field(default=ServicesMode.LOCAL, description="The mode of the services (local or remote)")

    # Strategies and configs stored in dicts for typed lookup (avoids getattr/setattr)
    _strategies: dict[str, ServicesStrategy] = PrivateAttr(default_factory=dict)
    _configs: dict[str, dict[str, Any | None]] = PrivateAttr(default_factory=dict)
    _singleton_cache: dict[str, Any] = PrivateAttr(default_factory=dict)

    # List of valid strategy names for validation
    _valid_strategy_names: ClassVar[set[str]] = {
        "storage",
        "cost",
        "registry",
        "filesystem",
        "identity",
        "communication",
        "user_profile",
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

        # Strategies that never use per-request IDs — safe to share as singletons
        self._stateless_strategies: frozenset[str] = frozenset({"registry", "communication"})

        # Default strategy definitions
        defaults: dict[str, ServicesStrategy] = {
            "storage": ServicesStrategy(local=DefaultStorage, remote=GrpcStorage),
            "cost": ServicesStrategy(local=DefaultCost, remote=GrpcCost),
            "registry": ServicesStrategy(local=DefaultRegistry, remote=GrpcRegistry),
            "filesystem": ServicesStrategy(local=DefaultFilesystem, remote=GrpcFilesystem),
            "identity": ServicesStrategy(local=DefaultIdentity, remote=DefaultIdentity),
            "communication": ServicesStrategy(local=DefaultCommunication, remote=GrpcCommunication),
            "user_profile": ServicesStrategy(local=DefaultUserProfile, remote=GrpcUserProfile),
        }

        # Apply strategy overrides
        for name in self._valid_strategy_names:
            override = services_config_strategies.get(name)
            self._strategies[name] = override if override is not None else defaults[name]
            self._configs[name] = services_config_params.get(name) or {}

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
            The configuration for the specified strategy, or empty dict if not found
        """
        return self._configs.get(name, {})

    def init_strategy(self, name: str, mission_id: str, setup_id: str, setup_version_id: str) -> Any:
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
        strategy = self._strategies.get(name)
        if strategy is None:
            msg = f"Strategy {name} not found in ServicesConfig."
            raise ValueError(msg)

        # Resolve the concrete strategy class via mode, then instantiate
        strategy_class = strategy[self.mode.value]

        # Stateless strategies (no per-request IDs used) — return cached singleton
        if name in self._stateless_strategies:
            cached = self._singleton_cache.get(name)
            if cached is not None:
                return cached
            instance = strategy_class(mission_id, setup_id, setup_version_id, **self.get_strategy_config(name) or {})
            self._singleton_cache[name] = instance
            return instance

        return strategy_class(mission_id, setup_id, setup_version_id, **self.get_strategy_config(name) or {})

    @property
    def storage(self) -> type[StorageStrategy]:
        """Get the storage service strategy class based on the current mode."""
        return self._strategies["storage"][self.mode.value]

    @property
    def cost(self) -> type[CostStrategy]:
        """Get the cost service strategy class based on the current mode."""
        return self._strategies["cost"][self.mode.value]

    @property
    def registry(self) -> type[RegistryStrategy]:
        """Get the registry service strategy class based on the current mode."""
        return self._strategies["registry"][self.mode.value]

    @property
    def filesystem(self) -> type[FilesystemStrategy]:
        """Get the filesystem service strategy class based on the current mode."""
        return self._strategies["filesystem"][self.mode.value]

    @property
    def identity(self) -> type[IdentityStrategy]:
        """Get the identity service strategy class based on the current mode."""
        return self._strategies["identity"][self.mode.value]

    @property
    def communication(self) -> type[CommunicationStrategy]:
        """Get the communication service strategy class based on the current mode."""
        return self._strategies["communication"][self.mode.value]

    @property
    def user_profile(self) -> type[UserProfileStrategy]:
        """Get the user_profile service strategy class based on the current mode."""
        return self._strategies["user_profile"][self.mode.value]

    def update_mode(self, mode: ServicesMode) -> None:
        """Update the strategy mode.

        Parameters:
            mode: The new mode to use for all strategies
        """
        self.mode = mode
        self._singleton_cache.clear()
