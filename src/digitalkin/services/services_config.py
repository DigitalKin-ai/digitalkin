"""Service Provider definitions."""

from typing import Any, ClassVar

from pydantic import BaseModel, Field, PrivateAttr

from digitalkin.models.services.services import ServicesMode
from digitalkin.services.communication import CommunicationStrategy, DefaultCommunication, GrpcCommunication
from digitalkin.services.cost import CostStrategy, DefaultCost, GrpcCost
from digitalkin.services.filesystem import DefaultFilesystem, FilesystemStrategy, GrpcFilesystem
from digitalkin.services.identity import DefaultIdentity, IdentityStrategy
from digitalkin.services.registry import DefaultRegistry, GrpcRegistry, RegistryStrategy
from digitalkin.services.secret import DefaultSecret, GrpcSecret, SecretStrategy
from digitalkin.services.services_models import ServicesStrategy
from digitalkin.services.storage import DefaultStorage, GrpcStorage, StorageStrategy
from digitalkin.services.user_profile import DefaultUserProfile, GrpcUserProfile, UserProfileStrategy


class ServicesConfig(BaseModel):
    """Service class describing the available services in a Module.

    This class manages the strategy implementations for various services,
    allowing them to be switched between local and remote modes.
    """

    mode: ServicesMode = Field(default=ServicesMode.LOCAL, description="The mode of the services (local or remote)")

    _strategies: dict[str, ServicesStrategy] = PrivateAttr(default_factory=dict)
    _configs: dict[str, dict[str, Any | None]] = PrivateAttr(default_factory=dict)
    _singleton_cache: dict[str, Any] = PrivateAttr(default_factory=dict)

    _valid_strategy_names: ClassVar[set[str]] = {
        "storage",
        "cost",
        "registry",
        "filesystem",
        "identity",
        "communication",
        "user_profile",
        "secret",
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

        # No per-request IDs → safe to share as singletons.
        self._stateless_strategies: frozenset[str] = frozenset({"registry", "communication"})

        defaults: dict[str, ServicesStrategy] = {
            "storage": ServicesStrategy(local=DefaultStorage, remote=GrpcStorage),
            "cost": ServicesStrategy(local=DefaultCost, remote=GrpcCost),
            "registry": ServicesStrategy(local=DefaultRegistry, remote=GrpcRegistry),
            "filesystem": ServicesStrategy(local=DefaultFilesystem, remote=GrpcFilesystem),
            "identity": ServicesStrategy(local=DefaultIdentity, remote=DefaultIdentity),
            "communication": ServicesStrategy(local=DefaultCommunication, remote=GrpcCommunication),
            "user_profile": ServicesStrategy(local=DefaultUserProfile, remote=GrpcUserProfile),
            "secret": ServicesStrategy(local=DefaultSecret, remote=GrpcSecret),
        }

        # Apply strategy overrides
        for name in self._valid_strategy_names:
            override = services_config_strategies.get(name)
            self._strategies[name] = override if override is not None else defaults[name]
            self._configs[name] = services_config_params.get(name) or {}

        # The secret service is backed by the UserProfileService — reuse the
        # user_profile client_config (same host/port) when no dedicated secret
        # config is registered, so GrpcSecret can build its channel.
        if not self._configs.get("secret"):
            self._configs["secret"] = self._configs.get("user_profile") or {}

        # AssociateTask is minted by the backend (same services-provider as user_profile /
        # CheckResourceAccess). In REMOTE mode, give the communication client that backend
        # address so it can dial AssociateTask for M2M tool calls. Skipped in LOCAL (no backend,
        # and DefaultCommunication takes no such arg).
        up_client_config = (self._configs.get("user_profile") or {}).get("client_config")
        if up_client_config is not None:
            self._configs["communication"].setdefault("gateway_backend_config", up_client_config)

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

        strategy_class = strategy[self.mode.value]

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
        """The storage service strategy class for the current mode."""
        return self._strategies["storage"][self.mode.value]

    @property
    def cost(self) -> type[CostStrategy]:
        """The cost service strategy class for the current mode."""
        return self._strategies["cost"][self.mode.value]

    @property
    def registry(self) -> type[RegistryStrategy]:
        """The registry service strategy class for the current mode."""
        return self._strategies["registry"][self.mode.value]

    @property
    def filesystem(self) -> type[FilesystemStrategy]:
        """The filesystem service strategy class for the current mode."""
        return self._strategies["filesystem"][self.mode.value]

    @property
    def identity(self) -> type[IdentityStrategy]:
        """The identity service strategy class for the current mode."""
        return self._strategies["identity"][self.mode.value]

    @property
    def communication(self) -> type[CommunicationStrategy]:
        """The communication service strategy class for the current mode."""
        return self._strategies["communication"][self.mode.value]

    @property
    def user_profile(self) -> type[UserProfileStrategy]:
        """The user_profile service strategy class for the current mode."""
        return self._strategies["user_profile"][self.mode.value]

    @property
    def secret(self) -> type[SecretStrategy]:
        """The secret service strategy class for the current mode."""
        return self._strategies["secret"][self.mode.value]

    def update_mode(self, mode: ServicesMode) -> None:
        """Update the strategy mode.

        Parameters:
            mode: The new mode to use for all strategies
        """
        self.mode = mode
        self._singleton_cache.clear()
