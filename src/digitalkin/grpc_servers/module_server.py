"""Module gRPC server implementation for DigitalKin."""

from typing import TYPE_CHECKING

from agentic_mesh_protocol.module.v1 import (
    module_service_pb2,
    module_service_pb2_grpc,
)

from digitalkin.grpc_servers._base_server import BaseServer
from digitalkin.grpc_servers.module_servicer import ModuleServicer
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import (
    ClientConfig,
    ModuleServerConfig,
    SecurityMode,
)
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.registry import GrpcRegistry

if TYPE_CHECKING:
    from digitalkin.services.registry import RegistryStrategy


class ModuleServer(BaseServer):
    """gRPC server for a DigitalKin module.

    This server exposes the module's functionality through the ModuleService gRPC interface.
    It can optionally register itself with a Registry server.

    Attributes:
        module: The module instance being served.
        server_config: Server configuration.
        client_config: Setup client configuration.
        module_servicer: The gRPC servicer handling module requests.
    """

    def __init__(
        self,
        module_class: type[BaseModule],
        server_config: ModuleServerConfig,
        client_config: ClientConfig | None = None,
    ) -> None:
        """Initialize the module server.

        Args:
            module_class: The module instance to be served.
            server_config: Server configuration including registry address if auto-registration is desired.
            client_config: Client configuration used by services.
        """
        super().__init__(server_config)
        self.module_class = module_class
        self.server_config = server_config
        self.client_config = client_config
        self.module_servicer: ModuleServicer | None = None
        self.registry: RegistryStrategy | None = None

        self._registry_client_config: ClientConfig | None = None
        if self.server_config.registry_address:
            self._registry_client_config = self._build_registry_client_config()
        self._prepare_registry_config()

    def _register_servicers(self) -> None:
        """Register the module servicer with the gRPC server.

        Raises:
            RuntimeError: No registered server
        """
        if self.server is None:
            msg = "Server must be created before registering servicers"
            raise RuntimeError(msg)

        logger.debug("Registering module servicer for %s", self.module_class.__name__)
        self.module_servicer = ModuleServicer(self.module_class)
        self.register_servicer(
            self.module_servicer,
            module_service_pb2_grpc.add_ModuleServiceServicer_to_server,
            service_descriptor=module_service_pb2.DESCRIPTOR,
        )
        logger.debug("Registered Module servicer")

    def _prepare_registry_config(self) -> None:
        """Prepare registry client config on module_class before server starts.

        This ensures ServicesConfig created by JobManager will have registry config,
        allowing spawned module instances to inherit the registry configuration.
        """
        if not self._registry_client_config:
            return

        self.module_class.services_config_params["registry"] = {"client_config": self._registry_client_config}

    def _build_registry_client_config(self) -> ClientConfig:
        """Build ClientConfig for registry from server_config.registry_address.

        Returns:
            ClientConfig configured for registry connection.
        """
        host, port = self.server_config.registry_address.rsplit(":", 1)
        return ClientConfig(
            host=host,
            port=int(port),
            mode=self.server_config.mode,
            security=self.client_config.security if self.client_config else SecurityMode.INSECURE,
            credentials=self.client_config.credentials if self.client_config else None,
        )

    def _init_registry(self) -> None:
        """Initialize server-level registry client for registration.

        Note: services_config_params["registry"] is already set in _prepare_registry_config()
        which runs in __init__(). This method only creates the server-level client instance.
        """
        if not self._registry_client_config:
            return

        self.registry = GrpcRegistry("", "", "", self._registry_client_config)

    def start(self) -> None:
        """Start the module server and register with the registry if configured."""
        logger.info("Starting module server", extra={"server_config": self.server_config})
        super().start()

        try:
            self._init_registry()
            self._register_with_registry()
        except Exception:
            logger.exception("Failed to register with registry")

        if self.module_servicer is not None:
            logger.debug("Setup post init started", extra={"client_config": self.client_config})
            self.module_servicer.setup.__post_init__(self.client_config)

    async def start_async(self) -> None:
        """Start the module server and register with the registry if configured."""
        logger.info("Starting module server", extra={"server_config": self.server_config})
        await super().start_async()

        try:
            self._init_registry()
            self._register_with_registry()
        except Exception:
            logger.exception("Failed to register with registry")

        if self.module_servicer is not None:
            logger.info("Setup post init started", extra={"client_config": self.client_config})
            await self.module_servicer.job_manager.start()
            self.module_servicer.setup.__post_init__(self.client_config)

    def stop(self, grace: float | None = None) -> None:
        """Stop the module server.

        Modules become inactive when they stop sending heartbeats
        """
        super().stop(grace)

    def _register_with_registry(self) -> None:
        """Register this module with the registry server."""
        if not self.registry:
            logger.debug("No registry configured, skipping registration")
            return

        module_id = self.module_class.get_module_id()
        version = self.module_class.metadata.get("version", "0.0.0")

        if not module_id or module_id == "unknown":
            logger.warning(
                "Module has no valid module_id, skipping registration",
                extra={"module_class": self.module_class.__name__},
            )
            return

        logger.info(
            "Attempting to register module with registry",
            extra={
                "module_id": module_id,
                "address": self.server_config.address,
                "port": self.server_config.port,
                "version": version,
                "registry_address": self.server_config.registry_address,
            },
        )

        result = self.registry.register(
            module_id=module_id,
            address=self.server_config.address,
            port=self.server_config.port,
            version=version,
        )

        if result:
            logger.info(
                "Module registered successfully",
                extra={
                    "module_id": result.module_id,
                    "address": self.server_config.address,
                    "port": self.server_config.port,
                    "registry_address": self.server_config.registry_address,
                },
            )
        else:
            logger.warning(
                "Module registration returned None (module may not exist in registry)",
                extra={
                    "module_id": module_id,
                    "registry_address": self.server_config.registry_address,
                },
            )
