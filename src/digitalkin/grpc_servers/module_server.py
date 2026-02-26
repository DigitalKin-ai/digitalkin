"""Module gRPC server implementation for DigitalKin."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

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
        interceptors: Sequence[Any] | None = None,
    ) -> None:
        """Initialize the module server.

        Args:
            module_class: The module instance to be served.
            server_config: Server configuration.
            client_config: Client configuration used by services and registry connection.
            interceptors: Optional sequence of gRPC server interceptors.
        """
        super().__init__(server_config, interceptors=interceptors)
        self.module_class = module_class
        self.server_config = server_config
        self.client_config = client_config
        self.module_servicer: ModuleServicer | None = None
        self.registry: RegistryStrategy | None = None

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

        # Initialize setup stub before server starts accepting RPCs
        if self.client_config is not None:
            self.module_servicer.setup.__post_init__(self.client_config)

        logger.debug("Registered Module servicer")

    def _prepare_registry_config(self) -> None:
        """Prepare registry client config on module_class before server starts.

        This ensures ServicesConfig created by JobManager will have registry config,
        allowing spawned module instances to inherit the registry configuration.
        """
        if not self.client_config:
            return

        self.module_class.services_config_params["registry"] = {"client_config": self.client_config}

    def _init_registry(self) -> None:
        """Initialize server-level registry client for registration."""
        if not self.client_config:
            return

        self.registry = GrpcRegistry("", "", "", self.client_config)

    def start(self) -> None:
        """Start the module server and register with the registry if configured."""
        import asyncio

        logger.info("Starting module server", extra={"server_config": self.server_config})
        super().start()

        try:
            self._init_registry()
            asyncio.get_event_loop().run_until_complete(self._register_with_registry())
        except Exception:
            logger.exception("Failed to register with registry")

    async def start_async(self) -> None:
        """Start the module server and register with the registry if configured."""
        logger.info("Starting module server", extra={"server_config": self.server_config})
        await super().start_async()

        # module_servicer is now set by _register_servicers() during super().start_async()
        if self.module_servicer is not None:
            await self.module_servicer.job_manager.start()

        try:
            self._init_registry()
            await self._register_with_registry()
        except Exception:
            logger.exception("Failed to register with registry")

    async def stop_async(self, grace: float | None = None) -> None:
        """Stop the module server with async cleanup.

        Deregisters from registry and stops the server. Modules also become
        inactive when they stop sending heartbeats as a fallback.
        """
        if self.registry is not None:
            try:
                module_id = self.module_class.get_module_id()
                if module_id and module_id != "unknown":
                    await self.registry.deregister(module_id)
            except Exception:
                logger.exception("Failed to deregister from registry")

        await super().stop_async(grace)

    async def _register_with_registry(self) -> None:
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

        advertise_address = self.server_config.advertise_host or self.server_config.host

        logger.info(
            "Attempting to register module with registry",
            extra={
                "module_id": module_id,
                "address": advertise_address,
                "port": self.server_config.port,
                "version": version,
            },
        )

        result = await self.registry.register(
            module_id=module_id,
            address=advertise_address,
            port=self.server_config.port,
            version=version,
        )

        if result:
            logger.info(
                "Module registered successfully",
                extra={
                    "module_id": result.module_id,
                    "address": advertise_address,
                    "port": self.server_config.port,
                },
            )
        else:
            logger.warning(
                "Module registration returned None (module may not exist in registry)",
                extra={"module_id": module_id, "address": advertise_address},
            )
