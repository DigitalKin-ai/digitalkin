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
        client_config: ClientConfig | None = None,
        interceptors: Sequence[Any] | None = None,
    ) -> None:
        """Initialize the module server.

        Args:
            module_class: The module instance to be served.
            client_config: Client configuration used by services and registry connection.
            interceptors: Optional sequence of gRPC server interceptors.
        """
        super().__init__(interceptors=interceptors)
        self.module_class = module_class
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

        # Ensure we have a per-class copy (not shared with parent) before mutation
        if "services_config_params" not in self.module_class.__dict__:
            self.module_class.services_config_params = dict(self.module_class.services_config_params)
        self.module_class.services_config_params["registry"] = {"client_config": self.client_config}

    def _init_registry(self) -> None:
        """Initialize server-level registry client for registration."""
        if not self.client_config:
            return

        self.registry = GrpcRegistry("", "", "", self.client_config)

    def start(self) -> None:
        """Start the module server and register with the registry if configured."""
        import asyncio

        logger.info("Starting module server", extra={"server_config": self._server_settings})
        super().start()

        try:
            self._init_registry()
            asyncio.get_event_loop().run_until_complete(self._register_with_registry())
        except Exception:
            logger.exception("Failed to register with registry")

    async def start_async(self) -> None:
        """Start the module server and register with the registry if configured."""
        logger.info("Starting module server", extra={"server_config": self._server_settings})
        await super().start_async()

        # module_servicer is now set by _register_servicers() during super().start_async()
        if self.module_servicer is not None:
            logger.debug("debug:start_async job_manager type=%s", type(self.module_servicer.job_manager).__name__)
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
                    logger.debug("debug:stop_async deregistering module_id=%s", module_id)
                    await self.registry.deregister(module_id)
            except Exception:
                logger.exception("Failed to deregister from registry")

        # Shut down servicer-level resources (GrpcSetup channel, registry cache)
        if self.module_servicer is not None:
            try:
                await self.module_servicer.shutdown()
            except Exception:
                logger.exception("Failed to shutdown module servicer resources")

            try:
                await self.module_servicer.job_manager.stop_all_modules()
            except Exception:
                logger.exception("Failed to stop all modules during shutdown")

            try:
                await self.module_servicer.job_manager.stop()
            except Exception:
                logger.exception("Failed to stop job manager during shutdown")

        # Close server-level registry channel
        if isinstance(self.registry, GrpcRegistry):
            try:
                await self.registry.close_channel()
            except Exception:
                logger.exception("Failed to close server registry channel")

        logger.debug("debug:stop_async stopping gRPC server grace=%s", grace)
        await super().stop_async(grace)

    async def _register_with_registry(self) -> None:
        """Register this module with the registry server.

        Probes the services-provider channel for readiness (1s max) before
        attempting registration.  When the provider is unreachable the module
        still starts — it just won't be discoverable until the next restart
        or a manual re-registration.
        """
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

        advertise_address = self._server_settings.channel.advertise_host or self._server_settings.channel.host

        # Fast connectivity probe — detect DOWN in ≤1 s
        if not await self.registry.wait_for_ready(timeout=1.0):
            logger.error(
                "Services provider is DOWN — channel not ready after 1 s, "
                "skipping registration (module will start without registry)",
                extra={
                    "module_id": module_id,
                    "address": advertise_address,
                    "port": self._server_settings.channel.port,
                },
            )
            return

        logger.info(
            "Attempting to register module with registry",
            extra={
                "module_id": module_id,
                "address": advertise_address,
                "port": self._server_settings.channel.port,
                "version": version,
            },
        )

        result = await self.registry.register(
            module_id=module_id,
            address=advertise_address,
            port=self._server_settings.channel.port,
            version=version,
        )

        if result:
            logger.info(
                "Module registered successfully",
                extra={
                    "module_id": result.module_id,
                    "address": advertise_address,
                    "port": self._server_settings.channel.port,
                },
            )
        else:
            logger.warning(
                "Module registration returned None (module may not exist in registry)",
                extra={"module_id": module_id, "address": advertise_address},
            )
