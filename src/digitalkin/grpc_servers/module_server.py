"""Module gRPC server implementation for DigitalKin."""

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from agentic_mesh_protocol.gateway.v1 import gateway_service_pb2_grpc
from agentic_mesh_protocol.module.v1 import module_service_pb2_grpc

from digitalkin.core.task_manager.redis import RedisClient
from digitalkin.grpc_servers._base_server import BaseServer
from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.grpc_servers.module_servicer import ModuleServicer
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.registry import GrpcRegistry

if TYPE_CHECKING:
    from digitalkin.services.registry import RegistryStrategy


class ModuleServer(BaseServer):
    """gRPC server for a DigitalKin module.

    Attributes:
        module_class: The module class being served.
        client_config: Client configuration for services and registry.
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
            module_class: The module class to serve.
            client_config: Client configuration for services and registry.
            interceptors: Optional gRPC server interceptors.
        """
        all_interceptors = list(interceptors) if interceptors else []
        self._gateway_circuit_breaker: Any = None
        redis_url = os.environ.get("DIGITALKIN_REDIS_URL")
        if redis_url:
            from digitalkin.grpc_servers.interceptors.circuit_breaker_interceptor import CircuitBreakerInterceptor
            from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

            self._gateway_circuit_breaker = CircuitBreaker(
                service_id="gateway",
                fail_max=int(os.environ.get("DIGITALKIN_CB_FAIL_MAX", "5")),
                reset_timeout=float(os.environ.get("DIGITALKIN_CB_RESET_TIMEOUT", "10")),
            )
            all_interceptors.append(CircuitBreakerInterceptor(self._gateway_circuit_breaker))

        super().__init__(interceptors=all_interceptors or None)
        self.module_class = module_class
        self.client_config = client_config
        self.registry: RegistryStrategy | None = None
        self.module_servicer: ModuleServicer | None = None
        self._gateway_servicer: GatewayServicer | None = None
        self._task_dispatcher: Any = None

        self._prepare_registry_config()

    def _register_servicers(self) -> None:
        """Register module and gateway servicers.

        Raises:
            RuntimeError: If server is not created yet.
        """
        if self.server is None:
            msg = "Server must be created before registering servicers"
            raise RuntimeError(msg)

        logger.debug("Registering module servicer for %s", self.module_class.__name__)
        self.module_servicer = ModuleServicer(self.module_class)
        self.register_servicer(
            self.module_servicer,
            module_service_pb2_grpc.add_ModuleServiceServicer_to_server,
            service_names=["agentic_mesh_protocol.module.v1.ModuleService"],
        )

        if self.client_config is not None:
            self.module_servicer.setup.__post_init__(self.client_config)

        logger.debug("Registered Module servicer")
        self._register_gateway_servicer()

    def _register_gateway_servicer(self) -> None:
        """Register the embedded GatewayServicer and TaskDispatcher.

        Gateway dispatches tasks via Redis XADD. TaskDispatcher picks them
        up via XREAD and runs modules through ModuleServicer's job manager.

        Raises:
            RuntimeError: If DIGITALKIN_REDIS_URL is not set.
        """
        redis_url = os.environ.get("DIGITALKIN_REDIS_URL")
        if not redis_url:
            msg = "DIGITALKIN_REDIS_URL is required. The gateway needs Redis for stream persistence."
            raise RuntimeError(msg)

        redis_client = RedisClient(redis_url)
        module_id = self.module_class.get_module_id()
        dispatch_key = f"dispatch:{module_id}"

        self._gateway_servicer = GatewayServicer(
            redis_client=redis_client,
            circuit_breaker=self._gateway_circuit_breaker,
            dispatch_key=dispatch_key,
        )

        self.register_servicer(
            self._gateway_servicer,
            gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server,
            service_names=["agentic_mesh_protocol.gateway.v1.GatewayService"],
        )

        # TaskDispatcher reads from the same dispatch stream
        from digitalkin.core.task_manager.task_dispatcher import TaskDispatcher

        self._task_dispatcher = TaskDispatcher(
            redis_client=redis_client,
            servicer=self.module_servicer,
            dispatch_key=dispatch_key,
        )

        logger.info("GatewayServicer + TaskDispatcher registered (Redis: %s, dispatch: %s)", redis_url, dispatch_key)

    def _prepare_registry_config(self) -> None:
        """Inject registry client config into module_class for spawned instances."""
        if not self.client_config:
            return

        if "services_config_params" not in self.module_class.__dict__:
            self.module_class.services_config_params = dict(self.module_class.services_config_params)
        self.module_class.services_config_params["registry"] = {"client_config": self.client_config}

    async def _init_and_register(self) -> None:
        """Initialize registry client, health-check, and register.

        Raises:
            RuntimeError: If client_config is missing, module_id is invalid,
                registry is unreachable, or registration fails.
        """
        if not self.client_config:
            msg = "client_config is required for registry registration"
            raise RuntimeError(msg)

        self.registry = GrpcRegistry("", "", "", self.client_config)

        if not await self.registry.wait_for_ready():
            msg = "Registry server is unreachable (health check failed after 1s)"
            raise RuntimeError(msg)

        module_id = self.module_class.get_module_id()
        version = self.module_class.metadata.get("version", "0.0.0")

        if not module_id or module_id == "unknown":
            msg = (
                f"Module {self.module_class.__name__} has no valid module_id. "
                "Set DIGITALKIN_MODULE_ID or define metadata['module_id']."
            )
            raise RuntimeError(msg)

        advertise_address = self._server_settings.channel.advertise_host or self._server_settings.channel.host

        logger.info(
            "Registering module with registry",
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

        if not result:
            msg = f"Registry registration failed for module_id={module_id}"
            raise RuntimeError(msg)

        logger.info(
            "Module registered successfully",
            extra={
                "module_id": result.module_id,
                "address": advertise_address,
                "port": self._server_settings.channel.port,
            },
        )

    async def start_async(self) -> None:
        """Start the module server.

        Raises:
            RuntimeError: If module_servicer failed to initialize.
        """
        logger.info("Starting module server", extra={"server_config": self._server_settings})
        await super().start_async()

        if self.module_servicer is None:
            msg = "module_servicer was not initialized during server startup"
            raise RuntimeError(msg)

        logger.debug("debug:start_async job_manager type=%s", type(self.module_servicer.job_manager).__name__)
        await self.module_servicer.job_manager.start()

        if self._gateway_servicer is not None:
            await self._gateway_servicer.start()

        if self._task_dispatcher is not None:
            await self._task_dispatcher.start()

        if self.client_config is not None:
            await self._init_and_register()

    async def _shutdown_servicer(self) -> None:
        """Shut down the module servicer and its job manager."""
        if self.module_servicer is None:
            return
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

    async def stop_async(self, grace: float | None = None) -> None:
        """Stop the module server with async cleanup."""
        if self.registry is not None:
            try:
                module_id = self.module_class.get_module_id()
                if module_id and module_id != "unknown":
                    logger.debug("debug:stop_async deregistering module_id=%s", module_id)
                    await self.registry.deregister(module_id)
            except Exception:
                logger.exception("Failed to deregister from registry")

        await self._shutdown_servicer()

        if self.registry is not None:
            try:
                await self.registry.close()
            except Exception:
                logger.exception("Failed to close registry")

        if self._task_dispatcher is not None:
            try:
                await self._task_dispatcher.stop()
            except Exception:
                logger.exception("Failed to stop task dispatcher")

        if self._gateway_servicer is not None:
            try:
                await self._gateway_servicer.stop()
            except Exception:
                logger.exception("Failed to stop gateway servicer")

        logger.debug("debug:stop_async stopping gRPC server grace=%s", grace)
        await super().stop_async(grace)
