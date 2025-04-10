"""Registry gRPC server implementation for DigitalKin."""

import logging

from digitalkin_proto.digitalkin.module_registry.v2 import (
    module_registry_service_pb2,
    module_registry_service_pb2_grpc,
)

from digitalkin.grpc_servers._base_server import BaseServer
from digitalkin.grpc_servers.registry_servicer import RegistryModule, RegistryServicer
from digitalkin.grpc_servers.utils.models import RegistryServerConfig

logger = logging.getLogger(__name__)


class RegistryServer(BaseServer):
    """gRPC server for DigitalKin module registry.

    This server implements the ModuleRegistryService which allows modules to register
    themselves and be discovered by other components in the system.

    Attributes:
        config: Server configuration.
        registry_servicer: The gRPC servicer handling registry requests.
    """

    def __init__(
        self,
        config: RegistryServerConfig,
    ) -> None:
        """Initialize the registry server.

        Args:
            config: Server configuration.
        """
        super().__init__(config)
        self.config = config
        self.registry_servicer: RegistryServicer | None = None

    def _register_servicers(self) -> None:
        """Register the registry servicer with the gRPC server.

        Raises:
            RuntimeError: If server is not registered during server creation
        """
        if self.server is None:
            msg = "Server must be created before registering servicers"
            raise RuntimeError(msg)

        logger.info("Registering registry servicer")
        self.registry_servicer = RegistryServicer()
        self.register_servicer(
            self.registry_servicer,
            module_registry_service_pb2_grpc.add_ModuleRegistryServiceServicer_to_server,
            service_descriptor=module_registry_service_pb2.DESCRIPTOR,
        )
        logger.info("Registered registry servicer")

    def get_registered_modules(self) -> list[RegistryModule]:
        """Get a list of all registered modules.

        Returns:
            A list of module information objects.
        """
        if self.registry_servicer:
            return list(self.registry_servicer.registered_modules.values())
        return []
