"""Module gRPC server implementation for DigitalKin."""

import logging

import grpc
from digitalkin_proto.digitalkin.module.v1 import module_service_pb2_grpc
from digitalkin_proto.digitalkin.module_registry.v1 import module_registry_service_pb2, module_registry_service_pb2_grpc

from digitalkin.grpc._base_server import BaseServer
from digitalkin.grpc.module_servicer import ModuleServicer
from digitalkin.grpc.utils.models import ModuleServerConfig, SecurityMode
from digitalkin.modules._base_module import BaseModule

logger = logging.getLogger(__name__)


class ModuleServer(BaseServer):
    """gRPC server for a DigitalKin module.

    This server exposes the module's functionality through the ModuleService gRPC interface.
    It can optionally register itself with a ModuleRegistry server.

    Attributes:
        module: The module instance being served.
        config: Server configuration.
        module_servicer: The gRPC servicer handling module requests.
    """

    def __init__(
        self,
        module: BaseModule,
        config: ModuleServerConfig,
    ):
        """Initialize the module server.

        Args:
            module: The module instance to be served.
            config: Server configuration including registry address if auto-registration is desired.
        """
        super().__init__(config)
        self.module = module
        self.config = config
        self.module_servicer: ModuleServicer | None = None

    def _register_servicers(self) -> None:
        """Register the module servicer with the gRPC server."""
        if self.server is None:
            raise RuntimeError("Server must be created before registering servicers")

        logger.info(f"Registering module servicer for {self.module.metadata['name']}")
        self.module_servicer = ModuleServicer(self.module)
        module_service_pb2_grpc.add_ModuleServiceServicer_to_server(self.module_servicer, self.server)
        self._servicers.append(self.module_servicer)

    def start(self) -> None:
        """Start the module server and register with the registry if configured."""
        super().start()

        # If a registry address is provided, register the module
        if self.config.registry_address:
            try:
                self._register_with_registry()
            except Exception as e:
                logger.error(f"Failed to register with registry: {e}")

    def stop(self, grace: float | None = None) -> None:
        """Stop the module server and deregister from the registry if needed."""
        # If registered with a registry, deregister
        if self.config.registry_address:
            try:
                self._deregister_from_registry()
            except Exception as e:
                logger.error(f"Failed to deregister from registry: {e}")

        super().stop(grace)

    def _register_with_registry(self) -> None:
        """Register this module with the registry server.

        Raises:
            grpc.RpcError: If communication with the registry server fails.
        """
        logger.info(f"Registering module with registry at {self.config.registry_address}")

        # Create appropriate channel based on security mode
        channel = self._create_registry_channel()

        try:
            with channel:
                # Create a stub (client)
                stub = module_registry_service_pb2_grpc.ModuleRegistryServiceStub(channel)

                # Determine module type
                module_type = self._determine_module_type()

                # Prepare capabilities list
                capabilities = self._get_module_capabilities()

                # Create registration request
                request = module_registry_service_pb2.RegisterRequest(
                    module_info={
                        "module_id": self.module.metadata["module_id"],
                        "name": self.module.metadata["name"],
                        "description": self.module.metadata["description"],
                        "version": self.module.metadata["version"],
                        "type": module_type,
                        "tags": self.module.metadata["tags"],
                        "capabilities": capabilities,
                    },
                    address=self.config.address,
                )

                # Call the register method
                response = stub.RegisterModule(request)

                if response.success:
                    logger.info(f"Module registered successfully: {response.message}")
                else:
                    logger.error(f"Module registration failed: {response.error_message}")
        except grpc.RpcError as e:
            logger.error(f"RPC error during registration: {e.details() if hasattr(e, 'details') else str(e)}")
            raise

    def _deregister_from_registry(self) -> None:
        """Deregister this module from the registry server.

        Raises:
            grpc.RpcError: If communication with the registry server fails.
        """
        logger.info(f"Deregistering module from registry at {self.config.registry_address}")

        # Create appropriate channel based on security mode
        channel = self._create_registry_channel()

        try:
            with channel:
                # Create a stub (client)
                stub = module_registry_service_pb2_grpc.ModuleRegistryServiceStub(channel)

                # Create deregistration request
                request = module_registry_service_pb2.DeregisterRequest(
                    module_id=self.module.metadata["module_id"],
                )

                # Call the deregister method
                response = stub.DeregisterModule(request)

                if response.success:
                    logger.info(f"Module deregistered successfully: {response.message}")
                else:
                    logger.error(f"Module deregistration failed: {response.error_message}")
        except grpc.RpcError as e:
            logger.error(f"RPC error during deregistration: {e.details() if hasattr(e, 'details') else str(e)}")
            raise

    def _create_registry_channel(self) -> grpc.Channel:
        """Create an appropriate channel to the registry server.

        Returns:
            A gRPC channel for communication with the registry.

        Raises:
            ValueError: If credentials are required but not provided.
        """
        if self.config.security == SecurityMode.SECURE and self.config.credentials:
            # Secure channel
            with open(self.config.credentials.server_cert_path, "rb") as cert_file:
                certificate_chain = cert_file.read()

            root_certificates = None
            if self.config.credentials.root_cert_path:
                with open(self.config.credentials.root_cert_path, "rb") as root_cert_file:
                    root_certificates = root_cert_file.read()

            # Create channel credentials
            channel_credentials = grpc.ssl_channel_credentials(root_certificates=root_certificates or certificate_chain)

            return grpc.secure_channel(self.config.registry_address, channel_credentials)
        else:
            # Insecure channel
            return grpc.insecure_channel(self.config.registry_address)

    def _determine_module_type(self) -> str:
        """Determine the module type based on its class.

        Returns:
            A string representing the module type.
        """
        module_type = "UNKNOWN"
        class_name = self.module.__class__.__name__

        if class_name == "ToolModule":
            module_type = "TOOL"
        elif class_name == "TriggerModule":
            module_type = "TRIGGER"
        elif class_name == "ArchetypeModule":
            module_type = "KIN"

        return module_type

    def _get_module_capabilities(self) -> list[str]:
        """Get the capabilities of the module.

        Returns:
            A list of capability strings.
        """
        if hasattr(self.module, "capabilities"):
            return self.module.capabilities
        return []
