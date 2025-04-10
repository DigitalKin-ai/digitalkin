"""Module gRPC server implementation for DigitalKin."""

import logging
import uuid

import grpc

from digitalkin.grpc_servers._base_server import BaseServer
from digitalkin.grpc_servers.module_servicer import ModuleServicer
from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.models import ModuleServerConfig, SecurityMode
from digitalkin.modules._base_module import BaseModule

from digitalkin_proto.digitalkin.module.v2 import (
    module_service_pb2,
    module_service_pb2_grpc,
)
from digitalkin_proto.digitalkin.module_registry.v2 import (
    metadata_pb2,
    module_registry_service_pb2_grpc,
    registration_pb2,
)

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
        module_class: type[BaseModule],
        config: ModuleServerConfig,
    ) -> None:
        """Initialize the module server.

        Args:
            module_class: The module instance to be served.
            config: Server configuration including registry address if auto-registration is desired.
        """
        super().__init__(config)
        self.module_class = module_class
        self.config = config
        self.module_servicer: ModuleServicer | None = None

    def _register_servicers(self) -> None:
        """Register the module servicer with the gRPC server.

        Raises:
            RuntimeError: No registered server
        """
        if self.server is None:
            msg = "Server must be created before registering servicers"
            raise RuntimeError(msg)

        logger.info("Registering module servicer for %s", self.module_class.__name__)
        self.module_servicer = ModuleServicer(self.module_class)
        self.register_servicer(
            self.module_servicer,
            module_service_pb2_grpc.add_ModuleServiceServicer_to_server,
            service_descriptor=module_service_pb2.DESCRIPTOR,
        )
        logger.info("Registered Module servicer")

    def start(self) -> None:
        """Start the module server and register with the registry if configured."""
        logger.critical(self.config)
        super().start()

        logger.critical(self.config)
        # If a registry address is provided, register the module
        if self.config.registry_address:
            try:
                self._register_with_registry()
            except Exception:
                logger.exception("Failed to register with registry")

    async def start_async(self) -> None:
        """Start the module server and register with the registry if configured."""
        logger.critical(self.config)
        await super().start_async()

        logger.critical(self.config)
        # If a registry address is provided, register the module
        if self.config.registry_address:
            try:
                self._register_with_registry()
            except Exception:
                logger.exception("Failed to register with registry")

    def stop(self, grace: float | None = None) -> None:
        """Stop the module server and deregister from the registry if needed."""
        # If registered with a registry, deregister
        if self.config.registry_address:
            try:
                self._deregister_from_registry()
            except ServerError:
                logger.exception("Failed to deregister from registry")

        super().stop(grace)

    def _register_with_registry(self) -> None:
        """Register this module with the registry server.

        Raises:
            ServerError: If communication with the registry server fails.
        """
        logger.info(
            "Registering module with registry at %s", self.config.registry_address
        )

        # Create appropriate channel based on security mode
        channel = self._create_registry_channel()

        with channel:
            # Create a stub (client)
            stub = module_registry_service_pb2_grpc.ModuleRegistryServiceStub(channel)

            # Determine module type
            module_type = self._determine_module_type()

            metadata = metadata_pb2.Metadata(
                name=self.module_class.metadata["name"],
                tags=[
                    metadata_pb2.Tag(tag=tag)
                    for tag in self.module_class.metadata["tags"]
                ],
                description=self.module_class.metadata["description"],
            )

            self.module_class.metadata["module_id"] = (
                f"{self.module_class.metadata['name']}:{uuid.uuid4()}"
            )
            # Create registration request
            request = registration_pb2.RegisterRequest(
                module_id=self.module_class.metadata["module_id"],
                version=self.module_class.metadata["version"],
                module_type=module_type,
                address=self.config.address,
                metadata=metadata,
            )

            try:
                # Call the register method
                logger.info(
                    "Request sent to registry for module: %s:%s",
                    self.module_class.metadata["name"],
                    self.module_class.metadata["module_id"],
                )
                response = stub.RegisterModule(request)

                if response.success:
                    logger.info("Module registered successfully")
                else:
                    logger.error("Module registration failed")
            except grpc.RpcError:
                logger.exception("RPC error during registration:")
                raise ServerError

    def _deregister_from_registry(self) -> None:
        """Deregister this module from the registry server.

        Raises:
            ServerError: If communication with the registry server fails.
        """
        logger.info(
            "Deregistering module from registry at %s", self.config.registry_address
        )

        # Create appropriate channel based on security mode
        channel = self._create_registry_channel()

        with channel:
            # Create a stub (client)
            stub = module_registry_service_pb2_grpc.ModuleRegistryServiceStub(channel)

            # Create deregistration request
            request = registration_pb2.DeregisterRequest(
                module_id=self.module_class.metadata["module_id"],
            )
        try:
            # Call the deregister method
            response = stub.DeregisterModule(request)

            if response.success:
                logger.info("Module deregistered successfull")
            else:
                logger.error("Module deregistration failed")
        except grpc.RpcError:
            logger.exception("RPC error during deregistration")
            raise ServerError

    def _create_registry_channel(self) -> grpc.Channel:
        """Create an appropriate channel to the registry server.

        Returns:
            A gRPC channel for communication with the registry.

        Raises:
            ValueError: If credentials are required but not provided.
        """
        if self.config.security == SecurityMode.SECURE and self.config.credentials:
            # Secure channel
            """TODO: use Path(self.config.credentials.server_cert_path).read_bytes()"""
            with open(self.config.credentials.server_cert_path, "rb") as cert_file:  # noqa: FURB101
                certificate_chain = cert_file.read()

            root_certificates = None
            if self.config.credentials.root_cert_path:
                with open(
                    self.config.credentials.root_cert_path, "rb"
                ) as root_cert_file:  # noqa: FURB101
                    root_certificates = root_cert_file.read()

            # Create channel credentials
            channel_credentials = grpc.ssl_channel_credentials(
                root_certificates=root_certificates or certificate_chain
            )

            return grpc.secure_channel(
                self.config.registry_address, channel_credentials
            )
        # Insecure channel
        return grpc.insecure_channel(self.config.registry_address)

    def _determine_module_type(self) -> str:
        """Determine the module type based on its class.

        Returns:
            A string representing the module type.
        """
        module_type = "UNKNOWN"
        class_name = self.module_class.__name__

        if class_name == "ToolModule":
            module_type = "TOOL"
        elif class_name == "TriggerModule":
            module_type = "TRIGGER"
        elif class_name == "ArchetypeModule":
            module_type = "KIN"

        return module_type
