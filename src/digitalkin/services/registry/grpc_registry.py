"""gRPC Registry client implementation.

This module provides a gRPC-based registry client that communicates with
the Service Provider's Registry service.
"""

from typing import Any

from agentic_mesh_protocol.registry.v1 import (
    registry_enums_pb2,
    registry_models_pb2,
    registry_requests_pb2,
    registry_service_pb2_grpc,
)

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.registry import (
    ModuleInfo,
    ModuleStatusInfo,
    RegistryModuleStatus,
    RegistryModuleType,
)
from digitalkin.services.registry.exceptions import (
    RegistryModuleNotFoundError,
    RegistryServiceError,
)
from digitalkin.services.registry.registry_strategy import RegistryStrategy


class GrpcRegistry(RegistryStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC-based registry client.

    This client communicates with the Service Provider's Registry service
    to perform module discovery, registration, and status management operations.
    """

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        client_config: ClientConfig,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the gRPC registry client."""
        RegistryStrategy.__init__(self, mission_id, setup_id, setup_version_id, config)
        self.service_name = "RegistryService"
        self.stub = registry_service_pb2_grpc.RegistryServiceStub(self._init_channel(client_config))
        logger.debug("Channel client 'Registry' initialized successfully")

    @staticmethod
    def _proto_to_module_info(
        descriptor: registry_models_pb2.ModuleDescriptor,
    ) -> ModuleInfo:
        """Convert proto ModuleDescriptor to ModuleInfo.

        Args:
            descriptor: Proto ModuleDescriptor message.

        Returns:
            ModuleInfo with mapped fields.
        """
        type_name = registry_enums_pb2.ModuleType.Name(descriptor.module_type).removeprefix("MODULE_TYPE_")
        return ModuleInfo(
            module_id=descriptor.id,
            module_type=RegistryModuleType[type_name],
            address=descriptor.address,
            port=descriptor.port,
            version=descriptor.version,
            name=descriptor.name,
            documentation=descriptor.documentation or None,
        )

    def discover_by_id(self, module_id: str) -> ModuleInfo:
        """Get module info by ID.

        Args:
            module_id: The module identifier.

        Returns:
            ModuleInfo with module details.

        Raises:
            RegistryModuleNotFoundError: If module not found.
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug("Discovering module by ID", extra={"module_id": module_id})

        with self.handle_grpc_errors("GetModule", RegistryServiceError):
            try:
                response = self.exec_grpc_query(
                    "GetModule",
                    registry_requests_pb2.GetModuleRequest(module_id=module_id),
                )
            except ServerError as e:
                msg = f"Failed to discover module '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            if not response.id:
                logger.warning("Module not found in registry", extra={"module_id": module_id})
                raise RegistryModuleNotFoundError(module_id)

            logger.debug(
                "Module discovered",
                extra={
                    "module_id": response.id,
                    "address": response.address,
                    "port": response.port,
                },
            )
            return self._proto_to_module_info(response)

    def search(
        self,
        name: str | None = None,
        module_type: str | None = None,
        organization_id: str | None = None,
    ) -> list[ModuleInfo]:
        """Search for modules by criteria.

        Args:
            name: Filter by name (partial match via query).
            module_type: Filter by type (archetype, tool).
            organization_id: Filter by organization.

        Returns:
            List of matching modules.

        Raises:
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug(
            "Searching modules",
            extra={
                "name": name,
                "module_type": module_type,
                "organization_id": organization_id,
            },
        )

        with self.handle_grpc_errors("DiscoverModules", RegistryServiceError):
            module_types = []
            if module_type:
                enum_val = RegistryModuleType[module_type.upper()]
                module_types.append(getattr(registry_enums_pb2, f"MODULE_TYPE_{enum_val.name}"))

            try:
                response = self.exec_grpc_query(
                    "DiscoverModules",
                    registry_requests_pb2.DiscoverModulesRequest(
                        query=name or "",
                        organization_id=organization_id or "",
                        module_types=module_types,
                    ),
                )
            except ServerError as e:
                msg = f"Failed to search modules: {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            logger.debug("Search returned %d modules", len(response.modules))
            return [self._proto_to_module_info(m) for m in response.modules]

    def get_status(self, module_id: str) -> ModuleStatusInfo:
        """Get module status by fetching the module.

        Args:
            module_id: The module identifier.

        Returns:
            ModuleStatusInfo with current status.

        Raises:
            RegistryModuleNotFoundError: If module not found.
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug("Getting module status", extra={"module_id": module_id})

        with self.handle_grpc_errors("GetModule", RegistryServiceError):
            try:
                response = self.exec_grpc_query(
                    "GetModule",
                    registry_requests_pb2.GetModuleRequest(module_id=module_id),
                )
            except ServerError as e:
                msg = f"Failed to get module status for '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            if not response.id:
                logger.warning("Module not found in registry", extra={"module_id": module_id})
                raise RegistryModuleNotFoundError(module_id)

            status_name = registry_enums_pb2.ModuleStatus.Name(response.status).removeprefix("MODULE_STATUS_")
            logger.debug(
                "Module status retrieved",
                extra={"module_id": response.id, "status": status_name},
            )
            return ModuleStatusInfo(
                module_id=response.id,
                status=RegistryModuleStatus[status_name],
            )

    def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
    ) -> ModuleInfo | None:
        """Register a module with the registry.

        Note: The new proto only updates address/port/version for an existing module.
        The module must already exist in the registry database.

        Args:
            module_id: Unique module identifier.
            address: Network address.
            port: Network port.
            version: Module version.

        Returns:
            ModuleInfo if successful, None if module not found.

        Raises:
            RegistryServiceError: If gRPC call fails.
        """
        logger.info(
            "Registering module with registry",
            extra={
                "module_id": module_id,
                "address": address,
                "port": port,
                "version": version,
            },
        )

        with self.handle_grpc_errors("RegisterModule", RegistryServiceError):
            try:
                response = self.exec_grpc_query(
                    "RegisterModule",
                    registry_requests_pb2.RegisterModuleRequest(
                        module_id=module_id,
                        address=address,
                        port=port,
                        version=version,
                    ),
                )
            except ServerError as e:
                msg = f"Failed to register module '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            if not response.module or not response.module.id:
                logger.warning(
                    "Registry returned empty response for module registration",
                    extra={"module_id": module_id},
                )
                return None

            logger.info(
                "Module registered successfully",
                extra={
                    "module_id": response.module.id,
                    "address": response.module.address,
                    "port": response.module.port,
                },
            )
            return self._proto_to_module_info(response.module)

    def heartbeat(self, module_id: str) -> RegistryModuleStatus:
        """Send heartbeat to keep module active.

        Args:
            module_id: The module identifier.

        Returns:
            Current module status after heartbeat.

        Raises:
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug("Sending heartbeat", extra={"module_id": module_id})

        with self.handle_grpc_errors("Heartbeat", RegistryServiceError):
            try:
                response = self.exec_grpc_query(
                    "Heartbeat",
                    registry_requests_pb2.HeartbeatRequest(module_id=module_id),
                )
            except ServerError as e:
                msg = f"Failed to send heartbeat for '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            status_name = registry_enums_pb2.ModuleStatus.Name(response.status).removeprefix("MODULE_STATUS_")
            logger.debug(
                "Heartbeat response",
                extra={"module_id": module_id, "status": status_name},
            )
            return RegistryModuleStatus[status_name]
