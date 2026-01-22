"""gRPC Registry client implementation.

This module provides a gRPC-based registry client that communicates with
the Service Provider's Registry service.
"""

from typing import Any

from agentic_mesh_protocol.registry.v1 import (
    registry_dto_pb2,
    registry_messages_pb2,
    registry_service_pb2_grpc,
)

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.registry.registry_exceptions import (
    RegistryModuleNotFoundError,
    RegistryServiceError,
)
from digitalkin.services.registry.registry_models import ModuleInfo, ModuleStatus, ModuleType
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

    # ════════════════════════════════ Private Methods ═════════════════════════════════ #

    @staticmethod
    def __proto_to_module_info(
        descriptor: registry_messages_pb2.ModuleDescriptor,
    ) -> ModuleInfo:
        """Convert proto ModuleDescriptor to ModuleInfo.

        Args:
            descriptor: Proto ModuleDescriptor message.

        Returns:
            ModuleInfo with mapped fields.
        """
        return ModuleInfo(
            id=descriptor.id,
            type=ModuleType.from_proto(descriptor.type),
            address=descriptor.address,
            port=descriptor.port,
            version=descriptor.version,
            name=descriptor.name,
            documentation=descriptor.documentation or None,
            status=ModuleStatus.from_proto(descriptor.status),
        )

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    def search(
        self,
        name: str | None = None,
            module_type: ModuleType | None = None,
        organization_id: str | None = None,
    ) -> list[ModuleInfo]:
        logger.debug(
            "Searching modules",
            extra={
                "name": name,
                "module_type": module_type,
                "organization_id": organization_id,
            },
        )

        with self.handle_grpc_errors("SearchModules", RegistryServiceError):
            module_types = []
            if module_type:
                module_types.append(module_type.to_proto())

            try:
                response = self.exec_grpc_query(
                    "SearchModules",
                    registry_dto_pb2.SearchModulesRequest(
                        query=name or "",
                        organization_id=organization_id or "",
                        module_types=module_types,
                    ),
                )
            except ServerError as e:
                msg = f"Failed to search modules: {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            logger.debug("Search returned %d modules", len(response.result))
            return [self.__proto_to_module_info(m.module_descriptor) for m in response.result]

    def get(self, module_id: str) -> ModuleInfo:
        logger.debug("Discovering module by ID", extra={"id": module_id})

        with self.handle_grpc_errors("GetModule", RegistryServiceError):
            try:
                response = self.exec_grpc_query(
                    "GetModule",
                    registry_dto_pb2.GetModuleRequest(module_id=module_id),
                )
            except ServerError as e:
                msg = f"Failed to discover module '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            if not response.result.success:
                logger.warning("Module not found in registry", extra={"module_id": module_id})
                raise RegistryModuleNotFoundError(module_id)

            logger.debug(
                "Module discovered",
                extra={
                    "module_id": response.result.module_descriptor.id,
                    "address": response.result.module_descriptor.address,
                    "port": response.result.module_descriptor.port,
                },
            )
            return self.__proto_to_module_info(response.result.module_descriptor)

    def get_status(self, module_id: str) -> ModuleStatus:
        logger.debug("Getting module status", extra={"module_id": module_id})

        with self.handle_grpc_errors("GetStatus", RegistryServiceError):
            try:
                response = self.exec_grpc_query(
                    "GetModuleStatus",
                    registry_dto_pb2.GetModuleRequest(module_id=module_id),
                )
            except ServerError as e:
                msg = f"Failed to get module status for '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            if not response.result.success:
                logger.warning("Module not found in registry", extra={"module_id": module_id})
                raise RegistryModuleNotFoundError(module_id)

            status_name = ModuleStatus.from_proto(response.result.module_descriptor.status)
            logger.debug(
                "Module status retrieved",
                extra={"module_id": response.result.module_descriptor.id, "status": status_name},
            )
            return status_name

    def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
    ) -> ModuleInfo | None:
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
                    registry_dto_pb2.RegisterModuleRequest(
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

            if not response.result.success:
                logger.warning(
                    "Registry returned empty response for module registration",
                    extra={"module_id": module_id},
                )
                return None

            logger.info(
                "Module registered successfully",
                extra={
                    "module_id": response.result.module_descriptor.id,
                    "address": response.result.module_descriptor.address,
                    "port": response.result.module_descriptor.port,
                },
            )
            return self.__proto_to_module_info(response.result.module_descriptor)

    def heartbeat(self, module_id: str) -> ModuleStatus:
        logger.debug("Sending heartbeat", extra={"module_id": module_id})

        with self.handle_grpc_errors("Heartbeat", RegistryServiceError):
            try:
                response = self.exec_grpc_query(
                    "Heartbeat",
                    registry_dto_pb2.HeartbeatRequest(module_id=module_id),
                )
            except ServerError as e:
                msg = f"Failed to send heartbeat for '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            status_name = ModuleStatus.from_proto(response.status)
            logger.debug(
                "Heartbeat response",
                extra={"module_id": module_id, "status": status_name},
            )
            return status_name
