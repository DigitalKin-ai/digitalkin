"""Mock Registry Servicer for testing the GrpcRegistry service."""

from typing import Any

import grpc
from agentic_mesh_protocol.pagination.v1 import bulk_pb2
from agentic_mesh_protocol.registry.v1 import (
    registry_dto_pb2,
    registry_messages_pb2,
    registry_service_pb2_grpc,
)

from digitalkin.logger import logger
from digitalkin.services.registry import ModuleType, ModuleStatus


class MockRegistryServicer(registry_service_pb2_grpc.RegistryServiceServicer):
    """Mock implementation of the Registry Service Servicer for testing."""

    def __init__(self) -> None:
        """Initialize the mock servicer with empty registry."""
        super().__init__()
        # module_id -> module data
        self.registered_modules: dict[str, dict[str, Any]] = {}

    @staticmethod
    def __create_module_descriptor(module_data: dict[str, Any]) -> registry_messages_pb2.ModuleDescriptor:
        """Create a ModuleDescriptor from module data.

        Args:
            module_data: The module data dictionary.

        Returns:
            ModuleDescriptor protobuf message.
        """
        return registry_messages_pb2.ModuleDescriptor(
            id=module_data["id"],
            name=module_data.get("name", module_data["id"]),
            type=module_data["type"].to_proto(),
            address=module_data["address"],
            port=module_data["port"],
            version=module_data["version"],
            documentation=module_data.get("documentation", ""),
            status=module_data["status"].to_proto(),
        )

    def RegisterModule(
        self,
            request: registry_dto_pb2.RegisterModuleRequest,
        context: grpc.ServicerContext,
    ) -> registry_dto_pb2.RegisterModuleResponse:
        """Register a module with the registry.

        Note: In the new proto, RegisterModule updates address/port/version for existing modules.

        Args:
            request: The registration request.
            context: The gRPC context.

        Returns:
            RegisterModuleResponse with module info.
        """
        module_id = request.module_id
        logger.debug("Mock: Registering module: %s", module_id)

        # Check if module exists - update if so, return empty if not
        if module_id not in self.registered_modules:
            # New proto expects module to already exist in registry
            logger.warning("Mock: Module '%s' not found for registration", module_id)
            result = registry_messages_pb2.RegistryResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND)), success=False)
            return registry_dto_pb2.RegisterModuleResponse(result=result)

        # Update the module info
        self.registered_modules[module_id].update({
            "address": request.address,
            "port": request.port,
            "version": request.version,
            "status": ModuleStatus.ACTIVE,
        })

        logger.debug("Mock: Module %s registered at %s:%d", module_id, request.address, request.port)
        result = registry_messages_pb2.RegistryResult(module_descriptor=self.__create_module_descriptor(self.registered_modules[module_id]),
                                                      success=True)
        return registry_dto_pb2.RegisterModuleResponse(result=result)

    def Heartbeat(
        self,
            request: registry_dto_pb2.HeartbeatRequest,
        context: grpc.ServicerContext,
    ) -> registry_dto_pb2.HeartbeatResponse:
        """Process heartbeat from a module.

        Args:
            request: The heartbeat request.
            context: The gRPC context.

        Returns:
            HeartbeatResponse with current status.
        """
        module_id = request.module_id
        logger.debug("Mock: Heartbeat from module: %s", module_id)

        # Check if module exists
        if module_id not in self.registered_modules:
            message = f"Module {module_id} not found in registry"
            logger.warning("Mock: %s", message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return registry_dto_pb2.HeartbeatResponse(status=ModuleStatus.UNSPECIFIED.to_proto())

        # Update status to ACTIVE and return
        self.registered_modules[module_id]["status"] = ModuleStatus.ACTIVE.to_proto()
        return registry_dto_pb2.HeartbeatResponse(status=ModuleStatus.ACTIVE.to_proto())

    def SearchModules(
        self,
            request: registry_dto_pb2.SearchModulesRequest,
        context: grpc.ServicerContext,
    ) -> registry_dto_pb2.SearchModulesResponse:
        """Discover modules based on search criteria.

        Args:
            request: The discover modules request.
            context: The gRPC context.

        Returns:
            DiscoverModulesResponse with matching modules.
        """
        logger.debug("Mock: Discovering modules with query '%s'", request.query)

        results = list(self.registered_modules.values())

        # Filter by query (name match)
        if request.query:
            results = [m for m in results if request.query in m.get("name", m["id"])]

        # Filter by module types if specified
        if request.module_types:
            type_strings = []
            for mt in request.module_types:
                mt = ModuleType.from_proto(mt)
                if mt == ModuleType.ARCHETYPE:
                    type_strings.append(ModuleType.ARCHETYPE)
                elif mt == ModuleType.TOOL:
                    type_strings.append(ModuleType.TOOL)
            if type_strings:
                results = [m for m in results if m.get("type", "") in type_strings]

        logger.debug("Mock: Found %d matching modules", len(results))
        results = [registry_messages_pb2.RegistryResult(module_descriptor=self.__create_module_descriptor(m), success=True) for m in results]
        return registry_dto_pb2.SearchModulesResponse(result=results)

    def GetModule(
        self,
            request: registry_dto_pb2.GetModuleRequest,
        context: grpc.ServicerContext,
    ) -> registry_dto_pb2.GetModuleResponse:
        """Get detailed information about a specific module.

        Args:
            request: The get module request.
            context: The gRPC context.

        Returns:
            ModuleDescriptor with module details.
        """
        logger.debug("Mock: Getting module: %s", request.module_id)

        # Check if module exists
        if request.module_id not in self.registered_modules:
            message = f"Module {request.module_id} not found in registry"
            logger.warning("Mock: %s", message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            result = registry_messages_pb2.RegistryResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND), message=message),
                                                          success=False)
            return registry_dto_pb2.GetModuleResponse(result=result)

        result = registry_messages_pb2.RegistryResult(module_descriptor=self.__create_module_descriptor(self.registered_modules[
                                                                                                            request.module_id]), success=True)
        return registry_dto_pb2.GetModuleResponse(result=result)

    def GetModuleStatus(self, request: registry_dto_pb2.GetModuleStatusRequest,
                        context: grpc.ServicerContext) -> registry_dto_pb2.GetModuleStatusResponse:
        """Get the current status of a module.

        Args:
            request: The get module status request.
            context: The gRPC context.

        Returns:
            GetModuleStatusResponse with the module's current status.
        """
        logger.debug("Mock: Getting status for module: %s", request.module_id)

        # Check if module exists
        if request.module_id not in self.registered_modules:
            message = f"Module {request.module_id} not found in registry"
            logger.warning("Mock: %s", message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return registry_dto_pb2.GetModuleStatusResponse(status=ModuleStatus.UNSPECIFIED.to_proto())

        status = self.registered_modules[request.module_id].get("status", ModuleStatus.ARCHIVED.to_proto())
        return registry_dto_pb2.GetModuleStatusResponse(status=status)
