"""Mock Registry Servicer for testing the GrpcRegistry service."""

from typing import Any

import grpc
from agentic_mesh_protocol.registry.v1 import (
    registry_enums_pb2,
    registry_models_pb2,
    registry_requests_pb2,
    registry_service_pb2_grpc,
)

from digitalkin.logger import logger


class MockRegistryServicer(registry_service_pb2_grpc.RegistryServiceServicer):
    """Mock implementation of the Registry Service Servicer for testing."""

    def __init__(self) -> None:
        """Initialize the mock servicer with empty registry."""
        super().__init__()
        # module_id -> module data
        self.registered_modules: dict[str, dict[str, Any]] = {}

    def _create_module_descriptor(self, module_data: dict[str, Any]) -> registry_models_pb2.ModuleDescriptor:
        """Create a ModuleDescriptor from module data.

        Args:
            module_data: The module data dictionary.

        Returns:
            ModuleDescriptor protobuf message.
        """
        # Map module type string to proto enum
        type_mapping = {
            "archetype": registry_enums_pb2.MODULE_TYPE_ARCHETYPE,
            "tool": registry_enums_pb2.MODULE_TYPE_TOOL,
        }
        module_type = type_mapping.get(module_data.get("module_type", ""), registry_enums_pb2.MODULE_TYPE_UNSPECIFIED)

        return registry_models_pb2.ModuleDescriptor(
            id=module_data["module_id"],
            name=module_data.get("name", module_data["module_id"]),
            module_type=module_type,
            address=module_data["address"],
            port=module_data["port"],
            version=module_data["version"],
            documentation=module_data.get("documentation", ""),
            status=module_data.get("status", registry_enums_pb2.MODULE_STATUS_READY),
        )

    def RegisterModule(
        self,
        request: registry_requests_pb2.RegisterModuleRequest,
        context: grpc.ServicerContext,
    ) -> registry_requests_pb2.RegisterModuleResponse:
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
            return registry_requests_pb2.RegisterModuleResponse()

        # Update the module info
        self.registered_modules[module_id].update({
            "address": request.address,
            "port": request.port,
            "version": request.version,
            "status": registry_enums_pb2.MODULE_STATUS_ACTIVE,
        })

        logger.debug("Mock: Module %s registered at %s:%d", module_id, request.address, request.port)
        return registry_requests_pb2.RegisterModuleResponse(
            module=self._create_module_descriptor(self.registered_modules[module_id])
        )

    def Heartbeat(
        self,
        request: registry_requests_pb2.HeartbeatRequest,
        context: grpc.ServicerContext,
    ) -> registry_requests_pb2.HeartbeatResponse:
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
            return registry_requests_pb2.HeartbeatResponse(status=registry_enums_pb2.MODULE_STATUS_UNSPECIFIED)

        # Update status to ACTIVE and return
        self.registered_modules[module_id]["status"] = registry_enums_pb2.MODULE_STATUS_ACTIVE
        return registry_requests_pb2.HeartbeatResponse(status=registry_enums_pb2.MODULE_STATUS_ACTIVE)

    def DiscoverModules(
        self,
        request: registry_requests_pb2.DiscoverModulesRequest,
        context: grpc.ServicerContext,
    ) -> registry_requests_pb2.DiscoverModulesResponse:
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
            results = [m for m in results if request.query in m.get("name", m["module_id"])]

        # Filter by module types if specified
        if request.module_types:
            type_strings = []
            for mt in request.module_types:
                if mt == registry_enums_pb2.MODULE_TYPE_ARCHETYPE:
                    type_strings.append("archetype")
                elif mt == registry_enums_pb2.MODULE_TYPE_TOOL:
                    type_strings.append("tool")
            if type_strings:
                results = [m for m in results if m.get("module_type", "") in type_strings]

        logger.debug("Mock: Found %d matching modules", len(results))
        return registry_requests_pb2.DiscoverModulesResponse(
            modules=[self._create_module_descriptor(m) for m in results]
        )

    def GetModule(
        self,
        request: registry_requests_pb2.GetModuleRequest,
        context: grpc.ServicerContext,
    ) -> registry_models_pb2.ModuleDescriptor:
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
            return registry_models_pb2.ModuleDescriptor()

        return self._create_module_descriptor(self.registered_modules[request.module_id])

    def DiscoverSetups(
        self,
        request: registry_requests_pb2.DiscoverSetupsRequest,
        context: grpc.ServicerContext,
    ) -> registry_requests_pb2.DiscoverSetupsResponse:
        """Discover setups based on search criteria.

        Args:
            request: The discover setups request.
            context: The gRPC context.

        Returns:
            DiscoverSetupsResponse with matching setups.
        """
        logger.debug("Mock: Discovering setups with query '%s'", request.query)
        # Not implemented in mock - return empty
        return registry_requests_pb2.DiscoverSetupsResponse()

    def GetSetup(
        self,
        request: registry_requests_pb2.GetSetupRequest,
        context: grpc.ServicerContext,
    ) -> registry_models_pb2.SetupDescriptor:
        """Get detailed information about a specific setup.

        Args:
            request: The get setup request.
            context: The gRPC context.

        Returns:
            SetupDescriptor with setup details.
        """
        logger.debug("Mock: Getting setup: %s", request.setup_id)
        # Not implemented in mock - return empty
        context.set_code(grpc.StatusCode.NOT_FOUND)
        return registry_models_pb2.SetupDescriptor()
