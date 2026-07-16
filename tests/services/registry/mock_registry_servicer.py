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
        # setup_id -> setup data
        self.setups: dict[str, dict[str, Any]] = {}

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
            "tool_module": registry_enums_pb2.MODULE_TYPE_TOOL_MODULE,
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

        # Update the module info; a declared module_type overrides the stored one
        self.registered_modules[module_id].update({
            "address": request.address,
            "port": request.port,
            "version": request.version,
            "status": registry_enums_pb2.MODULE_STATUS_ACTIVE,
        })
        if request.module_type != registry_enums_pb2.MODULE_TYPE_UNSPECIFIED:
            self.registered_modules[module_id]["module_type"] = (
                registry_enums_pb2.ModuleType.Name(request.module_type).removeprefix("MODULE_TYPE_").lower()
            )

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

    def _create_module_summary(self, module_data: dict[str, Any]) -> registry_models_pb2.ModuleSummary:
        """Create a ModuleSummary from module data.

        Args:
            module_data: The module data dictionary.

        Returns:
            ModuleSummary protobuf message.
        """
        type_mapping = {
            "archetype": registry_enums_pb2.MODULE_TYPE_ARCHETYPE,
            "tool_module": registry_enums_pb2.MODULE_TYPE_TOOL_MODULE,
        }
        return registry_models_pb2.ModuleSummary(
            id=module_data["module_id"],
            name=module_data.get("name", module_data["module_id"]),
            module_type=type_mapping.get(module_data.get("module_type", ""), registry_enums_pb2.MODULE_TYPE_UNSPECIFIED),
            version=module_data.get("version", ""),
            status=module_data.get("status", registry_enums_pb2.MODULE_STATUS_READY),
            visibility=module_data.get("visibility", registry_enums_pb2.VISIBILITY_PRIVATE),
            organization_id=module_data.get("organization_id", ""),
            documentation=module_data.get("documentation", ""),
        )

    def SearchModules(
        self,
        request: registry_requests_pb2.SearchModulesRequest,
        context: grpc.ServicerContext,
    ) -> registry_requests_pb2.SearchModulesResponse:
        """Search modules based on search criteria.

        Args:
            request: The search modules request.
            context: The gRPC context.

        Returns:
            SearchModulesResponse with matching module summaries and total count.
        """
        logger.debug("Mock: Searching modules with query '%s'", request.query)

        results = list(self.registered_modules.values())

        if request.module_ids:
            results = [m for m in results if m["module_id"] in request.module_ids]
        if request.module_types:
            type_strings = [
                registry_enums_pb2.ModuleType.Name(mt).removeprefix("MODULE_TYPE_").lower()
                for mt in request.module_types
            ]
            results = [m for m in results if m.get("module_type", "") in type_strings]
        if request.query:
            needle = request.query.lower()
            results = [
                m
                for m in results
                if needle in m.get("name", m["module_id"]).lower() or needle in m.get("documentation", "").lower()
            ]

        total = len(results)
        limit = request.limit or 20
        results = results[request.offset : request.offset + limit]

        logger.debug("Mock: Found %d matching modules (returning %d)", total, len(results))
        return registry_requests_pb2.SearchModulesResponse(
            modules=[self._create_module_summary(m) for m in results],
            total=total,
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

    def _create_setup_summary(self, setup_data: dict[str, Any]) -> registry_models_pb2.SetupSummary:
        """Create a SetupSummary from setup data.

        Args:
            setup_data: The setup data dictionary.

        Returns:
            SetupSummary protobuf message.
        """
        type_mapping = {
            "archetype": registry_enums_pb2.MODULE_TYPE_ARCHETYPE,
            "tool_module": registry_enums_pb2.MODULE_TYPE_TOOL_MODULE,
        }
        return registry_models_pb2.SetupSummary(
            id=setup_data["setup_id"],
            name=setup_data.get("name", setup_data["setup_id"]),
            documentation=setup_data.get("documentation", ""),
            status=setup_data.get("status", registry_enums_pb2.SETUP_STATUS_READY),
            visibility=setup_data.get("visibility", registry_enums_pb2.VISIBILITY_PRIVATE),
            organization_id=setup_data.get("organization_id", ""),
            module_id=setup_data.get("module_id", ""),
            module_name=setup_data.get("module_name", ""),
            module_type=type_mapping.get(setup_data.get("module_type", ""), registry_enums_pb2.MODULE_TYPE_UNSPECIFIED),
            setup_version_id=setup_data.get("setup_version_id", ""),
            setup_version=setup_data.get("setup_version", ""),
        )

    def SearchSetups(
        self,
        request: registry_requests_pb2.SearchSetupsRequest,
        context: grpc.ServicerContext,
    ) -> registry_requests_pb2.SearchSetupsResponse:
        """Search setups based on search criteria.

        Args:
            request: The search setups request.
            context: The gRPC context.

        Returns:
            SearchSetupsResponse with matching setup summaries and total count.
        """
        logger.debug("Mock: Searching setups with query '%s'", request.query)

        results = list(self.setups.values())

        if request.setup_ids:
            results = [s for s in results if s["setup_id"] in request.setup_ids]
        if request.module_ids:
            results = [s for s in results if s.get("module_id", "") in request.module_ids]
        if request.module_types:
            type_strings = [
                registry_enums_pb2.ModuleType.Name(mt).removeprefix("MODULE_TYPE_").lower()
                for mt in request.module_types
            ]
            results = [s for s in results if s.get("module_type", "") in type_strings]
        if request.statuses:
            results = [s for s in results if s.get("status", registry_enums_pb2.SETUP_STATUS_READY) in request.statuses]
        if request.query:
            needle = request.query.lower()
            results = [
                s
                for s in results
                if needle in s.get("name", s["setup_id"]).lower() or needle in s.get("documentation", "").lower()
            ]

        total = len(results)
        limit = request.limit or 20
        results = results[request.offset : request.offset + limit]

        logger.debug("Mock: Found %d matching setups (returning %d)", total, len(results))
        return registry_requests_pb2.SearchSetupsResponse(
            setups=[self._create_setup_summary(s) for s in results],
            total=total,
        )

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
