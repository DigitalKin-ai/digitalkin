"""Mock Registry Servicer for testing the GrpcRegistry service."""

from typing import Any

import grpc
from agentic_mesh_protocol.common.v1 import common_enums_pb2
from agentic_mesh_protocol.module.v1 import module_enums_pb2
from agentic_mesh_protocol.pagination.v1 import bulk_pb2, pagination_pb2
from agentic_mesh_protocol.registry.v1 import (
    registry_dto_pb2,
    registry_messages_pb2,
    registry_service_pb2_grpc,
)
from agentic_mesh_protocol.setup.v1 import setup_enums_pb2

from digitalkin.logger import logger


class MockRegistryServicer(registry_service_pb2_grpc.RegistryServiceServicer):
    """Mock implementation of the Registry Service Servicer for testing.

    Module data carries ``module_type`` as the lower-case proto name (``"tool_module"``,
    ``"archetype"``) or ``""`` for an undeclared type.
    """

    def __init__(self) -> None:
        """Initialize the mock servicer with empty registry."""
        super().__init__()
        # module_id -> module data
        self.registered_modules: dict[str, dict[str, Any]] = {}
        # setup_id -> setup data
        self.setups: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _module_type(name: str) -> module_enums_pb2.ModuleType:
        """Encode a lower-case module type name, ``""`` meaning undeclared.

        Args:
            name: The lower-case proto member name, or ``""``.

        Returns:
            The proto ModuleType.
        """
        return module_enums_pb2.ModuleType.Value(name.upper()) if name else module_enums_pb2.MODULE_TYPE_UNSPECIFIED

    @staticmethod
    def _not_found(identifier: str) -> registry_messages_pb2.RegistryResult:
        """Build the in-band NOT_FOUND outcome.

        Args:
            identifier: The id that was looked up.

        Returns:
            RegistryResult holding an OperationError.
        """
        return registry_messages_pb2.RegistryResult(
            identifier=identifier,
            error=bulk_pb2.OperationError(code="NOT_FOUND", message=f"{identifier} not found in registry"),
        )

    def _create_module_descriptor(self, module_data: dict[str, Any]) -> registry_messages_pb2.ModuleDescriptor:
        """Create a ModuleDescriptor from module data.

        Args:
            module_data: The module data dictionary.

        Returns:
            ModuleDescriptor protobuf message.
        """
        return registry_messages_pb2.ModuleDescriptor(
            id=module_data["module_id"],
            name=module_data.get("name", module_data["module_id"]),
            type=self._module_type(module_data.get("module_type", "")),
            address=module_data["address"],
            port=module_data["port"],
            version=module_data["version"],
            documentation=module_data.get("documentation", ""),
            status=module_data.get("status", module_enums_pb2.READY),
        )

    def RegisterModule(
        self,
            request: registry_dto_pb2.RegisterModuleRequest,
        context: grpc.ServicerContext,
    ) -> registry_dto_pb2.RegisterModuleResponse:
        """Register a module that already exists in the registry.

        Args:
            request: The registration request.
            context: The gRPC context.

        Returns:
            RegisterModuleResponse with the module, or a NOT_FOUND error outcome.
        """
        module_id = request.module_id
        logger.debug("Mock: Registering module: %s", module_id)

        if module_id not in self.registered_modules:
            logger.warning("Mock: Module '%s' not found for registration", module_id)
            return registry_dto_pb2.RegisterModuleResponse(result=self._not_found(module_id))

        self.registered_modules[module_id].update({
            "address": request.address,
            "port": request.port,
            "version": request.version,
            "status": module_enums_pb2.ACTIVE,
            "module_type": module_enums_pb2.ModuleType.Name(request.type).lower(),
        })

        logger.debug("Mock: Module %s registered at %s:%d", module_id, request.address, request.port)
        return registry_dto_pb2.RegisterModuleResponse(
            result=registry_messages_pb2.RegistryResult(
                identifier=module_id,
                module_descriptor=self._create_module_descriptor(self.registered_modules[module_id]),
            )
        )

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

        if module_id not in self.registered_modules:
            message = f"Module {module_id} not found in registry"
            logger.warning("Mock: %s", message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return registry_dto_pb2.HeartbeatResponse(status=module_enums_pb2.MODULE_STATUS_UNSPECIFIED)

        self.registered_modules[module_id]["status"] = module_enums_pb2.ACTIVE
        return registry_dto_pb2.HeartbeatResponse(status=module_enums_pb2.ACTIVE)

    def _create_module_summary(self, module_data: dict[str, Any]) -> registry_messages_pb2.ModuleSummary:
        """Create a ModuleSummary from module data.

        Args:
            module_data: The module data dictionary.

        Returns:
            ModuleSummary protobuf message.
        """
        return registry_messages_pb2.ModuleSummary(
            id=module_data["module_id"],
            name=module_data.get("name", module_data["module_id"]),
            type=self._module_type(module_data.get("module_type", "")),
            version=module_data.get("version", ""),
            status=module_data.get("status", module_enums_pb2.READY),
            visibility=module_data.get("visibility", common_enums_pb2.PRIVATE),
            organization_id=module_data.get("organization_id", ""),
            documentation=module_data.get("documentation", ""),
        )

    def SearchModules(
        self,
            request: registry_dto_pb2.SearchModulesRequest,
        context: grpc.ServicerContext,
    ) -> registry_dto_pb2.SearchModulesResponse:
        """Search modules based on search criteria.

        Args:
            request: The search modules request.
            context: The gRPC context.

        Returns:
            SearchModulesResponse with one module_summary result per match and the bulk total.
        """
        logger.debug("Mock: Searching modules with query '%s'", request.query)

        results = list(self.registered_modules.values())

        if request.module_ids:
            results = [m for m in results if m["module_id"] in request.module_ids]
        if request.module_types:
            type_strings = [module_enums_pb2.ModuleType.Name(mt).lower() for mt in request.module_types]
            results = [m for m in results if m.get("module_type", "") in type_strings]
        if request.query:
            needle = request.query.lower()
            results = [
                m
                for m in results
                if needle in m.get("name", m["module_id"]).lower() or needle in m.get("documentation", "").lower()
            ]

        total = len(results)
        limit = request.pagination.limit or 20
        results = results[request.pagination.offset: request.pagination.offset + limit]

        logger.debug("Mock: Found %d matching modules (returning %d)", total, len(results))
        return registry_dto_pb2.SearchModulesResponse(
            results=[
                registry_messages_pb2.RegistryResult(
                    identifier=m["module_id"], module_summary=self._create_module_summary(m)
                )
                for m in results
            ],
            bulk=bulk_pb2.BulkResponse(
                total_processed=len(results),
                pagination=pagination_pb2.PaginationResponse(total_count=total),
            ),
        )

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
            GetModuleResponse with the module descriptor, or a NOT_FOUND error outcome.
        """
        logger.debug("Mock: Getting module: %s", request.module_id)

        if request.module_id not in self.registered_modules:
            logger.warning("Mock: Module %s not found in registry", request.module_id)
            return registry_dto_pb2.GetModuleResponse(result=self._not_found(request.module_id))

        return registry_dto_pb2.GetModuleResponse(
            result=registry_messages_pb2.RegistryResult(
                identifier=request.module_id,
                module_descriptor=self._create_module_descriptor(self.registered_modules[request.module_id]),
            )
        )

    def _create_setup_summary(self, setup_data: dict[str, Any]) -> registry_messages_pb2.SetupSummary:
        """Create a SetupSummary from setup data.

        Args:
            setup_data: The setup data dictionary.

        Returns:
            SetupSummary protobuf message.
        """
        return registry_messages_pb2.SetupSummary(
            id=setup_data["setup_id"],
            name=setup_data.get("name", setup_data["setup_id"]),
            documentation=setup_data.get("documentation", ""),
            status=setup_data.get("status", setup_enums_pb2.READY),
            visibility=setup_data.get("visibility", common_enums_pb2.PRIVATE),
            organization_id=setup_data.get("organization_id", ""),
            module_id=setup_data.get("module_id", ""),
            module_name=setup_data.get("module_name", ""),
            module_type=self._module_type(setup_data.get("module_type", "")),
            setup_version_id=setup_data.get("setup_version_id", ""),
            setup_version=setup_data.get("setup_version", ""),
        )

    def SearchSetups(
        self,
            request: registry_dto_pb2.SearchSetupsRequest,
        context: grpc.ServicerContext,
    ) -> registry_dto_pb2.SearchSetupsResponse:
        """Search setups based on search criteria.

        Args:
            request: The search setups request.
            context: The gRPC context.

        Returns:
            SearchSetupsResponse with one setup_summary result per match and the bulk total.
        """
        logger.debug("Mock: Searching setups with query '%s'", request.query)

        results = list(self.setups.values())

        if request.setup_ids:
            results = [s for s in results if s["setup_id"] in request.setup_ids]
        if request.module_ids:
            results = [s for s in results if s.get("module_id", "") in request.module_ids]
        if request.module_types:
            type_strings = [module_enums_pb2.ModuleType.Name(mt).lower() for mt in request.module_types]
            results = [s for s in results if s.get("module_type", "") in type_strings]
        if request.statuses:
            results = [s for s in results if s.get("status", setup_enums_pb2.READY) in request.statuses]
        if request.query:
            needle = request.query.lower()
            results = [
                s
                for s in results
                if needle in s.get("name", s["setup_id"]).lower() or needle in s.get("documentation", "").lower()
            ]

        total = len(results)
        limit = request.pagination.limit or 20
        results = results[request.pagination.offset: request.pagination.offset + limit]

        logger.debug("Mock: Found %d matching setups (returning %d)", total, len(results))
        return registry_dto_pb2.SearchSetupsResponse(
            results=[
                registry_messages_pb2.RegistryResult(
                    identifier=s["setup_id"], setup_summary=self._create_setup_summary(s)
                )
                for s in results
            ],
            bulk=bulk_pb2.BulkResponse(
                total_processed=len(results),
                pagination=pagination_pb2.PaginationResponse(total_count=total),
            ),
        )

    def GetSetup(
        self,
            request: registry_dto_pb2.GetSetupRequest,
        context: grpc.ServicerContext,
    ) -> registry_dto_pb2.GetSetupResponse:
        """Get detailed information about a specific setup.

        Args:
            request: The get setup request.
            context: The gRPC context.

        Returns:
            GetSetupResponse with the setup descriptor (its module resolved), or a NOT_FOUND error outcome.
        """
        logger.debug("Mock: Getting setup: %s", request.setup_id)

        setup = self.setups.get(request.setup_id)
        if setup is None:
            return registry_dto_pb2.GetSetupResponse(result=self._not_found(request.setup_id))

        module = self.registered_modules.get(setup.get("module_id", ""))
        return registry_dto_pb2.GetSetupResponse(
            result=registry_messages_pb2.RegistryResult(
                identifier=request.setup_id,
                setup_descriptor=registry_messages_pb2.SetupDescriptor(
                    id=setup["setup_id"],
                    name=setup.get("name", setup["setup_id"]),
                    status=setup.get("status", setup_enums_pb2.READY),
                    visibility=setup.get("visibility", common_enums_pb2.PRIVATE),
                    setup_version_id=setup.get("setup_version_id", ""),
                    setup_version=setup.get("setup_version", ""),
                    module=self._create_module_descriptor(module) if module else None,
                ),
            )
        )
