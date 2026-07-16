"""gRPC Registry client implementation.

This module provides a gRPC-based registry client that communicates with
the Service Provider's Registry service.
"""

from enum import Enum
from typing import Any

import grpc
from agentic_mesh_protocol.registry.v1 import (
    registry_enums_pb2,
    registry_models_pb2,
    registry_requests_pb2,
    registry_service_pb2_grpc,
)
from google.protobuf.internal.enum_type_wrapper import EnumTypeWrapper
from grpc_health.v1 import health_pb2, health_pb2_grpc

from digitalkin.grpc_servers.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.registry import (
    ModuleInfo,
    RegistryModuleStatus,
    RegistryModuleType,
    RegistrySetupStatus,
    RegistryVisibility,
    SetupInfo,
    SetupSummary,
)
from digitalkin.models.settings.registry import get_registry_settings
from digitalkin.services.registry.exceptions import (
    RegistryModuleNotFoundError,
    RegistryServiceError,
)
from digitalkin.services.registry.registry_models import ModuleStatusInfo
from digitalkin.services.registry.registry_strategy import RegistryStrategy


class GrpcRegistry(RegistryStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC-based registry client.

    This client communicates with the Service Provider's Registry service
    to perform module discovery, registration, and status management operations.
    """

    service_name: str = "RegistryService"

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
        self._init_channel(client_config)
        self.stub = self._get_or_create_stub(registry_service_pb2_grpc.RegistryServiceStub)
        logger.debug("Channel client 'Registry' initialized successfully")

    async def close(self) -> None:
        """Release this instance's pooled gRPC channel ref."""
        await self.close_channel()

    async def wait_for_ready(self, timeout: float = 1.0) -> bool:
        """Probe the registry via the standard gRPC Health Check service.

        Args:
            timeout: Max seconds for the round-trip.

        Returns:
            True if the server responded SERVING, False otherwise.
        """
        health_stub = health_pb2_grpc.HealthStub(self._channel)
        try:
            response = await health_stub.Check(  # type: ignore[attr-defined]  # grpc_health generated stub lacks typed Check
                health_pb2.HealthCheckRequest(service=""),
                timeout=timeout,
            )
        except grpc.aio.AioRpcError:
            return False
        return response.status == health_pb2.HealthCheckResponse.SERVING

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
            module_name=descriptor.name,
            documentation=descriptor.documentation or None,
        )

    @staticmethod
    def _proto_to_setup_info(descriptor: registry_models_pb2.SetupDescriptor) -> SetupInfo | None:
        """Convert proto SetupDescriptor to SetupInfo.

        Args:
            descriptor: Proto SetupDescriptor message.

        Returns:
            SetupInfo with mapped fields, or None if descriptor is empty.
        """
        if not descriptor.id:
            return None
        status_name = registry_enums_pb2.SetupStatus.Name(descriptor.status).removeprefix("SETUP_STATUS_")
        visibility_name = registry_enums_pb2.Visibility.Name(descriptor.visibility).removeprefix("VISIBILITY_")
        return SetupInfo(
            setup_id=descriptor.id,
            name=descriptor.name,
            documentation=descriptor.documentation or None,
            status=RegistrySetupStatus[status_name],
            visibility=RegistryVisibility[visibility_name],
            organization_id=descriptor.organization_id or None,
            owner_id=descriptor.owner_id or None,
            card_id=descriptor.card_id or None,
            module_id=descriptor.module_id or None,
            module_name=descriptor.module.name or None,
            module_type=RegistryModuleType[
                registry_enums_pb2.ModuleType.Name(descriptor.module.module_type).removeprefix("MODULE_TYPE_")
            ]
            if descriptor.HasField("module")
            else None,
            setup_version_id=descriptor.setup_version_id or None,
            setup_version=descriptor.setup_version or None,
            config=dict(descriptor.config) if descriptor.config else None,
        )

    async def discover_by_id(self, module_id: str) -> ModuleInfo:
        """Get module info by ID.

        Args:
            module_id: The module identifier.

        Returns:
            ModuleInfo with module details.

        Raises:
            RegistryModuleNotFoundError: If module not found.
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug("Discovering module by ID: %s", module_id)

        async with self.handle_grpc_errors("GetModule", RegistryServiceError):
            try:
                response = await self.exec_grpc_query(
                    "GetModule",
                    registry_requests_pb2.GetModuleRequest(module_id=module_id),
                )
            except ServerError as e:
                msg = f"Failed to discover module '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            if not response.id:
                logger.warning("Module not found in registry: %s", module_id)
                raise RegistryModuleNotFoundError(module_id)

            logger.debug("Module discovered: module_id=%s at %s:%d", response.id, response.address, response.port)
            return self._proto_to_module_info(response)

    @staticmethod
    def _module_summary_to_module_info(summary: registry_models_pb2.ModuleSummary) -> ModuleInfo:
        """Convert proto ModuleSummary to ModuleInfo (address/port are never populated).

        Args:
            summary: Proto ModuleSummary message.

        Returns:
            ModuleInfo with mapped fields.
        """
        type_name = registry_enums_pb2.ModuleType.Name(summary.module_type).removeprefix("MODULE_TYPE_")
        status_name = registry_enums_pb2.ModuleStatus.Name(summary.status).removeprefix("MODULE_STATUS_")
        return ModuleInfo(
            module_id=summary.id,
            module_type=RegistryModuleType[type_name],
            version=summary.version,
            module_name=summary.name,
            documentation=summary.documentation or None,
            status=RegistryModuleStatus[status_name],
        )

    async def search(
        self,
        name: str | None = None,
        module_type: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[ModuleInfo]:
        """Search the module catalog (module blueprints; needs a setup to be invocable).

        Args:
            name: Case-insensitive free text matched against module name AND documentation.
            module_type: Filter by type (archetype, tool_module).
            limit: Max results (1-100).
            offset: Pagination offset.

        Returns:
            List of matching modules as trimmed ModuleInfo (address/port are never
            populated by search — resolve via discover_by_id when wiring communication).

        Raises:
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug("Searching modules: name=%s type=%s", name, module_type)

        async with self.handle_grpc_errors("SearchModules", RegistryServiceError):
            module_types: list[str] = []
            if module_type:
                enum_val = RegistryModuleType[module_type.upper()]
                module_types.append(self._encode_enum(registry_enums_pb2.ModuleType, "MODULE_TYPE", enum_val))

            try:
                response = await self.exec_grpc_query(
                    "SearchModules",
                    registry_requests_pb2.SearchModulesRequest(
                        query=name or "",
                        module_types=module_types,
                        limit=limit,
                        offset=offset,
                    ),
                    # TODO(validate): tightened agent-facing search deadline (was global 30s)
                    timeout=get_registry_settings().search_timeout_s,
                )
            except ServerError as e:
                msg = f"Failed to search modules: {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            logger.debug("Search returned %d of %d modules", len(response.modules), response.total)
            return [self._module_summary_to_module_info(m) for m in response.modules]

    async def get_status(self, module_id: str) -> ModuleStatusInfo:
        """Get module status by fetching the module.

        Args:
            module_id: The module identifier.

        Returns:
            ModuleStatusInfo with current status.

        Raises:
            RegistryModuleNotFoundError: If module not found.
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug("Getting module status: %s", module_id)

        async with self.handle_grpc_errors("GetModule", RegistryServiceError):
            try:
                response = await self.exec_grpc_query(
                    "GetModule",
                    registry_requests_pb2.GetModuleRequest(module_id=module_id),
                )
            except ServerError as e:
                msg = f"Failed to get module status for '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            if not response.id:
                logger.warning("Module not found in registry: %s", module_id)
                raise RegistryModuleNotFoundError(module_id)

            status_name = registry_enums_pb2.ModuleStatus.Name(response.status).removeprefix("MODULE_STATUS_")
            logger.debug("Module status retrieved: module_id=%s status=%s", response.id, status_name)
            return ModuleStatusInfo(
                module_id=response.id,
                status=RegistryModuleStatus[status_name],
            )

    async def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
        module_type: RegistryModuleType = RegistryModuleType.UNSPECIFIED,
        documentation: str = "",
    ) -> ModuleInfo | None:
        """Register a module with the registry.

        Note: The module must already exist in the registry database; registration
        updates its address/port/version and declares its type.

        Args:
            module_id: Unique module identifier.
            address: Network address.
            port: Network port.
            version: Module version.
            module_type: Declared module type (tool or archetype/kin).
            documentation: Internal documentation for registry index search.

        Returns:
            ModuleInfo if successful, None if module not found.

        Raises:
            RegistryServiceError: If gRPC call fails.
        """
        logger.info(
            "Registering module with registry: module_id=%s at %s:%d version=%s type=%s",
            module_id,
            address,
            port,
            version,
            module_type.value,
        )

        async with self.handle_grpc_errors("RegisterModule", RegistryServiceError):
            try:
                response = await self.exec_grpc_query(
                    "RegisterModule",
                    registry_requests_pb2.RegisterModuleRequest(
                        module_id=module_id,
                        address=address,
                        port=port,
                        version=version,
                        module_type=self._encode_enum(registry_enums_pb2.ModuleType, "MODULE_TYPE", module_type),
                        documentation=documentation,
                    ),
                )
            except ServerError as e:
                msg = f"Failed to register module '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            if not response.module or not response.module.id:
                logger.warning("Registry returned empty response for module registration: module_id=%s", module_id)
                return None

            logger.info(
                "Module registered successfully: module_id=%s at %s:%d",
                response.module.id,
                response.module.address,
                response.module.port,
            )
            return self._proto_to_module_info(response.module)

    async def heartbeat(self, module_id: str) -> RegistryModuleStatus:
        """Send heartbeat to keep module active.

        Args:
            module_id: The module identifier.

        Returns:
            Current module status after heartbeat.

        Raises:
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug("Sending heartbeat: %s", module_id)

        async with self.handle_grpc_errors("Heartbeat", RegistryServiceError):
            try:
                response = await self.exec_grpc_query(
                    "Heartbeat",
                    registry_requests_pb2.HeartbeatRequest(module_id=module_id),
                )
            except ServerError as e:
                msg = f"Failed to send heartbeat for '{module_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            status_name = registry_enums_pb2.ModuleStatus.Name(response.status).removeprefix("MODULE_STATUS_")
            logger.debug("Heartbeat response: module_id=%s status=%s", module_id, status_name)
            return RegistryModuleStatus[status_name]

    async def get_setup(self, setup_id: str) -> SetupInfo | None:
        """Get setup info.

        Args:
            setup_id: The setup identifier.

        Returns:
            SetupInfo if successful, None otherwise.

        Raises:
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug("Getting setup", extra={"setup_id": setup_id})
        async with self.handle_grpc_errors("GetSetup", RegistryServiceError):
            try:
                response = await self.exec_grpc_query(
                    "GetSetup",
                    registry_requests_pb2.GetSetupRequest(setup_id=setup_id),
                )
            except ServerError as e:
                msg = f"Failed to get setup '{setup_id}': {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e
            return self._proto_to_setup_info(response)

    @staticmethod
    def _encode_enum(proto_enum: EnumTypeWrapper, prefix: str, member: Enum) -> str:
        """Encode a Python registry enum to its proto name, validated against the proto.

        Args:
            proto_enum: The proto ``EnumTypeWrapper`` (e.g. ``registry_enums_pb2.SetupStatus``).
            prefix: The proto name prefix (e.g. ``"SETUP_STATUS"``).
            member: The Python enum member to encode.

        Returns:
            The validated proto enum name.

        Raises:
            ValueError: If ``member`` has no matching proto member (Python/proto drift).
        """
        name = f"{prefix}_{member.name}"
        try:
            proto_enum.Value(name)  # fail closed: never send a filter the server would ignore
        except ValueError:
            # TODO(validate): remove marker once enum encoding is validated in prod
            logger.error("[VALIDATE ENUMENC] no proto member %s — registry filter would silently drop", name)
            raise
        return name

    @staticmethod
    def _summary_to_setup_summary(summary: registry_models_pb2.SetupSummary) -> SetupSummary:
        """Convert proto SetupSummary to the search-safe SetupSummary (never carries config).

        Args:
            summary: Proto SetupSummary message.

        Returns:
            SetupSummary with mapped fields.
        """
        status_name = registry_enums_pb2.SetupStatus.Name(summary.status).removeprefix("SETUP_STATUS_")
        visibility_name = registry_enums_pb2.Visibility.Name(summary.visibility).removeprefix("VISIBILITY_")
        type_name = registry_enums_pb2.ModuleType.Name(summary.module_type).removeprefix("MODULE_TYPE_")
        return SetupSummary(
            setup_id=summary.id,
            name=summary.name,
            documentation=summary.documentation or None,
            status=RegistrySetupStatus[status_name],
            visibility=RegistryVisibility[visibility_name],
            organization_id=summary.organization_id or None,
            module_id=summary.module_id or None,
            module_name=summary.module_name or None,
            module_type=RegistryModuleType[type_name],
            setup_version_id=summary.setup_version_id or None,
            setup_version=summary.setup_version or None,
        )

    async def search_setups(
        self,
        query: str | None = None,
        setup_ids: list[str] | None = None,
        module_ids: list[str] | None = None,
        module_types: list[RegistryModuleType] | None = None,
        statuses: list[RegistrySetupStatus] | None = None,
        visibilities: list[RegistryVisibility] | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[SetupSummary]:
        """Search the setup catalog (configured, invocable module instances).

        Args:
            query: Case-insensitive free text matched against setup name AND documentation.
            setup_ids: Restrict to these setup ids.
            module_ids: Restrict to setups backed by these modules.
            module_types: Filter by backing module type (tool_module, archetype).
            statuses: Filter by setup status. None = no filter; agent-facing callers
                should pass READY/CONFIGURATION_SUCCEEDED for invocable setups.
            visibilities: Filter by visibility.
            limit: Max results (1-100).
            offset: Pagination offset.

        Returns:
            Matching setups as ``SetupSummary`` (no ``config`` field by construction).

        Raises:
            RegistryServiceError: If gRPC call fails.
        """
        logger.debug("Searching setups: query=%s limit=%d offset=%d", query, limit, offset)

        async with self.handle_grpc_errors("SearchSetups", RegistryServiceError):
            try:
                response = await self.exec_grpc_query(
                    "SearchSetups",
                    registry_requests_pb2.SearchSetupsRequest(
                        query=query or "",
                        setup_ids=setup_ids or [],
                        module_ids=module_ids or [],
                        module_types=[
                            self._encode_enum(registry_enums_pb2.ModuleType, "MODULE_TYPE", t)
                            for t in module_types or []
                        ],
                        statuses=[
                            self._encode_enum(registry_enums_pb2.SetupStatus, "SETUP_STATUS", s) for s in statuses or []
                        ],
                        visibilities=[
                            self._encode_enum(registry_enums_pb2.Visibility, "VISIBILITY", v)
                            for v in visibilities or []
                        ],
                        limit=limit,
                        offset=offset,
                    ),
                    # TODO(validate): tightened agent-facing search deadline (was global 30s)
                    timeout=get_registry_settings().search_timeout_s,
                )
            except ServerError as e:
                msg = f"Failed to search setups: {e}"
                logger.error(msg)
                raise RegistryServiceError(msg) from e

            return [self._summary_to_setup_summary(s) for s in response.setups]

    async def deregister(  # noqa: PLR6301
        self, module_id: str
    ) -> bool:  # Protocol uses heartbeat expiration; self available for future override
        """Deregister a module from the registry.

        Note: The registry protocol uses heartbeat expiration for deregistration.
        When a module stops sending heartbeats, it becomes inactive. This method
        logs the deregistration intent for observability.

        Args:
            module_id: The module identifier to deregister.

        Returns:
            True always (heartbeat expiration handles actual deregistration).
        """
        logger.info(
            "Module deregistration initiated for module_id=%s (will become inactive via heartbeat expiration)",
            module_id,
        )
        return True
