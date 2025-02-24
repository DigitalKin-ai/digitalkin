"""Registry servicer implementation for DigitalKin."""

import logging

import grpc
from digitalkin_proto.digitalkin.module_registry.v1 import (
    action_pb2,
    module_registry_service_pb2_grpc,
    monitoring_pb2,
    registration_pb2,
)

logger = logging.getLogger(__name__)


class RegistryServicer(module_registry_service_pb2_grpc.ModuleRegistryServiceServicer):
    """Implementation of the ModuleRegistryService.

    This servicer handles the registration, deregistration, and discovery of modules.

    Attributes:
        registered_modules: Dictionary of module_id to ModuleInfo.
    """

    def __init__(self):
        """Initialize the registry servicer."""
        self.registered_modules: dict[str, dict] = {}  # TODO replace with a database

    def RegisterModule(  # noqa: N802
        self,
        request: registration_pb2.RegisterRequest,
        context: grpc.ServicerContext,
    ) -> registration_pb2.RegisterResponse:
        """Register a module with the registry.

        Args:
            request: The register request containing module info and address.
            context: The gRPC context.

        Returns:
            A response indicating success or failure.
        """
        module_id = request.module_id
        logger.info(f"Registering module: {module_id}")

        if module_id in self.registered_modules:
            message = f"Module {module_id} already registered"
            logger.warning(message)
            return registration_pb2.RegisterResponse(
                success=False,
                error_message=message,
            )

        # Store the module info with address
        module_with_address = {
            "module_id": request.module_id,
            "name": request.name,
            "description": request.description,
            "type": request.type,
            "capabilities": request.capabilities,
            "tags": request.tags,
            "address": request.address,
        }
        self.registered_modules[module_id] = module_with_address

        logger.info(f"Module {module_id} registered at {request.address}")
        return registration_pb2.RegisterResponse(
            success=True,
            message=f"Module {module_id} registered successfully",
        )

    def DeregisterModule(  # noqa: N802
        self,
        request: registration_pb2.DeregisterRequest,
        context: grpc.ServicerContext,
    ) -> registration_pb2.DeregisterResponse:
        """Deregister a module from the registry.

        Args:
            request: The deregister request containing the module ID.
            context: The gRPC context.

        Returns:
            A response indicating success or failure.
        """
        module_id = request.module_id
        logger.info(f"Deregistering module: {module_id}")

        if module_id not in self.registered_modules:
            message = f"Module {module_id} not found in registry"
            logger.warning(message)
            return registration_pb2.DeregisterResponse(
                success=False,
                error_message=message,
            )

        # Remove the module
        del self.registered_modules[module_id]

        logger.info(f"Module {module_id} deregistered")
        return registration_pb2.DeregisterResponse(
            success=True,
            message=f"Module {module_id} deregistered successfully",
        )

    def DiscoverModule(  # noqa: N802
        self,
        request: action_pb2.DiscoverRequest,
        context: grpc.ServicerContext,
    ) -> action_pb2.DiscoverResponse:
        """Discover modules based on the specified criteria.

        Args:
            request: The discover request containing search criteria.
            context: The gRPC context.

        Returns:
            A response containing matching modules.
        """
        logger.info(f"Discovering modules with criteria: {request}")

        # Start with all modules
        results = list(self.registered_modules.values())

        # Filter by module_id if specified
        if request.module_id:
            results = [m for m in results if m["module_id"] == request.module_id]

        # Filter by name if specified
        if request.name:
            results = [m for m in results if request.name in m["name"]]

        # Filter by type if specified
        if request.type:
            results = [m for m in results if m["type"] == request.type]

        # Filter by capabilities if specified
        if request.capabilities:
            results = [m for m in results if all(cap in m["capabilities"] for cap in request.capabilities)]

        # Filter by tags if specified
        if request.tags:
            results = [m for m in results if all(tag in m["tags"] for tag in request.tags)]

        logger.info(f"Found {len(results)} matching modules")
        return action_pb2.DiscoverResponse(
            modules=results,
        )

    def UpdateModuleStatus(  # noqa: N802
        self,
        request: action_pb2.UpdateStatusRequest,
        context: grpc.ServicerContext,
    ) -> action_pb2.UpdateStatusResponse:
        """Update the status of a registered module.

        Args:
            request: The update status request.
            context: The gRPC context.

        Returns:
            A response indicating success or failure.
        """
        module_id = request.module_id
        logger.info(f"Updating status for module: {module_id}")

        if module_id not in self.registered_modules:
            message = f"Module {module_id} not found in registry"
            logger.warning(message)
            return action_pb2.UpdateStatusResponse(
                success=False,
                error_message=message,
            )

        # Update the status
        module_info = self.registered_modules[module_id]
        module_with_status = {**module_info, "status": request.status}
        self.registered_modules[module_id] = module_with_status

        logger.info(f"Status for module {module_id} updated to {request.status}")
        return action_pb2.UpdateStatusResponse(
            success=True,
            message=f"Module {module_id} status updated successfully",
        )

    def GetAllModules(  # noqa: N802
        self,
        request: monitoring_pb2.GetAllModulesRequest,
        context: grpc.ServicerContext,
    ) -> monitoring_pb2.GetAllModulesResponse:
        """Get all registered modules.

        Args:
            request: The get all modules request.
            context: The gRPC context.

        Returns:
            A response containing all registered modules.
        """
        logger.info("Getting all registered modules")

        modules = list(self.registered_modules.values())

        logger.info(f"Found {len(modules)} registered modules")
        return monitoring_pb2.GetAllModulesResponse(
            modules=modules,
        )
