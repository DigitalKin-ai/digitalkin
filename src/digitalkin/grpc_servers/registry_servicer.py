"""Registry servicer implementation for DigitalKin.

This module provides the gRPC service implementation for the Module Registry,
which handles registration, deregistration, discovery, and status management
of DigitalKin modules.
"""

import logging
from collections.abc import Iterator
from enum import Enum

import grpc
from digitalkin_proto.digitalkin.module_registry.v2 import (
    discover_pb2,
    metadata_pb2,
    module_registry_service_pb2_grpc,
    registration_pb2,
    status_pb2,
)
from pydantic import BaseModel
from typing_extensions import Self

logger = logging.getLogger(__name__)


class ExtendedEnum(Enum):
    """Tool enum class."""

    @classmethod
    def list(cls) -> list:
        """Classmethod to generate a list of enum values.

        Returns:
            list: Enum members' values
        """
        return [c.value for c in cls]


class ModuleStatus(ExtendedEnum):
    """Describe a Module current status.

    Represents the possible states a module can be in during its lifecycle.
    """

    # RUNNING: Module alive.
    RUNNING = 0
    # IDLE: Module waiting for an event / update.
    IDLE = 1
    # ENDED: Module signals the end of task or have been killed.
    ENDED = 2


class Tag(BaseModel):
    """Words representing a module capabilities.

    Used for module discovery and categorization.
    """

    # tag: Describe a Module function.
    tag: str

    def to_proto(self) -> metadata_pb2.Tag:
        """Convert Tag object from Pydantic to Proto.

        Returns:
            metadata_pb2.Tag: The protobuf representation of this tag.
        """
        return metadata_pb2.Tag(tag=self.tag)


class Metadata(BaseModel):
    """Different informations to index and describe a module.

    Contains human-readable information about a module and its capabilities.
    """

    # name: Module's name
    name: str
    # tags: List of tag to describe a module functionalities.
    tags: list[Tag]
    # description: Module's description for search and indexing
    description: str | None

    def to_proto(self) -> metadata_pb2.Metadata:
        """Convert Metadata object from Pydantic to Proto.

        Returns:
            metadata_pb2.Metadata: The protobuf representation of this metadata.
        """
        return metadata_pb2.Metadata(
            name=self.name,
            tags=(t.to_proto() for t in self.tags),
            description=self.description,
        )

    @classmethod
    def from_proto(cls, request_metadata: metadata_pb2.Metadata) -> Self:
        """Create Metadata object from Proto message.

        Args:
            request_metadata: The protobuf metadata to convert.

        Returns:
            Metadata: The Pydantic model representation of the metadata.
        """
        return cls(
            name=request_metadata.name,
            tags=[Tag(tag=t.tag) for t in request_metadata.tags],
            description=request_metadata.description,
        )


class RegistryModule(BaseModel):
    """Module's technical representation to index, search and monitor.

    Contains all the information needed to identify, locate and communicate
    with a module in the system.
    """

    # module_id: Id of the module
    module_id: str
    # module_type: Type of the module (trigger, tool, kin, view)
    module_type: str
    # address: Address used to communicate with the module
    address: str
    # port: Port used to communicate with the module
    port: int
    # version: Current module version.
    version: str
    # metadata: user defined module name, description and tags
    metadata: Metadata
    # status: Representation of the Module current state (running, idle, ended...).
    status: ModuleStatus
    # message: (Optional) Details about the status.
    message: str | None

    def to_proto(self) -> discover_pb2.DiscoverInfoResponse:
        """Convert RegistryModule object from Pydantic to Proto.

        Returns:
            metadata_pb2.Metadata: The protobuf representation of this metadata.
        """
        return discover_pb2.DiscoverInfoResponse(
            module_id=self.module_id,
            module_type=self.module_type,
            address=self.address,
            port=self.port,
            version=self.version,
            metadata=self.metadata.to_proto(),
        )


class RegistryServicer(module_registry_service_pb2_grpc.ModuleRegistryServiceServicer):
    """Implementation of the ModuleRegistryService.

    This servicer handles the registration, deregistration, and discovery of modules.
    It maintains an in-memory registry of all active modules and their metadata.

    Attributes:
        registered_modules: Dictionary mapping module_id to RegistryModule objects.
    """

    registered_modules: dict[str, RegistryModule]

    def __init__(self) -> None:
        """Initialize the registry servicer with an empty module registry."""
        self.registered_modules = {}  # TODO replace with a database

    def RegisterModule(  # noqa: N802
        self,
        request: registration_pb2.RegisterRequest,
        context: grpc.ServicerContext,
    ) -> registration_pb2.RegisterResponse:
        """Register a module with the registry.

        Adds a new module to the registry with its connection information and metadata.
        Fails if a module with the same ID is already registered.

        Args:
            request: The register request containing module info and address.
            context: The gRPC context for setting status codes and details.

        Returns:
            registration_pb2.RegisterResponse: A response indicating success or failure.
        """
        module_id = request.module_id
        logger.info("Registering module: %s", module_id)

        # Check if module is already registered
        if module_id in self.registered_modules:
            message = f"Module '{module_id}' already registered"
            logger.warning(message)

            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(message)
            return registration_pb2.RegisterResponse(success=False)

        # Store the module info with address
        self.registered_modules[module_id] = RegistryModule(
            module_id=request.module_id,
            module_type=request.module_type,
            address=request.address,
            port=request.port,
            version=request.version,
            metadata=Metadata.from_proto(request.metadata),
            status=ModuleStatus.RUNNING,
            message=None,
        )

        logger.info("Module %s registered at %s:%d", module_id, request.address, request.port)
        return registration_pb2.RegisterResponse(success=True)

    def DeregisterModule(  # noqa: N802
        self,
        request: registration_pb2.DeregisterRequest,
        context: grpc.ServicerContext,
    ) -> registration_pb2.DeregisterResponse:
        """Deregister a module from the registry.

        Removes a module from the registry based on its ID.
        Fails if the specified module is not found in the registry.

        Args:
            request: The deregister request containing the module ID.
            context: The gRPC context for setting status codes and details.

        Returns:
            registration_pb2.DeregisterResponse: A response indicating success or failure.
        """
        module_id = request.module_id
        logger.info("Deregistering module: %s", module_id)

        # Check if module exists in registry
        if module_id not in self.registered_modules:
            message = f"Module {module_id} not found in registry"
            logger.warning(message)

            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return registration_pb2.DeregisterResponse()

        # Remove the module
        del self.registered_modules[module_id]

        logger.info("Module %s deregistered", module_id)
        return registration_pb2.DeregisterResponse(success=True)

    def DiscoverInfoModule(  # noqa: N802
        self,
        request: discover_pb2.DiscoverInfoRequest,
        context: grpc.ServicerContext,
    ) -> discover_pb2.DiscoverInfoResponse:
        """Discover detailed information about a specific module.

        Retrieves complete information about a module based on its ID.

        Args:
            request: The discover request containing the module ID.
            context: The gRPC context (unused).

        Returns:
            discover_pb2.DiscoverInfoResponse: A response containing the module's information.
        """
        logger.info("Discovering module: %s", request.module_id)

        # Check if module exists in registry
        if request.module_id not in self.registered_modules:
            message = f"Module {request.module_id} not found in registry"
            logger.warning(message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return discover_pb2.DiscoverInfoResponse()
        return self.registered_modules[request.module_id].to_proto()

    def DiscoverSearchModule(  # noqa: N802
        self,
        request: discover_pb2.DiscoverSearchRequest,
        context: grpc.ServicerContext,  # noqa: ARG002
    ) -> discover_pb2.DiscoverSearchResponse:
        """Discover modules based on the specified criteria.

        Searches for modules that match the provided filters such as name,
        type, and tags.

        Args:
            request: The discover request containing search criteria.
            context: The gRPC context (unused).

        Returns:
            discover_pb2.DiscoverSearchResponse: A response containing matching modules.
        """
        logger.info("Discovering modules with criteria:")

        # Start with all modules
        results = list(self.registered_modules.values())
        logger.info("%s", list(results))
        # Filter by name if specified
        if request.name:
            logger.info("\tname %s", request.name)
            results = [m for m in results if request.name in m.metadata.name]

        # Filter by type if specified
        if request.module_type:
            logger.info("\tmodule_type %s", request.module_type)
            results = [m for m in results if m.module_type == request.module_type]

        # Filter by tags if specified
        if request.tags:
            logger.info("\ttags %s", request.tags)
            results = [m for m in results if any(tag in m.metadata.tags for tag in request.tags)]

        # Filter by description if specified
        """
        if request.description:
            results = [m for m in results if request.description in m.metadata.description]
        """

        logger.info("Found %d matching modules", len(results))
        return discover_pb2.DiscoverSearchResponse(modules=[r.to_proto() for r in results])

    def GetModuleStatus(  # noqa: N802
        self,
        request: status_pb2.ModuleStatusRequest,
        context: grpc.ServicerContext,
    ) -> status_pb2.ModuleStatusResponse:
        """Query a specific module's status.

        Retrieves the current status of a module based on its ID.

        Args:
            request: The status request containing the module ID.
            context: The gRPC context (unused).

        Returns:
            status_pb2.ModuleStatusResponse: A response containing the module's status.
        """
        logger.info("Getting status for module: %s", request.module_id)

        # Check if module exists in registry
        if request.module_id not in self.registered_modules:
            message = f"Module {request.module_id} not found in registry"
            logger.warning(message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return status_pb2.ModuleStatusResponse()

        module = self.registered_modules[request.module_id]
        return status_pb2.ModuleStatusResponse(module_id=module.module_id, status=module.status.name)

    def ListModuleStatus(  # noqa: N802
        self,
        request: status_pb2.ListModulesStatusRequest,
        context: grpc.ServicerContext,
    ) -> status_pb2.ListModulesStatusResponse:
        """Get a paginated list of registered modules and their statuses.

        Returns a subset of registered modules based on pagination parameters.

        Args:
            request: The request containing offset and list_size for pagination.
            context: The gRPC context (unused).

        Returns:
            status_pb2.ListModulesStatusResponse: A response containing a list of module statuses.
        """
        logger.info(
            "Getting registered modules with offset %d and limit %d",
            request.offset,
            request.list_size,
        )
        if request.offset > len(self.registered_modules):
            message = f"Out of range {request.offset}"
            logger.warning(message)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(message)
            return status_pb2.ListModulesStatusResponse()

        list_size = request.list_size
        if list_size == 0:
            list_size = len(self.registered_modules)

        modules_statuses = [
            status_pb2.ModuleStatusResponse(module_id=module.module_id, status=module.status.name)
            for module in list(self.registered_modules.values())[request.offset : request.offset + list_size]
        ]

        logger.info("Found %d registered modules", len(modules_statuses))
        return status_pb2.ListModulesStatusResponse(
            list_size=len(modules_statuses),
            modules_statuses=modules_statuses,
        )

    def GetAllModuleStatus(  # noqa: N802
        self,
        request: status_pb2.GetAllModulesStatusRequest,  # noqa: ARG002
        context: grpc.ServicerContext,  # noqa: ARG002
    ) -> Iterator[status_pb2.ModuleStatusResponse]:
        """Get all registered modules via a stream.

        Streams the status of all registered modules one by one.

        Args:
            request: The get all modules request (unused).
            context: The gRPC context (unused).

        Yields:
            status_pb2.ModuleStatusResponse: Responses containing individual module statuses.
        """
        logger.info("Streaming all %d registered modules", len(self.registered_modules))
        for module in self.registered_modules.values():
            yield status_pb2.ModuleStatusResponse(
                module_id=module.module_id,
                status=module.status.name,
            )

    def UpdateModuleStatus(  # noqa: N802
        self,
        request: status_pb2.UpdateStatusRequest,
        context: grpc.ServicerContext,
    ) -> status_pb2.UpdateStatusResponse:
        """Update the status of a registered module.

        Changes the current status of a module based on the provided request.
        Fails if the specified module is not found in the registry.

        Args:
            request: The update status request with module ID and new status.
            context: The gRPC context (unused).

        Returns:
            status_pb2.UpdateStatusResponse: A response indicating success or failure.
        """
        module_id = request.module_id
        logger.info("Updating status for module: %s to %s", module_id, request.status)

        # Check if module exists in registry
        if request.module_id not in self.registered_modules:
            message = f"Module {request.module_id} not found in registry"
            logger.warning(message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return status_pb2.UpdateStatusResponse()

        # Check if module status is correct
        if request.status not in ModuleStatus.list() or request.status is None:
            message = f"ModuleStatus {request.status} is unknonw, please check the requested status"
            logger.warning(message)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(message)
            return status_pb2.UpdateStatusResponse(success=False)

        # Update module status
        module_info = self.registered_modules[module_id]
        module_info.status = ModuleStatus(request.status)

        logger.info("Status for module %s updated to %s", module_id, request.status)
        return status_pb2.UpdateStatusResponse(success=True)
