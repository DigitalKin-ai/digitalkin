"""Test file for Module Registry Servicer from the client side."""

import grpc
from digitalkin_proto.digitalkin.module_registry.v2 import (
    module_registry_service_pb2,
    module_registry_service_pb2_grpc,
    registration_pb2,
)

from digitalkin.grpc_servers.registry_servicer import RegistryModule


class MockModuleRegistryServicer(module_registry_service_pb2_grpc.ModuleRegistryServiceServicer):
    """Implementation of the MockModuleRegistryServicer.

    Attributes:
        registered_modules: Dictionary mapping module_id to RegistryModule objects.
    """

    registered_modules: dict[str, RegistryModule]

    def __init__(self) -> None:
        """Initialize the registry servicer with an empty module registry."""
        self.registered_modules = {}  # TODO replace with a database

    def RegisterModule(
        self, request: registration_pb2.RegisterRequest, context: grpc.ServicerContext
    ) -> registration_pb2.RegisterResponse:
        """Mock Register a module with the registry.

        Args:
            request: The register request containing module info and address.
            context: The gRPC context for setting status codes and details.

        Returns:
            registration_pb2.RegisterResponse: A response indicating success or failure.
        """
        return registration_pb2.RegisterResponse(success=False)

    def DeregisterModule(
        self, request: registration_pb2.DeregisterRequest, context: grpc.ServicerContext
    ) -> registration_pb2.DeregisterResponse:
        """Mock Deregistera module from the registry.

        Args:
            request: The deregister request containing the module ID.
            context: The gRPC context for setting status codes and details.

        Returns:
            registration_pb2.DeregisterResponse: A response indicating success or failure.
        """
        if request.module_id == "nonexistent":
            return registration_pb2.DeregisterResponse(success=False)
        return registration_pb2.DeregisterResponse(success=True)


service_instance = MockModuleRegistryServicer()
service_name = module_registry_service_pb2.DESCRIPTOR.services_by_name["ModuleRegistryService"]
