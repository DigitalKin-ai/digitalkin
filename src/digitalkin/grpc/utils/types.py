"""Type definitions for gRPC utilities."""

from typing import Protocol, TypeVar

import grpc
from grpc import aio as grpc_aio

GrpcServer = grpc.Server | grpc_aio.Server

# Create a type variable for servicer implementations
T = TypeVar("T")


class ServiceObject(Protocol):
    """Protocol for individual services in a gRPC descriptor."""

    full_name: str


# Create a protocol for service descriptors
class ServiceDescriptor(Protocol):
    """Protocol for gRPC service descriptors."""

    services_by_name: dict[str, ServiceObject]
