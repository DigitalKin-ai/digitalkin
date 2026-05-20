"""Communication service for module-to-module and consumer interactions."""

from digitalkin.grpc_servers.exceptions import M2MAtCapacityError
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.communication.default_communication import DefaultCommunication
from digitalkin.services.communication.exceptions import (
    InvalidConsumerAddressError,
    M2MCallTimeout,
    M2MTargetUnavailable,
)
from digitalkin.services.communication.grpc_communication import GrpcCommunication

__all__ = [
    "CommunicationStrategy",
    "DefaultCommunication",
    "GrpcCommunication",
    "InvalidConsumerAddressError",
    "M2MAtCapacityError",
    "M2MCallTimeout",
    "M2MTargetUnavailable",
]
