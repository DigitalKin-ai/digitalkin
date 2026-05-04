"""Communication service for module-to-module and consumer interactions."""

from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.communication.default_communication import DefaultCommunication
from digitalkin.services.communication.gateway_consumer import (
    ConsumerConfig,
    GatewayConsumer,
    GatewayConsumerError,
    StartStreamRejected,
    StartStreamRpcError,
)
from digitalkin.services.communication.grpc_communication import (
    GrpcCommunication,
    InvalidConsumerAddressError,
)

__all__ = [
    "CommunicationStrategy",
    "ConsumerConfig",
    "DefaultCommunication",
    "GatewayConsumer",
    "GatewayConsumerError",
    "GrpcCommunication",
    "InvalidConsumerAddressError",
    "StartStreamRejected",
    "StartStreamRpcError",
]
