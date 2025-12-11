"""Communication service for module-to-module interaction."""

from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.communication.default_communication import DefaultCommunication
from digitalkin.services.communication.grpc_communication import GrpcCommunication

__all__ = ["CommunicationStrategy", "DefaultCommunication", "GrpcCommunication"]
