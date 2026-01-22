"""Communication service for module-to-module interaction."""

from digitalkin.services.communication.communication_default import DefaultCommunication
from digitalkin.services.communication.communication_grpc import GrpcCommunication
from digitalkin.services.communication.communication_strategy import CommunicationStrategy

__all__ = ["CommunicationStrategy", "DefaultCommunication", "GrpcCommunication"]
