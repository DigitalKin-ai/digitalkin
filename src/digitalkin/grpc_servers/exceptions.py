"""Exceptions for the DigitalKin gRPC server package."""

from digitalkin.exceptions import DigitalKinError


class ServerError(DigitalKinError):
    """Base class for server-related errors."""


class ConfigurationError(ServerError):
    """Error related to server configuration."""


class ServicerError(ServerError):
    """Error related to servicer operations."""


class SecurityError(ServerError):
    """Error related to security configuration."""


class PermissionDeniedError(ServerError):
    """Remote service rejected the call with gRPC PERMISSION_DENIED."""


class ServerStateError(ServerError):
    """Error related to server state (e.g., already started, not started)."""


class ReflectionError(ServerError):
    """Error related to gRPC reflection service."""


class CircuitOpenError(Exception):
    """Raised when a call is attempted on an open circuit."""


class M2MAtCapacityError(RuntimeError):
    """Concurrency slot couldn't be acquired before timeout."""
