"""Exceptions for the DigitalKin gRPC package."""


class DigitalKinError(Exception):
    """Base exception for all DigitalKin errors."""


class ServerError(DigitalKinError):
    """Base class for server-related errors."""


class ConfigurationError(ServerError):
    """Error related to server configuration."""


class ServicerError(ServerError):
    """Error related to servicer operations."""


class SecurityError(ServerError):
    """Error related to security configuration."""


class ServerStateError(ServerError):
    """Error related to server state (e.g., already started, not started)."""


class ReflectionError(ServerError):
    """Error related to gRPC reflection service."""


class HealthCheckError(ServerError):
    """Error related to gRPC health check service."""
