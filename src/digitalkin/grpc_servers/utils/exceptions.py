"""Exceptions for the DigitalKin gRPC package."""

from typing import Any

import grpc


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


class GrpcContextError(DigitalKinError):
    """Exception with full gRPC context for detailed error propagation and logging.

    This exception carries comprehensive context about gRPC operations to enable:
    - Detailed structured logging with all correlation IDs
    - Proper gRPC status code mapping in responses
    - Full traceability from client to server and back

    Attributes:
        grpc_code: The gRPC status code to return (UNAVAILABLE, DEADLINE_EXCEEDED, etc.).
        job_id: Job identifier for correlating logs across the request lifecycle.
        setup_id: Setup identifier for configuration traceability.
        setup_version_id: Setup version for configuration version tracking.
        mission_id: Mission identifier for multi-task correlation.
        service_name: The gRPC service name (e.g., "SetupService", "RegistryService").
        endpoint: The gRPC method name (e.g., "GetSetup", "StartModule").
        is_client: True if error occurred in gRPC client role, False for server role.
        original_error_type: The original exception class name before wrapping.
        original_error_message: The original exception message before wrapping.
    """

    grpc_code: grpc.StatusCode
    job_id: str | None
    setup_id: str | None
    setup_version_id: str | None
    mission_id: str | None
    service_name: str | None
    endpoint: str | None
    is_client: bool
    original_error_type: str | None
    original_error_message: str | None

    def __init__(  # All context fields needed for detailed gRPC error propagation # noqa: PLR0913, PLR0917
        self,
        message: str,
        grpc_code: grpc.StatusCode = grpc.StatusCode.UNKNOWN,
        job_id: str | None = None,
        setup_id: str | None = None,
        setup_version_id: str | None = None,
        mission_id: str | None = None,
        service_name: str | None = None,
        endpoint: str | None = None,
        is_client: bool = True,  # Boolean positional: matches gRPC client/server role convention # noqa: FBT001, FBT002
        original_error_type: str | None = None,
        original_error_message: str | None = None,
    ) -> None:
        """Initialize GrpcContextError with full context for debugging.

        Args:
            message: Human-readable error description with actionable details.
            grpc_code: gRPC status code for the response.
            job_id: Job identifier for log correlation.
            setup_id: Setup identifier for configuration traceability.
            setup_version_id: Setup version identifier.
            mission_id: Mission identifier for multi-task correlation.
            service_name: Name of the gRPC service (e.g., "SetupService").
            endpoint: Name of the gRPC method (e.g., "GetSetup").
            is_client: True if this error occurred in client role.
            original_error_type: Original exception class name.
            original_error_message: Original exception message.
        """
        self.grpc_code = grpc_code
        self.job_id = job_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id
        self.mission_id = mission_id
        self.service_name = service_name
        self.endpoint = endpoint
        self.is_client = is_client
        self.original_error_type = original_error_type
        self.original_error_message = original_error_message
        super().__init__(message)

    def to_grpc_error(self) -> tuple[grpc.StatusCode, str]:
        """Convert to tuple for gRPC context.set_code() and context.set_details().

        Returns:
            Tuple of (status_code, detailed_message) formatted for gRPC response.
            The message includes role, service, endpoint, IDs, and original error.
        """
        role = "gRPC-client" if self.is_client else "gRPC-server"

        # Build service context
        if self.service_name and self.endpoint:
            service_ctx = f"[{role}:{self.service_name}.{self.endpoint}]"
        elif self.service_name:
            service_ctx = f"[{role}:{self.service_name}]"
        else:
            service_ctx = f"[{role}]"

        # Build ID context
        id_parts = []
        if self.job_id:
            id_parts.append(f"job_id={self.job_id}")
        if self.mission_id:
            id_parts.append(f"mission_id={self.mission_id}")
        if self.setup_id:
            id_parts.append(f"setup_id={self.setup_id}")
        if self.setup_version_id:
            id_parts.append(f"setup_version_id={self.setup_version_id}")
        id_ctx = f" ({', '.join(id_parts)})" if id_parts else ""

        # Build original error context
        if self.original_error_type and self.original_error_message:
            original_ctx = f" [original: {self.original_error_type}: {self.original_error_message}]"
        elif self.original_error_type:
            original_ctx = f" [original: {self.original_error_type}]"
        else:
            original_ctx = ""

        details = f"{service_ctx}{id_ctx} {self}{original_ctx}"
        return self.grpc_code, details

    def to_log_extra(self) -> dict:
        """Generate extra dict for structured logging with full context.

        Returns:
            Dict with all context fields for logger.info(..., extra=...).
        """
        return {
            "grpc_code": self.grpc_code.name,
            "grpc_code_value": self.grpc_code.value[0],
            "job_id": self.job_id,
            "setup_id": self.setup_id,
            "setup_version_id": self.setup_version_id,
            "mission_id": self.mission_id,
            "service_name": self.service_name,
            "endpoint": self.endpoint,
            "is_client": self.is_client,
            "original_error_type": self.original_error_type,
            "original_error_message": self.original_error_message,
            "error_message": str(self),
        }


class ConnectionTimeoutError(GrpcContextError):
    """Timeout during WebSocket handshake or gRPC connection establishment.

    Raised when:
    - WebSocket handshake to SurrealDB times out ("timed out during opening handshake")
    - gRPC channel connection times out
    - Initial authentication/signin times out
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        """Initialize with DEADLINE_EXCEEDED status code."""
        super().__init__(message, grpc_code=grpc.StatusCode.DEADLINE_EXCEEDED, **kwargs)


class ConnectionUnavailableError(GrpcContextError):
    """Service or database connection is unavailable or was lost.

    Raised when:
    - WebSocket keepalive ping timeout ("sent 1011 (internal error) keepalive ping timeout")
    - Connection actively refused (service not running)
    - Connection closed unexpectedly during operation
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        """Initialize with UNAVAILABLE status code."""
        super().__init__(message, grpc_code=grpc.StatusCode.UNAVAILABLE, **kwargs)


class SetupFetchError(GrpcContextError):
    """Failed to fetch setup configuration from gRPC Setup service.

    Raised when:
    - Setup service returns error response
    - setup_id not found
    - Setup data validation fails
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        """Initialize with FAILED_PRECONDITION status code."""
        super().__init__(message, grpc_code=grpc.StatusCode.FAILED_PRECONDITION, **kwargs)


class HeartbeatFailureError(GrpcContextError):
    """SurrealDB heartbeat operation failed.

    Raised when:
    - Initial heartbeat CREATE fails
    - Heartbeat MERGE/UPDATE fails
    - SurrealDB returns error code in response
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        """Initialize with UNAVAILABLE status code."""
        super().__init__(message, grpc_code=grpc.StatusCode.UNAVAILABLE, **kwargs)


class ValidationFailedError(GrpcContextError):
    """Request validation failed due to invalid input data.

    Raised when:
    - Pydantic model validation fails
    - Required fields are missing
    - Field values are out of valid range
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        """Initialize with INVALID_ARGUMENT status code."""
        super().__init__(message, grpc_code=grpc.StatusCode.INVALID_ARGUMENT, **kwargs)
