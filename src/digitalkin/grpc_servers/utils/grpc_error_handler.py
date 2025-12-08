"""Shared error handling utilities for gRPC services."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.logger import logger


class GrpcErrorHandlerMixin:
    """Mixin class providing common gRPC error handling functionality."""

    @contextmanager
    def handle_grpc_errors(  # noqa: PLR6301
        self,
        operation: str,
        service_error_class: type[Exception] | None = None,
    ) -> Generator[Any, Any, Any]:
        """Handle gRPC errors for the given operation.

        Args:
            operation: Name of the operation being performed.
            service_error_class: Optional specific service exception class to raise.
                                If not provided, uses the generic ServerError.

        Yields:
            Context for the operation.

        Raises:
            ServerError: For gRPC-related errors.
            service_error_class: For service-specific errors if provided.
        """
        if service_error_class is None:
            service_error_class = ServerError

        try:
            yield
        except service_error_class as e:
            # Re-raise service-specific errors as-is
            msg = f"{service_error_class.__name__} in {operation}: {e}"
            logger.exception(msg)
            raise service_error_class(msg) from e
        except ServerError as e:
            # Handle gRPC server errors
            msg = f"gRPC {operation} failed: {e}"
            logger.exception(msg)
            raise ServerError(msg) from e
        except Exception as e:
            # Handle unexpected errors
            msg = f"Unexpected error in {operation}: {e}"
            logger.exception(msg)
            raise service_error_class(msg) from e
