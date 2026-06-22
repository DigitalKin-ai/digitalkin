"""Shared error handling utilities for gRPC services."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.logger import logger


class GrpcErrorHandlerMixin:
    """Mixin class providing common gRPC error handling functionality."""

    @asynccontextmanager
    async def handle_grpc_errors(  # Mixin: self available for subclass overrides # noqa: PLR6301
        self,
        operation: str,
        service_error_class: type[Exception] | None = None,
    ) -> AsyncGenerator[Any, Any]:
        """Handle gRPC errors for the given operation.

        Args:
            operation: Name of the operation being performed.
            service_error_class: Optional specific service exception class to raise.
                                If not provided, uses the generic ServerError.

        Yields:
            Context for the operation.

        Raises:
            PermissionDeniedError: Re-raised as-is so the authz status is never masked.
            ServerError: For gRPC-related errors.
            service_error_class: For service-specific errors if provided.
        """
        if service_error_class is None:
            service_error_class = ServerError

        try:
            yield
        except PermissionDeniedError:
            raise
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
