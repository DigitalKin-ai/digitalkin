"""Shared error handling utilities for gRPC services."""

from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager
from typing import Any

from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.logger import logger


class GrpcErrorHandlerMixin:
    """Mixin class providing common gRPC error handling functionality."""

    @staticmethod
    def raise_on_error(result: Any, service_error_class: type[Exception]) -> None:
        """Raise when a ``<Domain>Result`` outcome holds an ``OperationError``.

        Args:
            result: A ``<Domain>Result`` message (``oneof outcome`` with an ``error`` branch).
            service_error_class: Exception raised with the error code and message.

        Raises:
            service_error_class: When the outcome is an ``OperationError``.
        """
        if result.WhichOneof("outcome") == "error":
            msg = f"{result.identifier}: {result.error.code} {result.error.message}"
            raise service_error_class(msg)

    @staticmethod
    def successful_results(operation: str, results: Iterable[Any]) -> list[Any]:
        """Keep the ``<Domain>Result`` items of a bulk response that hold an item.

        Every item holding an ``OperationError`` is dropped and logged with its content.

        Args:
            operation: RPC name, for the log line.
            results: The ``results`` of a list or batch response.

        Returns:
            The results whose outcome is not an ``OperationError``, in order.
        """
        kept = []
        for result in results:
            if result.WhichOneof("outcome") == "error":
                logger.warning(
                    "%s dropped result %s: %s %s",
                    operation,
                    result.identifier,
                    result.error.code,
                    result.error.message,
                )
                continue
            kept.append(result)
        return kept

    @asynccontextmanager
    async def handle_grpc_errors(  # Mixin: self available for subclass overrides # ruff: ignore[no-self-use]
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
