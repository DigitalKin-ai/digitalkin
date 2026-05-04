"""Tests for GrpcErrorHandlerMixin — shared gRPC error handling.

Covers pass-through, service-specific errors, ServerError wrapping,
and unexpected exception conversion.
"""

import pytest

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin

pytestmark = pytest.mark.timeout(5)


class _TestHandler(GrpcErrorHandlerMixin):
    """Concrete subclass for testing the mixin."""


class CustomServiceError(Exception):
    """Test-specific service error."""


class TestGrpcErrorHandlerSmoke:
    """Basic error handling paths."""

    @pytest.mark.smoke
    async def test_no_error_passes_through(self) -> None:
        """Context manager yields without error when body succeeds."""
        handler = _TestHandler()
        result = None

        async with handler.handle_grpc_errors("test_op"):
            result = "ok"

        assert result == "ok"

    @pytest.mark.smoke
    async def test_server_error_logged_and_reraised(self) -> None:
        """ServerError is caught, logged, and re-raised as ServerError."""
        handler = _TestHandler()

        with pytest.raises(ServerError, match="ServerError in test_op"):
            async with handler.handle_grpc_errors("test_op"):
                raise ServerError("connection refused")


class TestGrpcErrorHandlerEdgeCases:
    """Edge cases and custom error classes."""

    @pytest.mark.edge_case
    async def test_service_specific_error_reraised(self) -> None:
        """When service_error_class is provided, matching errors use that class."""
        handler = _TestHandler()

        with pytest.raises(CustomServiceError, match="CustomServiceError in test_op"):
            async with handler.handle_grpc_errors("test_op", CustomServiceError):
                raise CustomServiceError("custom failure")

    @pytest.mark.edge_case
    async def test_unexpected_error_converted_to_service_error(self) -> None:
        """Unexpected exceptions are wrapped in service_error_class."""
        handler = _TestHandler()

        with pytest.raises(CustomServiceError, match="Unexpected error in test_op"):
            async with handler.handle_grpc_errors("test_op", CustomServiceError):
                raise ValueError("something broke")

    @pytest.mark.edge_case
    async def test_unexpected_error_defaults_to_server_error(self) -> None:
        """Without service_error_class, unexpected errors become ServerError."""
        handler = _TestHandler()

        with pytest.raises(ServerError, match="Unexpected error in test_op"):
            async with handler.handle_grpc_errors("test_op"):
                raise RuntimeError("runtime failure")

    @pytest.mark.edge_case
    async def test_cancelled_error_not_caught(self) -> None:
        """CancelledError propagates without being wrapped."""
        import asyncio

        handler = _TestHandler()

        with pytest.raises(asyncio.CancelledError):
            async with handler.handle_grpc_errors("test_op"):
                raise asyncio.CancelledError
