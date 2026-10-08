"""Tests for GrpcErrorHandlerMixin — shared gRPC error handling.

Covers pass-through, service-specific errors, ServerError wrapping,
and unexpected exception conversion.
"""

import logging

import pytest
from agentic_mesh_protocol.pagination.v1 import bulk_pb2
from agentic_mesh_protocol.setup.v1 import setup_messages_pb2

from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin

pytestmark = pytest.mark.timeout(5)


class _TestHandler(GrpcErrorHandlerMixin):
    """Concrete subclass for testing the mixin."""

    async def raise_inside(self, error: BaseException, service_error_class: type[Exception] | None = None) -> None:
        """Raise ``error`` inside ``handle_grpc_errors("test_op", service_error_class)``."""
        async with self.handle_grpc_errors("test_op", service_error_class):
            raise error


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
            await handler.raise_inside(ServerError("connection refused"))


class TestGrpcErrorHandlerEdgeCases:
    """Edge cases and custom error classes."""

    @pytest.mark.edge_case
    async def test_service_specific_error_reraised(self) -> None:
        """When service_error_class is provided, matching errors use that class."""
        handler = _TestHandler()

        with pytest.raises(CustomServiceError, match="CustomServiceError in test_op"):
            await handler.raise_inside(CustomServiceError("custom failure"), CustomServiceError)

    @pytest.mark.edge_case
    async def test_unexpected_error_converted_to_service_error(self) -> None:
        """Unexpected exceptions are wrapped in service_error_class."""
        handler = _TestHandler()

        with pytest.raises(CustomServiceError, match="Unexpected error in test_op"):
            await handler.raise_inside(ValueError("something broke"), CustomServiceError)

    @pytest.mark.edge_case
    async def test_unexpected_error_defaults_to_server_error(self) -> None:
        """Without service_error_class, unexpected errors become ServerError."""
        handler = _TestHandler()

        with pytest.raises(ServerError, match="Unexpected error in test_op"):
            await handler.raise_inside(RuntimeError("runtime failure"))

    @pytest.mark.edge_case
    async def test_permission_denied_preserved(self) -> None:
        """PermissionDeniedError is re-raised as-is, never re-wrapped into the service error class."""
        handler = _TestHandler()

        with pytest.raises(PermissionDeniedError, match="denied"):
            await handler.raise_inside(PermissionDeniedError("denied"), CustomServiceError)

    @pytest.mark.edge_case
    async def test_cancelled_error_not_caught(self) -> None:
        """CancelledError propagates without being wrapped."""
        import asyncio

        handler = _TestHandler()

        with pytest.raises(asyncio.CancelledError):
            async with handler.handle_grpc_errors("test_op"):
                raise asyncio.CancelledError


class TestResultOutcomes:
    """``<Domain>Result`` outcome handling: an ``OperationError`` raises or is dropped."""

    @pytest.mark.unit
    def test_raise_on_error_raises_with_code_and_message(self) -> None:
        """An error outcome raises the service error carrying identifier, code and message."""
        result = setup_messages_pb2.SetupResult(
            identifier="setups:abc",
            error=bulk_pb2.OperationError(code="NOT_FOUND", message="no such setup"),
        )

        with pytest.raises(CustomServiceError, match="setups:abc: NOT_FOUND no such setup"):
            _TestHandler.raise_on_error(result, CustomServiceError)

    @pytest.mark.unit
    def test_raise_on_error_passes_an_item_outcome(self) -> None:
        """An item outcome does not raise."""
        result = setup_messages_pb2.SetupResult(
            identifier="setups:abc", setup=setup_messages_pb2.Setup(id="setups:abc")
        )

        _TestHandler.raise_on_error(result, CustomServiceError)

    @pytest.mark.unit
    def test_successful_results_drops_and_logs_each_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """Error items are dropped one by one, each logged with its content; order is kept."""
        ok_1 = setup_messages_pb2.SetupResult(identifier="setups:a", setup=setup_messages_pb2.Setup(id="setups:a"))
        failed = setup_messages_pb2.SetupResult(
            identifier="setups:b",
            error=bulk_pb2.OperationError(code="PERMISSION_DENIED", message="not yours"),
        )
        ok_2 = setup_messages_pb2.SetupResult(identifier="setups:c", setup=setup_messages_pb2.Setup(id="setups:c"))

        with caplog.at_level(logging.WARNING):
            kept = _TestHandler.successful_results("ListSetups", [ok_1, failed, ok_2])

        assert kept == [ok_1, ok_2]
        assert "ListSetups dropped result setups:b: PERMISSION_DENIED not yours" in caplog.text
