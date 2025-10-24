"""Tests for the BaseServer implementation."""

import sys
from typing import NoReturn
from unittest import mock

import grpc
import pytest
from grpc import aio as grpc_aio

from digitalkin.grpc_servers._base_server import BaseServer
from digitalkin.grpc_servers.utils.exceptions import (
    SecurityError,
    ServerStateError,
    ServicerError,
)


# Create a concrete implementation of BaseServer for testing
class MockServer(BaseServer):
    """A concrete implementation of BaseServer for testing."""

    def _register_servicers(self) -> None:
        """Register test servicers with the gRPC server."""
        if self.server is None:
            msg = "Server must be created before registering servicers"
            raise ServicerError(msg)

        # For tests, we just need the method to be implemented
        # Actual servicer registration happens in tests


# Basic initialization tests
def test_base_server_init(server_config_sync_insecure) -> None:
    """Test initialization of BaseServer."""
    server = MockServer(server_config_sync_insecure)

    if server.config != server_config_sync_insecure:
        pytest.fail(f"Expected config to be {server_config_sync_insecure}, got {server.config}")

    if server.server is not None:
        pytest.fail(f"Expected server to be None, got {server.server}")

    if server._servicers != []:
        pytest.fail(f"Expected _servicers to be empty list, got {server._servicers}")

    if server._service_names != []:
        pytest.fail(f"Expected _service_names to be empty list, got {server._service_names}")

    if server._health_servicer is not None:
        pytest.fail(f"Expected _health_servicer to be None, got {server._health_servicer}")


# Servicer registration tests
def test_register_servicer_without_server(server_config_sync_insecure) -> None:
    """Test that registering a servicer before creating the server raises an error."""
    server = MockServer(server_config_sync_insecure)

    # Should raise ServicerError since server is not created
    with pytest.raises(ServicerError):
        server.register_servicer(
            servicer=mock.MagicMock(),
            add_to_server_fn=mock.MagicMock(),
        )


def test_register_servicer(server_config_sync_insecure) -> None:
    """Test registering a servicer with the server."""
    server = MockServer(server_config_sync_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)
    server.server = mock_grpc_server

    # Create a mock servicer and add_to_server function
    mock_servicer = mock.MagicMock()
    mock_add_fn = mock.MagicMock()

    # Register the servicer
    server.register_servicer(
        servicer=mock_servicer,
        add_to_server_fn=mock_add_fn,
    )

    # Verify the servicer was added correctly
    mock_add_fn.assert_called_once_with(mock_servicer, mock_grpc_server)

    if server._servicers != [mock_servicer]:
        pytest.fail(f"Expected _servicers to be [{mock_servicer}], got {server._servicers}")


def test_register_servicer_with_explicit_names(server_config_sync_insecure) -> None:
    """Test registering a servicer with explicit service names."""
    server = MockServer(server_config_sync_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)
    server.server = mock_grpc_server

    # Register the servicer with explicit service names
    server.register_servicer(
        servicer=mock.MagicMock(),
        add_to_server_fn=mock.MagicMock(),
        service_names=["my.test.Service"],
    )

    # Verify the service name was added
    if "my.test.Service" not in server._service_names:
        pytest.fail(f"Expected 'my.test.Service' to be in _service_names, got {server._service_names}")


def test_register_servicer_with_descriptor(server_config_sync_insecure) -> None:
    """Test registering a servicer with a service descriptor."""
    server = MockServer(server_config_sync_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)
    server.server = mock_grpc_server

    # Create a mock descriptor
    mock_descriptor = mock.MagicMock()
    mock_service = mock.MagicMock()
    mock_service.full_name = "my.test.DescriptorService"
    mock_descriptor.services_by_name = {"Service": mock_service}

    # Register the servicer with the descriptor
    server.register_servicer(
        servicer=mock.MagicMock(),
        add_to_server_fn=mock.MagicMock(),
        service_descriptor=mock_descriptor,
    )

    # Verify the service name was added from the descriptor
    if "my.test.DescriptorService" not in server._service_names:
        pytest.fail(f"Expected 'my.test.DescriptorService' to be in _service_names, got {server._service_names}")


def test_register_servicer_failure(server_config_sync_insecure) -> None:
    """Test handling of registration failure."""
    server = MockServer(server_config_sync_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)
    server.server = mock_grpc_server

    # Create a function that raises an exception
    def add_fn_error(servicer, server) -> NoReturn:
        msg = "Registration error"
        raise RuntimeError(msg)

    # Attempt to register with the failing function
    with pytest.raises(ServicerError):
        server.register_servicer(
            servicer=mock.MagicMock(),
            add_to_server_fn=add_fn_error,
        )


# Reflection tests
def test_add_reflection(server_config_sync_insecure) -> None:
    """Test adding reflection service to the server."""
    # First, clear any existing imports of the module
    if "grpc_reflection.v1alpha.reflection" in sys.modules:
        del sys.modules["grpc_reflection.v1alpha.reflection"]

    # Create a mock module with the required attributes
    mock_reflection = mock.MagicMock()
    mock_reflection.SERVICE_NAME = "grpc.reflection.v1alpha.ServerReflection"

    # Directly patch the module in sys.modules
    with mock.patch.dict("sys.modules", {"grpc_reflection.v1alpha.reflection": mock_reflection}):
        # Create the server
        server = MockServer(server_config_sync_insecure)

        # Create a mock server
        mock_grpc_server = mock.MagicMock(spec=grpc.Server)
        server.server = mock_grpc_server

        # Add a service name
        server._service_names = ["my.test.Service"]

        # Call add_reflection
        server._add_reflection()

        # Verify the function was called
        mock_reflection.enable_server_reflection.assert_called_once_with(
            ["my.test.Service", "grpc.reflection.v1alpha.ServerReflection"],
            mock_grpc_server,
        )


def test_add_reflection_import_error(server_config_sync_insecure) -> None:
    """Test handling of import error for reflection."""
    server = MockServer(server_config_sync_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)
    server.server = mock_grpc_server

    # Add a service name
    server._service_names = ["my.test.Service"]

    # Mock the import to raise ImportError
    with mock.patch(
        "importlib.import_module",
        side_effect=ImportError("No module named 'grpc_reflection'"),
    ):
        # Call add_reflection - should not raise exception
        server._add_reflection()


# Server creation tests
def test_create_server_sync(server_config_sync_insecure) -> None:
    """Test creating a synchronous server."""
    server = MockServer(server_config_sync_insecure)

    with (
        mock.patch("digitalkin.grpc_servers._base_server.grpc.server") as mock_server,
        mock.patch("digitalkin.grpc_servers._base_server.futures.ThreadPoolExecutor") as mock_executor,
    ):
        result = server._create_server()

        # Verify server was created with correct parameters
        mock_executor.assert_called_once_with(max_workers=server_config_sync_insecure.max_workers)
        mock_server.assert_called_once()

        # Verify result is the mock server
        if result != mock_server.return_value:
            pytest.fail(f"Expected result to be {mock_server.return_value}, got {result}")


def test_create_server_async(server_config_async_insecure) -> None:
    """Test creating an asynchronous server."""
    server = MockServer(server_config_async_insecure)

    with mock.patch("digitalkin.grpc_servers._base_server.grpc_aio.server") as mock_server:
        result = server._create_server()

        # Verify server was created with correct parameters
        mock_server.assert_called_once_with(options=server_config_async_insecure.server_options)

        # Verify result is the mock server
        if result != mock_server.return_value:
            pytest.fail(f"Expected result to be {mock_server.return_value}, got {result}")


# Port configuration tests
def test_add_insecure_port_sync(server_config_sync_insecure) -> None:
    """Test adding an insecure port to a sync server."""
    server = MockServer(server_config_sync_insecure)
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)

    server._add_insecure_port(mock_grpc_server)

    # Verify add_insecure_port was called
    mock_grpc_server.add_insecure_port.assert_called_once_with(server_config_sync_insecure.address)


def test_add_insecure_port_async(server_config_async_insecure) -> None:
    """Test adding an insecure port to an async server."""
    server = MockServer(server_config_async_insecure)
    mock_grpc_server = mock.MagicMock(spec=grpc_aio.Server)

    server._add_insecure_port(mock_grpc_server)

    # Verify add_insecure_port was called
    mock_grpc_server.add_insecure_port.assert_called_once_with(server_config_async_insecure.address)


@mock.patch("digitalkin.grpc_servers._base_server.grpc.ssl_server_credentials")
def test_add_secure_port_sync(mock_ssl_creds, server_config_sync_secure) -> None:
    """Test adding a secure port to a sync server."""
    server = MockServer(server_config_sync_secure)
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)

    # Mock the SSL credentials
    mock_ssl_creds.return_value = "mock_credentials"

    # Call add_secure_port with file mocking
    with mock.patch("builtins.open", mock.mock_open(read_data=b"DUMMY DATA")):
        server._add_secure_port(mock_grpc_server)

    # Verify add_secure_port was called
    mock_grpc_server.add_secure_port.assert_called_once_with(server_config_sync_secure.address, "mock_credentials")


def test_add_secure_port_no_credentials(server_config_sync_insecure) -> None:
    """Test error when adding secure port with no credentials."""
    server = MockServer(server_config_sync_insecure)
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)

    # Call add_secure_port
    with pytest.raises(SecurityError):
        server._add_secure_port(mock_grpc_server)


# Server start tests
def test_start_sync(server_config_sync_insecure) -> None:
    """Test starting a synchronous server."""
    server = MockServer(server_config_sync_insecure)

    with (
        mock.patch.object(server, "_create_server") as mock_create,
        mock.patch.object(server, "_register_servicers") as mock_register,
        mock.patch.object(server, "_add_health_service") as mock_health,
        mock.patch.object(server, "_add_reflection") as mock_reflection,
    ):
        # Create a mock server
        mock_grpc_server = mock.MagicMock(spec=grpc.Server)
        mock_create.return_value = mock_grpc_server

        # Call start
        server.start()

        # Verify methods were called
        mock_create.assert_called_once()
        mock_register.assert_called_once()
        mock_health.assert_called_once()
        mock_reflection.assert_called_once()

        # Verify server was started
        mock_grpc_server.start.assert_called_once()

        # Verify server was set
        if server.server != mock_grpc_server:
            pytest.fail(f"Expected server.server to be {mock_grpc_server}, got {server.server}")


def test_start_error(server_config_sync_insecure) -> None:
    """Test error handling when starting server fails."""
    server = MockServer(server_config_sync_insecure)

    with mock.patch.object(server, "_create_server") as mock_create:
        # Create a mock server that raises an exception
        mock_grpc_server = mock.MagicMock(spec=grpc.Server)
        mock_grpc_server.start.side_effect = RuntimeError("Start error")
        mock_create.return_value = mock_grpc_server

        # Call start
        with pytest.raises(ServerStateError):
            server.start()


# Tests for async server methods
@pytest.mark.asyncio
async def test_start_async_method(server_config_async_insecure) -> None:
    """Test the _start_async method."""
    server = MockServer(server_config_async_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc_aio.Server)
    server.server = mock_grpc_server

    # Call _start_async
    await server._start_async()

    # Verify start was called
    mock_grpc_server.start.assert_called_once()


@pytest.mark.asyncio
async def test_start_async_server(server_config_async_insecure) -> None:
    """Test the start_async method."""
    server = MockServer(server_config_async_insecure)

    with (
        mock.patch.object(server, "_create_server") as mock_create,
        mock.patch.object(server, "_register_servicers") as mock_register,
        mock.patch.object(server, "_add_health_service") as mock_health,
        mock.patch.object(server, "_add_reflection") as mock_reflection,
        mock.patch.object(server, "_start_async") as mock_start_async,
    ):
        # Create a mock server
        mock_grpc_server = mock.MagicMock(spec=grpc_aio.Server)
        mock_create.return_value = mock_grpc_server

        # Call start_async
        await server.start_async()

        # Verify methods were called
        mock_create.assert_called_once()
        mock_register.assert_called_once()
        mock_health.assert_called_once()
        mock_reflection.assert_called_once()
        mock_start_async.assert_called_once()

        # Verify server was set
        if server.server != mock_grpc_server:
            pytest.fail(f"Expected server.server to be {mock_grpc_server}, got {server.server}")


# Server stop tests
def test_stop_sync(server_config_sync_insecure) -> None:
    """Test stopping a synchronous server."""
    server = MockServer(server_config_sync_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)
    server.server = mock_grpc_server

    # Call stop
    server.stop(grace=5.0)

    # Verify stop was called
    mock_grpc_server.stop.assert_called_once_with(grace=5.0)

    # Verify server was cleared
    if server.server is not None:
        pytest.fail(f"Expected server.server to be None, got {server.server}")


def test_stop_no_server(server_config_sync_insecure) -> None:
    """Test stopping when no server is running."""
    server = MockServer(server_config_sync_insecure)

    # Call stop (should not raise an exception)
    server.stop()


@pytest.mark.asyncio
async def test_stop_async_method(server_config_async_insecure) -> None:
    """Test the _stop_async method."""
    server = MockServer(server_config_async_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc_aio.Server)
    server.server = mock_grpc_server

    # Call _stop_async
    await server._stop_async(grace=5.0)

    # Verify stop was called
    mock_grpc_server.stop.assert_called_once_with(grace=5.0)


@pytest.mark.asyncio
async def test_stop_async_server(server_config_async_insecure) -> None:
    """Test the stop_async method."""
    server = MockServer(server_config_async_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc_aio.Server)
    server.server = mock_grpc_server

    # Call stop_async
    await server.stop_async(grace=5.0)

    # Verify stop was called
    mock_grpc_server.stop.assert_called_once_with(grace=5.0)

    # Verify server was cleared
    if server.server is not None:
        pytest.fail(f"Expected server.server to be None, got {server.server}")


# Termination tests
def test_wait_for_termination_sync(server_config_sync_insecure) -> None:
    """Test wait_for_termination with a sync server."""
    server = MockServer(server_config_sync_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)
    server.server = mock_grpc_server

    # Call wait_for_termination
    server.wait_for_termination()

    # Verify wait_for_termination was called
    mock_grpc_server.wait_for_termination.assert_called_once()


@pytest.mark.asyncio
async def test_await_termination_async(server_config_async_insecure) -> None:
    """Test await_termination with an async server."""
    server = MockServer(server_config_async_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc_aio.Server)
    server.server = mock_grpc_server

    # Call await_termination
    await server.await_termination()

    # Verify wait_for_termination was called
    mock_grpc_server.wait_for_termination.assert_called_once()


# Health service tests
def test_add_health_service_alternative(server_config_sync_insecure) -> None:
    """Test adding health service to the server (simplified)."""
    # Skip this test if health-checking package isn't installed
    try:
        import grpc_health  # noqa: F401, PLC0415
    except ImportError:
        pytest.skip("grpc_health package not installed")

    server = MockServer(server_config_sync_insecure)

    # Create a mock server
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)
    server.server = mock_grpc_server

    # Add a service name
    server._service_names = ["my.test.Service"]

    # Call add_health_service
    server._add_health_service()

    # Check that the health servicer was created
    if server._health_servicer is None:
        pytest.fail("Expected _health_servicer to be not None")

    # Check that at least one service name was added (the health service)
    if not len(server._service_names) > 1:
        pytest.fail(f"Expected _service_names to have more than one entry, got {server._service_names}")

    if not any("health" in name.lower() for name in server._service_names):
        pytest.fail(f"Expected _service_names to contain a health service, got {server._service_names}")


def test_base_server_register_servicers_is_abstract() -> None:
    """Test that _register_servicers is an abstract method that must be implemented."""
    from digitalkin.models.grpc_servers.models import ServerConfig  # noqa: PLC0415

    config = ServerConfig()  # type: ignore

    # Create a class that doesn't implement _register_servicers
    class BadServer(BaseServer):
        pass

    # Attempting to instantiate BadServer should raise TypeError
    try:
        BadServer(config)  # type: ignore
        pytest.fail("Expected TypeError when creating class without implementing _register_servicers")
    except TypeError:
        # This is expected - abstract method must be implemented
        pass


def test_register_servicers_checks_server_existence(
    server_config_sync_insecure,
) -> None:
    """Test that a good implementation of _register_servicers checks for server existence."""
    # MockServer already implements the check
    server = MockServer(server_config_sync_insecure)

    # Ensure server is None
    server.server = None

    # Calling _register_servicers should raise ServicerError
    with pytest.raises(ServicerError, match="Server must be created before registering servicers"):
        server._register_servicers()


def test_register_servicers_with_server(server_config_sync_insecure) -> None:
    """Test that _register_servicers works when server is set."""

    # Create a special test server with a flag to check if method was called
    class FlagServer(BaseServer):
        def _register_servicers(self) -> None:
            if self.server is None:
                msg = "Server must be created before registering servicers"
                raise ServicerError(msg)
            self.method_called = True

    server = FlagServer(server_config_sync_insecure)

    # Mock the server
    mock_grpc_server = mock.MagicMock(spec=grpc.Server)
    server.server = mock_grpc_server

    # Call the method
    server._register_servicers()

    # Check if the method was properly called
    if not hasattr(server, "method_called") or not server.method_called:
        pytest.fail("_register_servicers implementation was not called properly")
