"""Common fixtures for test suite."""

import logging

import grpc_testing
import pytest
from _pytest.fixtures import SubRequest

from digitalkin.grpc_servers.utils.models import (
    SecurityMode,
    ServerConfig,
    ServerCredentials,
    ServerMode,
)

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Silence some loggers during tests
logging.getLogger("grpc").setLevel(logging.WARNING)


@pytest.fixture
def server_config_sync_insecure():
    """Create a sync insecure server configuration."""
    return ServerConfig(
        host="localhost",
        port=50051,
        mode=ServerMode.SYNC,
        security=SecurityMode.INSECURE,
    )


@pytest.fixture
def server_config_async_insecure():
    """Create an async insecure server configuration."""
    return ServerConfig(
        host="localhost",
        port=50052,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
    )


@pytest.fixture
def dummy_certs(tmp_path):
    """Create dummy certificates for testing secure connections."""
    # Create certificate files
    server_key = tmp_path / "server.key"
    server_cert = tmp_path / "server.crt"
    ca_cert = tmp_path / "ca.crt"

    # Write dummy content
    server_key.write_text("DUMMY KEY CONTENT")
    server_cert.write_text("DUMMY CERT CONTENT")
    ca_cert.write_text("DUMMY CA CERT CONTENT")

    return {
        "server_key_path": server_key,
        "server_cert_path": server_cert,
        "root_cert_path": ca_cert,
    }


@pytest.fixture
def server_config_sync_secure(dummy_certs):
    """Create a sync secure server configuration."""
    credentials = ServerCredentials(**dummy_certs)

    return ServerConfig(
        host="localhost",
        port=50053,
        mode=ServerMode.SYNC,
        security=SecurityMode.SECURE,
        credentials=credentials,
    )


@pytest.fixture
def server_config_async_secure(dummy_certs):
    """Create an async secure server configuration."""
    credentials = ServerCredentials(**dummy_certs)

    return ServerConfig(
        host="localhost",
        port=50054,
        mode=ServerMode.ASYNC,
        security=SecurityMode.SECURE,
        credentials=credentials,
    )


@pytest.fixture(scope="module")
def grpc_test_server(request: SubRequest) -> grpc_testing.Server:
    """Generate a Test Server with associated servicers.

    Creates a gRPC testing server from service instances defined in the test module.
    The test module must define two variables:
      - service_instance: An instance of the service to be tested
      - service_name: The service descriptor from the generated protobuf

    Args:
        request: The pytest request object containing module information.

    Raises:
        RuntimeError: If service_instance or service_name is not defined in the test module.

    Returns:
        grpc_testing.Server: Instance of gRPC testing server with the associated service.
    """
    # Get the service instance from the test module
    service_instance = getattr(request.module, "service_instance", None)
    if service_instance is None:
        msg = "Test module must define a variable `service_instance`"
        raise RuntimeError(msg)

    # Get the service descriptor from the test module
    service_name = getattr(request.module, "service_name", None)
    if service_name is None:
        msg = "Test module must define a variable `service_name`"
        raise RuntimeError(msg)

    # Create and return the gRPC testing server
    servicers = {service_name: service_instance}
    return grpc_testing.server_from_dictionary(
        servicers, grpc_testing.strict_real_time()
    )
