"""Common fixtures for test suite."""

import logging

import pytest

# Configure logging for tests
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Silence some loggers during tests
logging.getLogger("grpc").setLevel(logging.WARNING)


@pytest.fixture
def server_config_sync_insecure():
    """Create a sync insecure server configuration."""
    from digitalkin.grpc.utils.models import SecurityMode, ServerConfig, ServerMode

    return ServerConfig(
        host="localhost",
        port=50051,
        mode=ServerMode.SYNC,
        security=SecurityMode.INSECURE,
    )


@pytest.fixture
def server_config_async_insecure():
    """Create an async insecure server configuration."""
    from digitalkin.grpc.utils.models import SecurityMode, ServerConfig, ServerMode

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
    from digitalkin.grpc.utils.models import SecurityMode, ServerConfig, ServerCredentials, ServerMode

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
    from digitalkin.grpc.utils.models import SecurityMode, ServerConfig, ServerCredentials, ServerMode

    credentials = ServerCredentials(**dummy_certs)

    return ServerConfig(
        host="localhost",
        port=50054,
        mode=ServerMode.ASYNC,
        security=SecurityMode.SECURE,
        credentials=credentials,
    )
