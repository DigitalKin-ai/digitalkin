"""Tests for the gRPC utility models."""

import pytest

from digitalkin.grpc_servers.utils.exceptions import ConfigurationError, SecurityError
from digitalkin.models.grpc_servers.models import (
    ModuleServerConfig,
    RegistryServerConfig,
    SecurityMode,
    ServerConfig,
    ServerCredentials,
    ServerMode,
)


def test_server_mode_enum() -> None:
    """Test the ServerMode enum."""
    # Check SYNC value
    if ServerMode.SYNC != "sync":
        pytest.fail(f"Expected ServerMode.SYNC to be 'sync', got '{ServerMode.SYNC}'")

    # Check ASYNC value
    if ServerMode.ASYNC != "async":
        pytest.fail(f"Expected ServerMode.ASYNC to be 'async', got '{ServerMode.ASYNC}'")

    # Check all enum members
    expected_members = [ServerMode.SYNC, ServerMode.ASYNC]
    actual_members = list(ServerMode)

    if actual_members != expected_members:
        pytest.fail(f"Expected ServerMode enum to have members {expected_members}, got {actual_members}")


def test_security_mode_enum() -> None:
    """Test the SecurityMode enum."""
    # Check SECURE value
    if SecurityMode.SECURE != "secure":
        pytest.fail(f"Expected SecurityMode.SECURE to be 'secure', got '{SecurityMode.SECURE}'")

    # Check INSECURE value
    if SecurityMode.INSECURE != "insecure":
        pytest.fail(f"Expected SecurityMode.INSECURE to be 'insecure', got '{SecurityMode.INSECURE}'")

    # Check all enum members
    expected_members = [SecurityMode.SECURE, SecurityMode.INSECURE]
    actual_members = list(SecurityMode)

    if actual_members != expected_members:
        pytest.fail(f"Expected SecurityMode enum to have members {expected_members}, got {actual_members}")


def test_server_credentials_validation(tmp_path) -> None:
    """Test validation of ServerCredentials."""
    # Create test certificate files
    server_key = tmp_path / "server.key"
    server_cert = tmp_path / "server.crt"
    ca_cert = tmp_path / "ca.crt"

    server_key.write_text("TEST KEY")
    server_cert.write_text("TEST CERT")
    ca_cert.write_text("TEST CA CERT")

    # Test valid credentials
    creds = ServerCredentials(
        server_key_path=server_key,
        server_cert_path=server_cert,
        root_cert_path=ca_cert,
    )

    # Check key path
    if creds.server_key_path != server_key:
        pytest.fail(f"Expected server_key_path to be {server_key}, got {creds.server_key_path}")

    # Check cert path
    if creds.server_cert_path != server_cert:
        pytest.fail(f"Expected server_cert_path to be {server_cert}, got {creds.server_cert_path}")

    # Check root cert path
    if creds.root_cert_path != ca_cert:
        pytest.fail(f"Expected root_cert_path to be {ca_cert}, got {creds.root_cert_path}")

    # Test optional root cert
    creds_no_ca = ServerCredentials(
        server_key_path=server_key,
        server_cert_path=server_cert,
    )

    # Check root cert is None
    if creds_no_ca.root_cert_path is not None:
        pytest.fail(f"Expected root_cert_path to be None, got {creds_no_ca.root_cert_path}")


def test_server_credentials_validation_errors(tmp_path) -> None:
    """Test validation errors in ServerCredentials."""
    # Create only one of the files
    server_key = tmp_path / "server.key"
    server_key.write_text("TEST KEY")

    # Missing certificate file should raise error
    with pytest.raises(SecurityError):
        ServerCredentials(
            server_key_path=server_key,
            server_cert_path=tmp_path / "nonexistent.crt",
        )


def test_server_config_defaults() -> None:
    """Test default values for ServerConfig."""
    config = ServerConfig()

    # Check host
    if config.host != "0.0.0.0":  # noqa: S104
        pytest.fail(f"Expected default host to be '0.0.0.0', got '{config.host}'")

    # Check port
    if config.port != 50051:
        pytest.fail(f"Expected default port to be 50051, got {config.port}")

    # Check max_workers
    if config.max_workers != 10:
        pytest.fail(f"Expected default max_workers to be 10, got {config.max_workers}")

    # Check mode
    if config.mode != ServerMode.SYNC:
        pytest.fail(f"Expected default mode to be {ServerMode.SYNC}, got {config.mode}")

    # Check security
    if config.security != SecurityMode.INSECURE:
        pytest.fail(f"Expected default security to be {SecurityMode.INSECURE}, got {config.security}")

    # Check credentials
    if config.credentials is not None:
        pytest.fail(f"Expected default credentials to be None, got {config.credentials}")

    # Check server_options
    if config.server_options != [
        ("grpc.max_receive_message_length", 100 * 1024 * 1024),  # 100MB
        ("grpc.max_send_message_length", 100 * 1024 * 1024),  # 100MB
    ]:
        pytest.fail(f"Expected default server_options to be empty list, got {config.server_options}")

    # Check enable_reflection
    if config.enable_reflection is not True:
        pytest.fail(f"Expected default enable_reflection to be True, got {config.enable_reflection}")

    # Check enable_health_check
    if config.enable_health_check is not True:
        pytest.fail(f"Expected default enable_health_check to be True, got {config.enable_health_check}")


def test_server_config_custom() -> None:
    """Test custom values for ServerConfig."""
    expected_options = [("grpc.max_receive_message_length", 10 * 1024 * 1024)]

    config = ServerConfig(
        host="localhost",
        port=8000,
        max_workers=4,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        server_options=expected_options,
        enable_reflection=False,
        enable_health_check=False,
    )

    # Check host
    if config.host != "localhost":
        pytest.fail(f"Expected host to be 'localhost', got '{config.host}'")

    # Check port
    if config.port != 8000:
        pytest.fail(f"Expected port to be 8000, got {config.port}")

    # Check max_workers
    if config.max_workers != 4:
        pytest.fail(f"Expected max_workers to be 4, got {config.max_workers}")

    # Check mode
    if config.mode != ServerMode.ASYNC:
        pytest.fail(f"Expected mode to be {ServerMode.ASYNC}, got {config.mode}")

    # Check security
    if config.security != SecurityMode.INSECURE:
        pytest.fail(f"Expected security to be {SecurityMode.INSECURE}, got {config.security}")

    # Check server_options
    if config.server_options != expected_options:
        pytest.fail(f"Expected server_options to be {expected_options}, got {config.server_options}")

    # Check enable_reflection
    if config.enable_reflection is not False:
        pytest.fail(f"Expected enable_reflection to be False, got {config.enable_reflection}")

    # Check enable_health_check
    if config.enable_health_check is not False:
        pytest.fail(f"Expected enable_health_check to be False, got {config.enable_health_check}")


def test_server_config_secure_without_credentials() -> None:
    """Test error when secure mode is specified without credentials."""
    # When creating a ServerConfig with secure mode but no credentials,
    # it should raise ConfigurationError
    with pytest.raises(ConfigurationError, match="Credentials must be provided when using secure mode"):
        ServerConfig(
            security=SecurityMode.SECURE,
            credentials=None,
        )


def test_server_config_secure_with_credentials(dummy_certs) -> None:
    """Test that secure mode with proper credentials is valid."""
    credentials = ServerCredentials(**dummy_certs)

    # This should not raise an exception
    config = ServerConfig(
        security=SecurityMode.SECURE,
        credentials=credentials,
    )

    if config.security != SecurityMode.SECURE:
        pytest.fail(f"Expected security to be {SecurityMode.SECURE}, got {config.security}")

    if config.credentials != credentials:
        pytest.fail(f"Expected credentials to match input, got {config.credentials}")


def test_server_config_insecure_with_credentials(dummy_certs) -> None:
    """Test that insecure mode can have credentials (though not required)."""
    credentials = ServerCredentials(**dummy_certs)

    # This should not raise an exception
    config = ServerConfig(
        security=SecurityMode.INSECURE,
        credentials=credentials,
    )

    if config.security != SecurityMode.INSECURE:
        pytest.fail(f"Expected security to be {SecurityMode.INSECURE}, got {config.security}")

    if config.credentials != credentials:
        pytest.fail(f"Expected credentials to match input, got {config.credentials}")


def test_server_config_insecure_without_credentials() -> None:
    """Test that insecure mode without credentials is valid."""
    # This should not raise an exception
    config = ServerConfig(
        security=SecurityMode.INSECURE,
        credentials=None,
    )

    if config.security != SecurityMode.INSECURE:
        pytest.fail(f"Expected security to be {SecurityMode.INSECURE}, got {config.security}")

    if config.credentials is not None:
        pytest.fail(f"Expected credentials to be None, got {config.credentials}")


def test_server_config_port_validation() -> None:
    """Test port validation in ServerConfig."""
    # Valid ports
    try:
        ServerConfig(port=1)
        ServerConfig(port=65535)
    except Exception as e:
        pytest.fail(f"Valid port validation failed: {e}")

    # Invalid ports - keep pytest.raises as it's not an assertion
    with pytest.raises(ConfigurationError):
        ServerConfig(port=0)

    with pytest.raises(ConfigurationError):
        ServerConfig(port=65536)


def test_server_config_address() -> None:
    """Test the address property of ServerConfig."""
    config = ServerConfig(host="localhost", port=8000)
    expected_address = "localhost:8000"

    if config.address != expected_address:
        pytest.fail(f"Expected address to be '{expected_address}', got '{config.address}'")


def test_module_server_config() -> None:
    """Test ModuleServerConfig specific properties."""
    config = ModuleServerConfig(registry_address="localhost:50051")
    expected_registry_address = "localhost:50051"

    if config.registry_address != expected_registry_address:
        pytest.fail(f"Expected registry_address to be '{expected_registry_address}', got '{config.registry_address}'")


def test_registry_server_config() -> None:
    """Test RegistryServerConfig specific properties."""
    config = RegistryServerConfig(database_url="sqlite:///registry.db")
    expected_database_url = "sqlite:///registry.db"

    if config.database_url != expected_database_url:
        pytest.fail(f"Expected database_url to be '{expected_database_url}', got '{config.database_url}'")
