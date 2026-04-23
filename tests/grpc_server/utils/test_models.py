"""Tests for the gRPC utility models."""

import pytest

from digitalkin.grpc_servers.utils.exceptions import ConfigurationError, SecurityError
from digitalkin.models.settings.server.server import ServerSettings
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode, Credentials


@pytest.mark.grpc
class TestEnums:
    """Tests for server mode and security mode enumerations."""

    def test_server_mode_enum(self) -> None:
        """Test the ServerMode enum."""
        # Check SYNC value
        assert ControlFlow.SYNC == "sync"

        # Check ASYNC value
        assert ControlFlow.ASYNC == "async"

        # Check all enum members
        expected_members = [ControlFlow.SYNC, ControlFlow.ASYNC]
        actual_members = list(ControlFlow)

        assert actual_members == expected_members

    def test_security_mode_enum(self) -> None:
        """Test the SecurityMode enum."""
        # Check SECURE value
        assert SecurityMode.SECURE == "secure"
        # Check INSECURE value
        assert SecurityMode.INSECURE == "insecure"

        # Check all enum members
        expected_members = [SecurityMode.SECURE, SecurityMode.INSECURE]
        actual_members = list(SecurityMode)

        assert actual_members == expected_members


@pytest.mark.grpc
@pytest.mark.validation
class TestServerCredentials:
    """Tests for server credentials validation and error handling."""

    def test_server_credentials_validation(self, tmp_path) -> None:
        """Test validation of ServerCredentials."""
        # Create test certificate files
        server_key = tmp_path / "server.key"
        server_cert = tmp_path / "server.crt"
        ca_cert = tmp_path / "ca.crt"

        server_key.write_text("TEST KEY")
        server_cert.write_text("TEST CERT")
        ca_cert.write_text("TEST CA CERT")

        # Test valid credentials
        creds = Credentials(
            key_path=server_key,
            cert_path=server_cert,
            root_cert_path=ca_cert,
        )

        # Check key path
        assert creds.key_path == server_key

        # Check cert path
        assert creds.cert_path == server_cert

        # Check root cert path
        assert creds.root_cert_path == ca_cert

        # Test optional root cert
        creds_no_ca = Credentials(
            key_path=server_key,
            cert_path=server_cert,
        )

        # Check root cert is None
        assert creds_no_ca.root_cert_path is None

    def test_server_credentials_validation_errors(self, tmp_path) -> None:
        """Test validation errors in ServerCredentials."""
        # Create only one of the files
        server_key = tmp_path / "server.key"
        server_key.write_text("TEST KEY")

        # Missing certificate file should raise error
        with pytest.raises(SecurityError):
            Credentials(
                key_path=server_key,
                cert_path=tmp_path / "nonexistent.crt",
            )


@pytest.mark.grpc
@pytest.mark.validation
class TestServerConfig:
    """Tests for server configuration validation and properties."""

    def test_server_config_defaults(self) -> None:
        """Test default values for ServerConfig."""
        config = ServerSettings()

        # Check host
        assert config.channel.host == "[::]"
        # Check port
        assert config.channel.port == 50055
        # Check max_workers
        assert config.max_workers == 10
        # Check mode
        assert config.channel.communication_mode == ControlFlow.ASYNC
        # Check security
        assert config.channel.security == SecurityMode.INSECURE
        # Check credentials
        assert config.channel.credentials is None

        # Check server_options (message limits + keepalive support)
        expected_server_options = [
            ('grpc.max_receive_message_length', 104857600),
            ('grpc.max_send_message_length', 104857600),
            ('grpc.keepalive_time_ms', 120000),
            ('grpc.keepalive_timeout_ms', 20000),
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.http2.min_ping_interval_without_data_ms', 10000)
        ]
        assert config.grpc.options == expected_server_options
        # Check enable_reflection
        assert config.reflection is True
        # Check enable_health_check
        assert config.health_check is True

    def test_server_config_custom(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test custom values for ServerConfig."""
        expected_message_lenght = 10 * 1024 * 1024

        monkeypatch.setenv("SERVER_CHANNEL_HOST", "localhost")
        monkeypatch.setenv("SERVER_CHANNEL_PORT", "8000")
        monkeypatch.setenv("SERVER_CHANNEL_COMMUNICATION_MODE", "async")
        monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "insecure")
        monkeypatch.setenv("SERVER_GRPC_OPTIONS_MAX_SEND_MESSAGE_LENGTH", str(expected_message_lenght))
        monkeypatch.setenv("SERVER_MAX_WORKERS", "4")
        monkeypatch.setenv("SERVER_REFLECTION", "false")
        monkeypatch.setenv("SERVER_HEALTH_CHECK", "false")

        config = ServerSettings()

        # Check host
        assert config.channel.host == "localhost"
        # Check port
        assert config.channel.port == 8000
        # Check max_workers
        assert config.max_workers == 4
        # Check mode
        assert config.channel.communication_mode == ControlFlow.ASYNC
        # Check security
        assert config.channel.security == SecurityMode.INSECURE
        # Check server_options
        assert config.grpc.max_send_message_length == expected_message_lenght
        # Check enable_reflection
        assert config.reflection is False
        # Check enable_health_check
        assert config.health_check is False

    def test_server_config_secure_without_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test error when secure mode is specified without credentials."""
        # When creating a ServerConfig with secure mode but no credentials,
        # it should raise ConfigurationError
        with pytest.raises(ConfigurationError, match="Credentials must be provided when using secure mode"):
            monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "secure")
            monkeypatch.delenv("SERVER_CHANNEL_CREDENTIALS", raising=False)
            ServerSettings()

    def test_server_config_secure_with_credentials(self, dummy_certs, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that secure mode with proper credentials is valid."""
        monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "secure")

        config = ServerSettings()

        assert config.channel.security == SecurityMode.SECURE
        assert config.channel.credentials.key_path == dummy_certs[0]
        assert config.channel.credentials.cert_path == dummy_certs[1]
        assert config.channel.credentials.root_cert_path == dummy_certs[2]

    def test_server_config_insecure_with_credentials(self, dummy_certs, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that insecure mode can have credentials (though not required)."""
        monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "insecure")

        config = ServerSettings()

        assert config.channel.security == SecurityMode.INSECURE
        assert config.channel.credentials.key_path == dummy_certs[0]
        assert config.channel.credentials.cert_path == dummy_certs[1]
        assert config.channel.credentials.root_cert_path == dummy_certs[2]

    def test_server_config_insecure_without_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that insecure mode without credentials is valid."""
        # This should not raise an exception
        monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "insecure")
        monkeypatch.delenv("SERVER_CHANNEL_CREDENTIALS", raising=False)

        config = ServerSettings()

        assert config.channel.security == SecurityMode.INSECURE
        assert config.channel.credentials is None

    def test_server_config_port_validation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test port assignment behavior in ServerSettings.

        The current model declares ``port`` as ``int`` without explicit range checks,
        so out-of-range values are accepted.
        """
        # Common ports
        try:
            monkeypatch.setenv("SERVER_CHANNEL_PORT", "1")
            ServerSettings()
            monkeypatch.setenv("SERVER_CHANNEL_PORT", "65535")
            ServerSettings()
        except Exception as e:
            pytest.fail(f"Valid port validation failed: {e}")

        # Out-of-range values are currently accepted (no explicit bounds on `port`).
        with pytest.raises(ConfigurationError, match="Port must be between 1 and 65535, got 0"):
            monkeypatch.setenv("SERVER_CHANNEL_PORT", "0")
            config_low = ServerSettings()

        with pytest.raises(ConfigurationError, match="Port must be between 1 and 65535, got 65536"):
            monkeypatch.setenv("SERVER_CHANNEL_PORT", "65536")
            config_low = ServerSettings()

    def test_server_config_address(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test the address property of ServerSettings."""
        monkeypatch.setenv("SERVER_CHANNEL_HOST", "localhost")
        monkeypatch.setenv("SERVER_CHANNEL_PORT", "8000")
        config = ServerSettings()
        expected_address = "localhost:8000"

        assert config.channel.address == expected_address


@pytest.mark.grpc
class TestServerConfigSubclasses:
    """Tests for server configuration subclasses (ModuleServerConfig and RegistryServerConfig)."""

    def test_module_server_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test ModuleServerConfig specific properties."""
        monkeypatch.setenv("SERVER_CHANNEL_ADVERTISE_HOST", "digitalkin-test-archetype-server")
        config = ServerSettings()
        expected_advertise_host = "digitalkin-test-archetype-server"

        assert config.channel.advertise_host == expected_advertise_host
