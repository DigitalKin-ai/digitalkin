"""Tests for the environment-driven module configuration."""

from pathlib import Path

import pytest

from digitalkin.grpc_servers.exceptions import SecurityError
from digitalkin.models.services.services import ServicesMode
from digitalkin.models.settings.certificate import get_certificate_settings
from digitalkin.models.settings.client.client import get_client_settings
from digitalkin.models.settings.module import get_module_settings
from digitalkin.models.settings.server.server import get_server_settings
from digitalkin.models.settings.utils.channel import SecurityMode
from digitalkin.services.services_config import ServicesConfig
from digitalkin.utils.env_manager import EnvManager

pytestmark = pytest.mark.unit


class TestServicesMode:
    """SERVICE_MODE resolution."""

    def test_defaults_to_remote(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A deployed module talks to remote services unless told otherwise."""
        monkeypatch.delenv("SERVICE_MODE", raising=False)
        get_module_settings.cache_clear()
        assert EnvManager.services_mode() == ServicesMode.REMOTE

    def test_reads_service_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SERVICE_MODE", "local")
        get_module_settings.cache_clear()
        assert EnvManager.services_mode() == ServicesMode.LOCAL


class TestClientConfig:
    """ClientConfig built from the environment."""

    def test_uses_client_channel_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CLIENT_CHANNEL_HOST", "services-provider")
        monkeypatch.setenv("CLIENT_CHANNEL_PORT", "50151")
        get_client_settings.cache_clear()
        EnvManager.reset()

        config = EnvManager.client_config()

        assert config.address == "services-provider:50151"
        assert config.security == SecurityMode.INSECURE
        assert config.credentials is None

    def test_is_cached(self) -> None:
        assert EnvManager.client_config() is EnvManager.client_config()

    def test_reset_rebuilds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = EnvManager.client_config()
        monkeypatch.setenv("CLIENT_CHANNEL_HOST", "other-host")
        get_client_settings.cache_clear()
        EnvManager.reset()

        assert EnvManager.client_config() is not first
        assert EnvManager.client_config().host == "other-host"

    def test_secure_mode_reads_the_certificate_volume(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        (tmp_path / "ca.crt").write_text("ca")
        monkeypatch.setenv("CLIENT_CHANNEL_SECURITY", "secure")
        monkeypatch.setenv("CERTIFICATE_SERVICES_PROVIDER_CERT_VOLUME", str(tmp_path))
        get_client_settings.cache_clear()
        get_certificate_settings.cache_clear()
        EnvManager.reset()

        config = EnvManager.client_config()

        assert config.security == SecurityMode.SECURE
        assert config.credentials is not None
        assert config.credentials.root_cert_path == tmp_path / "ca.crt"
        assert config.credentials.client_key_path is None

    def test_mtls_presents_the_client_pair(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        for name in ("ca.crt", "client.crt", "client.key"):
            (tmp_path / name).write_text(name)
        monkeypatch.setenv("CLIENT_CHANNEL_SECURITY", "secure")
        monkeypatch.setenv("CLIENT_CHANNEL_MTLS", "true")
        monkeypatch.setenv("CERTIFICATE_SERVICES_PROVIDER_CERT_VOLUME", str(tmp_path))
        get_client_settings.cache_clear()
        get_certificate_settings.cache_clear()
        EnvManager.reset()

        credentials = EnvManager.client_config().credentials

        assert credentials is not None
        assert credentials.client_key_path == tmp_path / "client.key"
        assert credentials.client_cert_path == tmp_path / "client.crt"

    def test_secure_mode_without_a_ca_fails_loudly(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setenv("CLIENT_CHANNEL_SECURITY", "secure")
        monkeypatch.setenv("CERTIFICATE_SERVICES_PROVIDER_CERT_VOLUME", str(tmp_path))
        get_client_settings.cache_clear()
        get_certificate_settings.cache_clear()
        EnvManager.reset()

        with pytest.raises(SecurityError, match="Missing ca.crt"):
            EnvManager.client_config()

    def test_explicit_credential_paths_win_over_the_volume(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        explicit = tmp_path / "explicit-ca.crt"
        explicit.write_text("ca")
        monkeypatch.setenv("CLIENT_CHANNEL_SECURITY", "secure")
        monkeypatch.setenv("CLIENT_CHANNEL_CREDENTIALS__ROOT_CERT_PATH", str(explicit))
        get_client_settings.cache_clear()
        EnvManager.reset()

        credentials = EnvManager.client_config().credentials

        assert credentials is not None
        assert credentials.root_cert_path == explicit


class TestServicesConfigInjection:
    """Remote strategies receive the environment's client config by default.

    Async because building a remote strategy opens a ``grpc.aio`` channel, which
    needs a running event loop.
    """

    async def test_remote_strategy_gets_the_env_client_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CLIENT_CHANNEL_HOST", "services-provider")
        get_client_settings.cache_clear()
        EnvManager.reset()

        config = ServicesConfig(mode=ServicesMode.REMOTE)
        user_profile = config.init_strategy("user_profile", "mission", "setup", "version")

        assert user_profile._channel_cache_key is not None
        assert "services-provider" in user_profile._channel_cache_key
        await user_profile.close()

    async def test_registered_client_config_is_not_overridden(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CLIENT_CHANNEL_HOST", "services-provider")
        get_client_settings.cache_clear()
        EnvManager.reset()
        explicit = EnvManager.client_config().model_copy(update={"host": "explicit-host"})

        config = ServicesConfig(
            services_config_params={"user_profile": {"client_config": explicit}},
            mode=ServicesMode.REMOTE,
        )
        user_profile = config.init_strategy("user_profile", "mission", "setup", "version")

        assert user_profile._channel_cache_key is not None
        assert "explicit-host" in user_profile._channel_cache_key
        await user_profile.close()

    def test_local_strategy_gets_no_client_config(self) -> None:
        config = ServicesConfig(mode=ServicesMode.LOCAL)

        # DefaultIdentity rejects a client_config kwarg; building it proves none was injected.
        assert config.init_strategy("identity", "mission", "setup", "version") is not None


class TestServicesModeFromCli:
    """The launch flag wins over the environment."""

    def test_dev_mode_flag_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.argv", ["module", "-d", "REMOTE"])
        assert EnvManager.services_mode() == ServicesMode.REMOTE

    def test_long_flag_is_read(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.argv", ["module", "--dev-mode", "remote"])
        assert EnvManager.services_mode() == ServicesMode.REMOTE

    def test_unknown_flag_value_falls_back_to_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.argv", ["module", "-d", "bogus"])
        monkeypatch.setenv("SERVICE_MODE", "remote")
        get_module_settings.cache_clear()
        assert EnvManager.services_mode() == ServicesMode.REMOTE


class TestServerCredentials:
    """Server credentials resolve like the client's."""

    def test_explicit_paths_win(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        key, cert = tmp_path / "explicit.key", tmp_path / "explicit.crt"
        key.write_text("k")
        cert.write_text("c")
        monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "secure")
        monkeypatch.setenv("SERVER_CHANNEL_CREDENTIALS__KEY_PATH", str(key))
        monkeypatch.setenv("SERVER_CHANNEL_CREDENTIALS__CERT_PATH", str(cert))
        get_server_settings.cache_clear()

        credentials = EnvManager.server_credentials()

        assert credentials.key_path == key
        assert credentials.cert_path == cert

    def test_falls_back_to_the_certificate_volume(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        for name in ("server.key", "server.crt", "ca.crt"):
            (tmp_path / name).write_text(name)
        monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "secure")
        monkeypatch.setenv("CERTIFICATE_CERT_VOLUME", str(tmp_path))
        get_server_settings.cache_clear()
        get_certificate_settings.cache_clear()

        credentials = EnvManager.server_credentials()

        assert credentials.key_path == tmp_path / "server.key"
        assert credentials.cert_path == tmp_path / "server.crt"
        # No mTLS: the CA stays out, otherwise gRPC would require client auth.
        assert credentials.root_cert_path is None

    def test_mtls_returns_the_root_ca(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        for name in ("server.key", "server.crt", "ca.crt"):
            (tmp_path / name).write_text(name)
        monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "secure")
        monkeypatch.setenv("SERVER_CHANNEL_MTLS", "true")
        monkeypatch.setenv("CERTIFICATE_CERT_VOLUME", str(tmp_path))
        get_server_settings.cache_clear()
        get_certificate_settings.cache_clear()

        assert EnvManager.server_credentials().root_cert_path == tmp_path / "ca.crt"

    def test_empty_volume_fails_loudly(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "secure")
        monkeypatch.setenv("CERTIFICATE_CERT_VOLUME", str(tmp_path))
        get_server_settings.cache_clear()
        get_certificate_settings.cache_clear()

        with pytest.raises(SecurityError, match="Server key or certificate not found"):
            EnvManager.server_credentials()


class TestUnsetRequired:
    """The boot check and the minimum template share the ``env_required`` marker."""

    def test_reports_empty_required_fields_by_env_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from pydantic import Field, SecretStr
        from pydantic_settings import BaseSettings, SettingsConfigDict

        class Probe(BaseSettings):
            model_config = SettingsConfigDict(env_prefix="PROBE_")
            token: SecretStr = Field(default=SecretStr(""), json_schema_extra={"env_required": True})
            module_id: str = Field(default="", validation_alias="PROBE_MODULE", json_schema_extra={"env_required": True})
            optional: str = Field(default="")

        monkeypatch.delenv("PROBE_TOKEN", raising=False)
        monkeypatch.setenv("PROBE_MODULE", "modules:1")

        assert EnvManager.unset_required(Probe()) == ["PROBE_TOKEN"]

    def test_empty_when_nothing_is_required(self) -> None:
        from pydantic_settings import BaseSettings

        class Probe(BaseSettings):
            name: str = "x"

        assert EnvManager.unset_required(Probe()) == []
