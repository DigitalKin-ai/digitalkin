"""Tests for the ``.env`` template generator."""

from pathlib import Path

import pytest

from digitalkin.utils.env_template import EnvTemplate

pytestmark = pytest.mark.unit

PACKAGE = "digitalkin.models.settings"


class TestRender:
    """The template mirrors the declared settings classes."""

    def test_every_settings_field_appears(self) -> None:
        rendered = EnvTemplate.render([PACKAGE])

        for name in (
            "SERVER_CHANNEL_HOST",
            "SERVER_GRPC_OPTIONS_KEEPALIVE_TIME",
            "CLIENT_CHANNEL_PORT",
            "CLIENT_CIRCUIT_BREAKER_FAIL_MAX",
            "DIGITALKIN_REDIS_URL",
            "CERTIFICATE_CERT_VOLUME",
            "SERVICE_MODE",
        ):
            assert f"{name}=" in rendered

    def test_defaults_are_rendered_in_env_syntax(self) -> None:
        rendered = EnvTemplate.render([PACKAGE])

        assert "CLIENT_CHANNEL_SECURITY=insecure" in rendered  # enum → value
        assert "CLIENT_CHANNEL_MTLS=false" in rendered  # bool → lowercase
        assert "DIGITALKIN_REDIS_URL=redis://localhost:6379/0" in rendered  # SecretStr → value

    def test_descriptions_become_comments(self) -> None:
        rendered = EnvTemplate.render([PACKAGE])

        assert "# Consecutive failures before the circuit opens." in rendered

    def test_composed_settings_are_not_duplicated(self) -> None:
        rendered = EnvTemplate.render([PACKAGE])

        # ServerSettings composes `channel`; only the leaf class renders its fields.
        assert "SERVER_CHANNEL=" not in rendered
        assert "SERVER_GRPC=" not in rendered

    def test_base_classes_are_not_rendered(self) -> None:
        """BaseChannelSettings' own CHANNEL_ prefix is never read: subclasses override it."""
        rendered = EnvTemplate.render([PACKAGE])

        assert "CHANNEL_HOST=" not in rendered.replace("SERVER_CHANNEL_HOST=", "").replace("CLIENT_CHANNEL_HOST=", "")

    def test_runtime_prefixed_fields_are_skipped(self) -> None:
        """BulkheadServiceSettings gets its prefix at instantiation, so it has no static name."""
        rendered = EnvTemplate.render([PACKAGE])

        assert "\nMAX=" not in rendered

    def test_env_example_replaces_the_runtime_default(self) -> None:
        """A placeholder documents the shape without becoming the value the code runs with."""
        from pydantic import Field, SecretStr
        from pydantic_settings import BaseSettings, SettingsConfigDict

        class Probe(BaseSettings):
            model_config = SettingsConfigDict(env_prefix="PROBE_")
            token: SecretStr = Field(default=SecretStr(""), json_schema_extra={"env_example": "sk-xxxx"})

        rows = EnvTemplate._entries(Probe, minimum=False)

        assert rows == [("PROBE_TOKEN", "sk-xxxx", "", False)]
        assert Probe().token.get_secret_value() == ""

    def test_nested_credentials_are_commented_out(self) -> None:
        rendered = EnvTemplate.render([PACKAGE])

        assert "#CLIENT_CHANNEL_CREDENTIALS__ROOT_CERT_PATH=" in rendered
        assert "#SERVER_CHANNEL_CREDENTIALS__KEY_PATH=" in rendered


class TestMinimum:
    """The minimum template keeps only the variables a deployment must set."""

    def test_keeps_the_required_fields(self) -> None:
        rendered = EnvTemplate.render([PACKAGE], minimum=True)

        for name in (
            "DIGITALKIN_REDIS_URL",
            "CLIENT_CHANNEL_HOST",
            "CLIENT_CHANNEL_PORT",
            "SERVER_CHANNEL_ADVERTISE_HOST",
        ):
            assert f"{name}=" in rendered

    def test_service_mode_has_a_usable_default(self) -> None:
        """Remote is the deployed default, so SERVICE_MODE is documented but not required."""
        assert "SERVICE_MODE=" not in EnvTemplate.render([PACKAGE], minimum=True)
        assert "SERVICE_MODE=remote" in EnvTemplate.render([PACKAGE])

    def test_drops_everything_else(self) -> None:
        rendered = EnvTemplate.render([PACKAGE], minimum=True)

        assert "CLIENT_CIRCUIT_BREAKER_FAIL_MAX" not in rendered
        assert "SERVER_GRPC_OPTIONS_KEEPALIVE_TIME" not in rendered

    def test_is_a_subset_of_the_full_template(self) -> None:
        full = EnvTemplate.render([PACKAGE])
        minimum = EnvTemplate.render([PACKAGE], minimum=True)

        names = [line.split("=")[0] for line in minimum.splitlines() if "=" in line and not line.startswith("#")]
        assert names
        for name in names:
            assert f"{name}=" in full


class TestMain:
    """The CLI writes and checks the file."""

    def test_writes_the_file(self, tmp_path: Path) -> None:
        target = tmp_path / ".env.exemple"

        assert EnvTemplate.main(["--package", PACKAGE, "--output", str(target)]) == 0
        assert "CLIENT_CHANNEL_HOST=" in target.read_text()

    def test_check_passes_on_a_fresh_file(self, tmp_path: Path) -> None:
        target = tmp_path / ".env.exemple"
        EnvTemplate.main(["--package", PACKAGE, "--output", str(target)])

        assert EnvTemplate.main(["--package", PACKAGE, "--output", str(target), "--check"]) == 0

    def test_check_fails_on_a_stale_file(self, tmp_path: Path) -> None:
        target = tmp_path / ".env.exemple"
        target.write_text("STALE=1\n")

        assert EnvTemplate.main(["--package", PACKAGE, "--output", str(target), "--check"]) == 1

    def test_check_fails_when_the_file_is_missing(self, tmp_path: Path) -> None:
        target = tmp_path / "absent.env"

        assert EnvTemplate.main(["--package", PACKAGE, "--output", str(target), "--check"]) == 1

    def test_repository_templates_are_up_to_date(self) -> None:
        """The committed templates must match the settings classes."""
        root = Path(__file__).resolve().parents[2]

        assert (root / ".env.exemple").read_text() == EnvTemplate.render([PACKAGE])
        assert (root / ".env.minimum").read_text() == EnvTemplate.render([PACKAGE], minimum=True)
