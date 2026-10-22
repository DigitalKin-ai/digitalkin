"""Langfuse settings: tracing is opt-in through both keys."""

import pytest

from digitalkin.community.langfuse import LangfuseSettings, get_langfuse_settings


class TestLangfuseSettings:
    def test_disabled_without_keys(self) -> None:
        assert get_langfuse_settings().enabled is False

    def test_enabled_with_both_keys(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_LANGFUSE_PUBLIC_KEY", "pk")
        monkeypatch.setenv("DIGITALKIN_LANGFUSE_SECRET_KEY", "sk")
        monkeypatch.setenv("DIGITALKIN_LANGFUSE_BASE_URL", "https://lf.test")

        settings = get_langfuse_settings()

        assert settings.enabled is True
        assert settings.base_url == "https://lf.test"
        assert settings.secret_key.get_secret_value() == "sk"

    def test_one_key_is_not_enough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_LANGFUSE_PUBLIC_KEY", "pk")

        assert LangfuseSettings().enabled is False

    def test_environment_defaults_to_development(self) -> None:
        assert LangfuseSettings().environment == "development"

    def test_environment_reads_the_variable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_LANGFUSE_ENVIRONMENT", "production")

        assert LangfuseSettings().environment == "production"

    def test_ignores_the_unprefixed_names(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The old ``LANGFUSE_*`` names are no longer read: a deployment must set the prefixed ones."""
        monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk")
        monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk")
        monkeypatch.setenv("LANGFUSE_ENVIRONMENT", "production")

        settings = LangfuseSettings()

        assert settings.enabled is False
        assert settings.environment == "development"
