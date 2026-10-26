"""LiteLLM proxy settings: the ``LITELLM_*`` names and the boot check."""

import pytest

from digitalkin.community.litellm import LiteLLMConfig, LiteLLMSettings, get_litellm_settings
from digitalkin.utils.env_manager import EnvManager

_NAMES = ("DIGITALKIN_LITELLM_BASE_URL", "DIGITALKIN_LITELLM_API_KEY", "LITELLM_BASE_URL", "LITELLM_API_KEY")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _NAMES:
        monkeypatch.delenv(name, raising=False)


class TestLiteLLMSettings:
    def test_reads_the_litellm_names(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LITELLM_BASE_URL", "https://proxy.test/")
        monkeypatch.setenv("LITELLM_API_KEY", "token")

        settings = get_litellm_settings()

        assert settings.base_url == "https://proxy.test/"
        assert settings.api_key.get_secret_value() == "token"

    def test_ignores_the_prefixed_names(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Deployments set ``LITELLM_*``: the ``DIGITALKIN_LITELLM_*`` names are not read."""
        monkeypatch.setenv("DIGITALKIN_LITELLM_BASE_URL", "https://prefixed.test/")
        monkeypatch.setenv("DIGITALKIN_LITELLM_API_KEY", "prefixed")

        settings = LiteLLMSettings()

        assert not settings.base_url
        assert not settings.api_key.get_secret_value()

    def test_token_is_a_secret(self) -> None:
        assert "token" not in repr(LiteLLMSettings(api_key="token"))  # type: ignore[arg-type]

    def test_a_placeholder_is_not_a_value(self) -> None:
        """The template's ``sk-xxxx`` example must never become the runtime default."""
        settings = LiteLLMSettings()

        assert not settings.api_key.get_secret_value()
        assert not settings.base_url

    def test_factory_is_cached(self) -> None:
        assert get_litellm_settings() is get_litellm_settings()

    def test_config_picks_up_the_environment_when_built(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LITELLM_BASE_URL", "https://late.test/")
        monkeypatch.setenv("LITELLM_API_KEY", "late-token")

        config = LiteLLMConfig()

        assert config.base_url == "https://late.test/"
        assert config.bearer_token == "late-token"
        assert config.timeout == 60


class TestUnsetRequired:
    """``EnvManager.unset_required`` reports the names the module refuses to boot without."""

    def test_reports_an_unset_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LITELLM_BASE_URL", "https://llm.test/")

        assert EnvManager.unset_required(LiteLLMSettings()) == ["LITELLM_API_KEY"]

    def test_reports_an_unset_base_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LITELLM_API_KEY", "token")

        assert EnvManager.unset_required(LiteLLMSettings()) == ["LITELLM_BASE_URL"]

    def test_empty_when_everything_is_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LITELLM_BASE_URL", "https://llm.test/")
        monkeypatch.setenv("LITELLM_API_KEY", "token")

        assert EnvManager.unset_required(LiteLLMSettings()) == []
