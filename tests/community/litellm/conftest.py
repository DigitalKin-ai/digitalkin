"""Fixtures for the LiteLLM integration tests."""

from collections.abc import Iterator

import pytest

from digitalkin.community.litellm import LiteLLMRegistry, get_litellm_settings


@pytest.fixture(autouse=True)
def _fresh_litellm_settings() -> Iterator[None]:
    """Rebuild the settings and the shared registry from each test's environment."""
    get_litellm_settings.cache_clear()
    LiteLLMRegistry._shared = None
    yield
    get_litellm_settings.cache_clear()
    LiteLLMRegistry._shared = None
