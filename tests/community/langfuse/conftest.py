"""Fixtures for the Langfuse integration tests."""

from collections.abc import Iterator

import pytest

from digitalkin.community.langfuse import LangfuseTracing, get_langfuse_settings


@pytest.fixture(autouse=True)
def _fresh_langfuse(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Start every test from an empty Langfuse environment and an uninitialized provider."""
    for name in ("PUBLIC_KEY", "SECRET_KEY", "BASE_URL", "ENVIRONMENT"):
        monkeypatch.delenv(f"LANGFUSE_{name}", raising=False)
        monkeypatch.delenv(f"DIGITALKIN_LANGFUSE_{name}", raising=False)
    monkeypatch.setattr(LangfuseTracing, "_initialized", False)
    get_langfuse_settings.cache_clear()
    yield
    get_langfuse_settings.cache_clear()
