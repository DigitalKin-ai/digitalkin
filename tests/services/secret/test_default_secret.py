"""Tests for DefaultSecret (in-memory local strategy)."""

from digitalkin.services.secret.default_secret import DefaultSecret


def _secret() -> DefaultSecret:
    return DefaultSecret(mission_id="m", setup_id="s", setup_version_id="sv")


class TestDefaultSecret:
    async def test_get_missing_returns_none(self) -> None:
        assert await _secret().get_secret() is None

    async def test_add_then_get_roundtrip(self) -> None:
        sec = _secret()
        sec.add_secret({"api_key": "xyz"})
        assert await sec.get_secret() == {"api_key": "xyz"}

    async def test_isolated_per_setup(self) -> None:
        a = DefaultSecret(mission_id="m", setup_id="sa", setup_version_id="sv")
        a.add_secret({"k": 1})
        b = DefaultSecret(mission_id="m", setup_id="sb", setup_version_id="sv")
        assert await b.get_secret() is None
