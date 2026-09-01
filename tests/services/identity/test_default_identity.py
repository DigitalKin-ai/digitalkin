"""Coverage for DefaultIdentity (stub strategy)."""

from __future__ import annotations

from digitalkin.services.identity.default_identity import DefaultIdentity


class TestDefaultIdentity:
    async def test_get_identity_returns_default(self) -> None:
        identity = DefaultIdentity(mission_id="m1", setup_id="s1", setup_version_id="sv1")
        assert await identity.get_identity() == "default_identity"
