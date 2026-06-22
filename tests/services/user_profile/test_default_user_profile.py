"""Coverage for DefaultUserProfile (in-memory strategy, was ~39%)."""

from __future__ import annotations

from digitalkin.services.user_profile.default_user_profile import DefaultUserProfile


def _profile() -> DefaultUserProfile:
    return DefaultUserProfile(mission_id="m1", setup_id="s1", setup_version_id="sv1")


class TestDefaultUserProfile:
    async def test_get_missing_returns_none(self) -> None:
        assert await _profile().get_user_profile() is None

    async def test_add_then_get_roundtrip(self) -> None:
        up = _profile()
        up.add_user_profile({"name": "alice", "role": "admin"})
        assert await up.get_user_profile() == {"name": "alice", "role": "admin"}

    async def test_isolated_per_mission(self) -> None:
        a = DefaultUserProfile(mission_id="ma", setup_id="s", setup_version_id="sv")
        a.add_user_profile({"x": 1})
        b = DefaultUserProfile(mission_id="mb", setup_id="s", setup_version_id="sv")
        assert await b.get_user_profile() is None
