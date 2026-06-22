"""Tests for UserProfileTools — lazy fetch, caching, and error retry."""

import json
from typing import Any

from digitalkin.community.agno.toolkits import UserProfileTools
from digitalkin.services.user_profile import DefaultUserProfile, UserProfileServiceError, UserProfileStrategy


class _CountingProfile(UserProfileStrategy):
    """Strategy that counts calls and can fail on demand."""

    def __init__(self, profile: dict[str, Any] | None, fail_times: int = 0) -> None:
        super().__init__("missions:m1", "", "")
        self._profile = profile
        self._fail_times = fail_times
        self.calls = 0

    async def get_user_profile(self) -> dict[str, Any] | None:
        self.calls += 1
        if self._fail_times > 0:
            self._fail_times -= 1
            msg = "boom"
            raise UserProfileServiceError(msg)
        return self._profile

    async def check_resource_access(self, resource_type: int, resource_id: str) -> bool:
        return True


async def test_profile_returned_as_json() -> None:
    strategy = DefaultUserProfile("missions:m1", "", "")
    strategy.add_user_profile({"name": "Ada", "plan": "pro"})
    tools = UserProfileTools(strategy)
    result = json.loads(await tools.get_user_profile())
    assert result["output"] == {"name": "Ada", "plan": "pro"}


async def test_missing_profile_reports_unavailable() -> None:
    tools = UserProfileTools(DefaultUserProfile("missions:m1", "", ""))
    result = json.loads(await tools.get_user_profile())
    assert result["error"] == "user profile is not available"


async def test_profile_fetched_once_across_calls() -> None:
    strategy = _CountingProfile({"name": "Ada"})
    tools = UserProfileTools(strategy)
    await tools.get_user_profile()
    await tools.get_user_profile()
    assert strategy.calls == 1


async def test_service_error_retried_on_next_call() -> None:
    strategy = _CountingProfile({"name": "Ada"}, fail_times=1)
    tools = UserProfileTools(strategy)
    assert json.loads(await tools.get_user_profile())["error"] == "user profile is not available"
    assert json.loads(await tools.get_user_profile())["output"] == {"name": "Ada"}
    assert strategy.calls == 2
