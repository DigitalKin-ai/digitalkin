"""Toolkit exposing the current user's profile to the agent."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.logger import logger
from digitalkin.services.user_profile.exceptions import UserProfileServiceError

if TYPE_CHECKING:
    from digitalkin.models.module import ModuleContext
    from digitalkin.services.user_profile.user_profile_strategy import UserProfileStrategy


class UserProfileTools(DkToolkit):
    """Toolkit that gives the agent access to the current user's profile.

    The profile is fetched lazily from the module's
    :class:`~digitalkin.services.user_profile.UserProfileStrategy` on first use and
    cached for the toolkit's lifetime. A service failure is NOT cached, so a
    transient error is retried on the next call; a successful ``None`` (no profile) is.
    """

    def __init__(self, user_profile: UserProfileStrategy, context: ModuleContext | None = None) -> None:
        """Initialize toolkit with the ``get_user_profile`` tool.

        Args:
            user_profile: The module's user-profile service strategy.
            context: Module context; enables AG-UI notifications via the base toolkit.
        """
        self._user_profile = user_profile
        self._profile: dict[str, Any] | None = None
        self._loaded = False
        super().__init__(
            name="user_profile_tools",
            tools=[self.get_user_profile],
            context=context,
        )

    async def get_user_profile(self) -> str:
        """Retrieve the current user's profile: name, email, subscription plan, remaining credits, and mission cost.

        ``mission_cost`` is what the current mission has accumulated so far, in the same
        unit as the credit balance.

        IMPORTANT: You do NOT know what credits represent, how they are consumed,
        or what they correspond to in terms of usage. Never speculate, explain, or
        invent information about credits. Simply report the raw values as-is.

        Returns:
            The canonical envelope: ``{"output": <profile>, ...}`` or ``{"error": ...}``.
        """
        if not self._loaded:
            try:
                self._profile = await self._user_profile.get_user_profile()
                self._loaded = True
            except UserProfileServiceError as error:
                logger.warning("UserProfileTools: failed to fetch profile: %s", error)
        if not self._profile:
            return self._fail("user profile is not available", tool="get_user_profile")
        return self._ok(self._profile, tool="get_user_profile")
