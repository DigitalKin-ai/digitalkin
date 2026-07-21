"""This module contains the base class for service strategies."""


class BaseStrategy:
    """Base class for all strategies.

    Provides the shared id fields and a no-op ``close()`` default. It has no
    abstract members, so it is a plain base (not an ``ABC``); concrete
    strategies subclass it and override as needed.
    """

    def __init__(self, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup this strategy is associated with
            setup_version_id: The ID of the setup version this strategy is associated with
        """
        self.mission_id: str = mission_id
        self.setup_id: str = setup_id
        self.setup_version_id: str = setup_version_id

    async def close(self) -> None:
        """Release resources held by this strategy. No-op by default."""
