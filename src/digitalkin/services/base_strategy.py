"""This module contains the abstract base class for storage strategies."""

from abc import ABC


class RequestContext:
    """Lightweight per-request identity context.

    Carries mission_id, setup_id, setup_version_id for injection into
    shared service method calls instead of storing them in service constructors.
    """

    __slots__ = ("mission_id", "setup_id", "setup_version_id")

    def __init__(self, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        """Initialize the request context.

        Args:
            mission_id: The ID of the mission for this request.
            setup_id: The ID of the setup for this request.
            setup_version_id: The ID of the setup version for this request.
        """
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id


class BaseStrategy(ABC):
    """Abstract base class for all strategies.

    This class defines the interface for all strategies.
    Strategies are shared singletons — request-specific IDs are passed
    via RequestContext at call-time, not stored in the constructor.
    """

    def __init__(self) -> None:
        """Initialize the strategy."""
