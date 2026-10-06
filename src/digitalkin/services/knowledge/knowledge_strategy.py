"""Abstract base class for knowledge strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.services.knowledge import Citation
from digitalkin.models.settings.knowledge import get_knowledge_settings
from digitalkin.services.base_strategy import BaseStrategy

MAX_QUERY_CHARACTERS = 4000
MAX_SOURCES = 16
MAX_LIMIT = 50
"""Bounds the protocol itself enforces on ``SearchRequest`` (protovalidate). Applied here so a
caller gets a usable answer instead of an ``INVALID_ARGUMENT`` round-trip."""


class KnowledgeStrategy(BaseStrategy, ABC):
    """Abstract base class for knowledge strategies.

    Defines the read-only interface onto the enterprise knowledge base a user has
    connected: sources are connected by the customer through the product, never by an
    agent, so searching is all a module can do.
    """

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the strategy."""
        super().__init__(mission_id, setup_id, setup_version_id)
        self.config = config

    @abstractmethod
    async def search(
        self,
        query: str,
        sources: list[str] | None = None,
        limit: int | None = None,
    ) -> list[Citation]:
        """Search the knowledge base the current user may read.

        The results are already restricted to what this user is allowed to see — the
        permission filter is compiled into the query, never applied afterwards — so a
        caller has nothing to filter and no identity to pass.

        Args:
            query: What to look for, in natural language.
            sources: Restrict the search to these connectors (e.g. ``["google_drive"]``);
                ``None`` searches every connected source.
            limit: Maximum number of documents to return; ``None`` uses the configured default.

        Returns:
            One citation per matching document, best first. Empty when nothing matched.

        Raises:
            KnowledgeServiceError: If the search could not be answered.
        """
        ...

    @staticmethod
    def normalize(
        query: str,
        sources: list[str] | None,
        limit: int | None,
    ) -> tuple[str, list[str], int]:
        """Clamp a search request to what the protocol accepts.

        Args:
            query: The raw query.
            sources: The requested connectors, if any.
            limit: The requested number of documents, if any.

        Returns:
            The trimmed query, the capped source list, and the bounded limit.

        Raises:
            ValueError: If the query holds nothing to search for.
        """
        trimmed = query.strip()
        if not trimmed:
            msg = "query must not be empty"
            raise ValueError(msg)
        asked = get_knowledge_settings().default_limit if limit is None else limit
        return (
            trimmed[:MAX_QUERY_CHARACTERS],
            (sources or [])[:MAX_SOURCES],
            max(1, min(asked, MAX_LIMIT)),
        )

    async def wait_for_ready(self, timeout: float = 1.0) -> bool:  # ruff: ignore[no-self-use]
        """Check whether the knowledge backend is reachable.

        Args:
            timeout: Max seconds to wait for connectivity.

        Returns:
            True if ready. Default implementation always returns True.
        """
        _ = timeout
        return True
