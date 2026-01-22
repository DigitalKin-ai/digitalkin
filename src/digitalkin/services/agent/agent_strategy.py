"""This module contains the abstract base class for agent strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.base_strategy import BaseStrategy


class AgentStrategy(BaseStrategy, ABC):
    """Abstract base class for agent strategies."""

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    @abstractmethod
    async def start(self) -> None:
        """Start the agent."""
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        """Stop the agent."""
        raise NotImplementedError

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        return await super().create()

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        return await super().get()

    async def list(self, *args: Any, **kwargs: Any) -> Any:
        return await super().list()

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        return await super().search()

    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        return await super().delete()

    async def update(self, *args: Any, **kwargs: Any) -> Any:
        return await super().update()

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        return await super().upload()
