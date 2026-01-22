"""This module contains the abstract base class for agent strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy


class AgentStrategy(BaseStrategy, ABC):
    """Abstract base class for agent strategies."""

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    @abstractmethod
    def start(self) -> None:
        """Start the agent."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stop the agent."""
        raise NotImplementedError

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    def create(self, *args: Any, **kwargs: Any) -> Any:
        return super().create()

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return super().get()

    def list(self, *args: Any, **kwargs: Any) -> Any:
        return super().list()

    def search(self, *args: Any, **kwargs: Any) -> Any:
        return super().search()

    def delete(self, *args: Any, **kwargs: Any) -> Any:
        return super().delete()

    def update(self, *args: Any, **kwargs: Any) -> Any:
        return super().update()

    def upload(self, *args: Any, **kwargs: Any) -> Any:
        return super().upload()
