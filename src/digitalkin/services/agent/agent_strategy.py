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
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop the agent."""
        raise NotImplementedError

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().create(args, kwargs)

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().get(args, kwargs)

    async def list(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().list(args, kwargs)

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().search(args, kwargs)

    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().delete(args, kwargs)

    async def update(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().update(args, kwargs)

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().upload(args, kwargs)
