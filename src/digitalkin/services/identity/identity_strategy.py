"""This module contains the abstract base class for identity strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.base_strategy import BaseStrategy


class IdentityStrategy(BaseStrategy, ABC):
    """IdentityStrategy is the abstract base class for all identity strategies."""

    # ════════════════════════════════ Overriding Methods ════════════════════════════════ #

    @abstractmethod
    async def get(self) -> str:
        """Get the identity.

        Returns:
            The identity string.
        """
        return await super().get()

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented.

        Returns:
            NotImplementedError from base class.
        """
        return await super().create(args, kwargs)

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
