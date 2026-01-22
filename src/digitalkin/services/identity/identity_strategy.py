"""This module contains the abstract base class for identity strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.base_strategy import BaseStrategy


class IdentityStrategy(BaseStrategy, ABC):
    """IdentityStrategy is the abstract base class for all identity strategies."""

    # ════════════════════════════════ Overriding Methods ════════════════════════════════ #

    @abstractmethod
    async def get(self) -> str:
        """Get the identity."""
        return await super().get()

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        return await super().create()

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
