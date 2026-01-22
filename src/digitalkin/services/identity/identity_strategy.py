"""This module contains the abstract base class for identity strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy


class IdentityStrategy(BaseStrategy, ABC):
    """IdentityStrategy is the abstract base class for all identity strategies."""

    # ════════════════════════════════ Overriding Methods ════════════════════════════════ #

    @abstractmethod
    async def get(self) -> str:
        """Get the identity."""
        return super().get()

    # ══════════════════════════════ Unimplemented Methods ═══════════════════════════════ #

    def create(self, *args: Any, **kwargs: Any) -> Any:
        return super().create()

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
