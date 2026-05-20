"""Models for dynamic-schema fetcher resolution."""

from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class ResolveResult(BaseModel):
    """Result of resolving dynamic fetchers.

    Provides structured access to resolved values and any errors that occurred.
    This allows callers to handle partial failures gracefully.

    Attributes:
        values: Dict mapping key names to successfully resolved values.
        errors: Dict mapping key names to exceptions that occurred during resolution.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    values: dict[str, Any] = Field(default_factory=dict)
    errors: dict[str, Exception] = Field(default_factory=dict)

    @property
    def success(self) -> bool:
        """Check if all fetchers resolved successfully.

        Returns:
            True if no errors occurred, False otherwise.
        """
        return len(self.errors) == 0

    @property
    def partial(self) -> bool:
        """Check if some but not all fetchers succeeded.

        Returns:
            True if there are both values and errors, False otherwise.
        """
        return len(self.values) > 0 and len(self.errors) > 0

    def get(self, key: str, default: T | None = None) -> T | None:
        """Get a resolved value by key.

        Args:
            key: The fetcher key name.
            default: Default value if key not found or errored.

        Returns:
            The resolved value or default.
        """
        return self.values.get(key, default)  # type: ignore[return-value]
