"""Tool reference types for module configuration."""

from enum import Enum

from pydantic import BaseModel, Field, PrivateAttr, model_validator

from digitalkin.models.services.registry import ModuleInfo
from digitalkin.services.registry import RegistryStrategy


class ToolSelectionMode(str, Enum):
    """Tool selection mode."""

    TAG = "tag"
    FIXED = "fixed"
    DISCOVERABLE = "discoverable"


class ToolReferenceConfig(BaseModel):
    """Tool selection configuration. The module_id serves as both identifier and cache key."""

    mode: ToolSelectionMode = Field(default=ToolSelectionMode.FIXED)
    module_id: str | None = Field(default=None)
    tag: str | None = Field(default=None)
    organization_id: str | None = Field(default=None)

    @model_validator(mode="after")
    def validate_config(self) -> "ToolReferenceConfig":
        """Validate required fields based on mode.

        Returns:
            Self if validation passes.

        Raises:
            ValueError: If required field is missing for the mode.
        """
        if self.mode == ToolSelectionMode.FIXED and not self.module_id:
            msg = "module_id required when mode is FIXED"
            raise ValueError(msg)
        if self.mode == ToolSelectionMode.TAG and not self.tag:
            msg = "tag required when mode is TAG"
            raise ValueError(msg)
        return self


class ToolReference(BaseModel):
    """Reference to a tool module, resolved via registry during config setup."""

    config: ToolReferenceConfig
    _cached_info: ModuleInfo | None = PrivateAttr(default=None)

    @property
    def slug(self) -> str | None:
        """Cache key (same as module_id).

        Returns:
            Module ID used as cache key.
        """
        return self.config.module_id

    @property
    def module_id(self) -> str | None:
        """Module identifier.

        Returns:
            Module ID or None if not set.
        """
        return self.config.module_id

    @property
    def module_info(self) -> ModuleInfo | None:
        """Resolved module information.

        Returns:
            ModuleInfo if resolved, None otherwise.
        """
        return self._cached_info

    @property
    def is_resolved(self) -> bool:
        """Whether this reference has been resolved.

        Returns:
            True if resolved, False otherwise.
        """
        return self._cached_info is not None

    def resolve(self, registry: RegistryStrategy) -> ModuleInfo | None:
        """Resolve this reference using the registry.

        Args:
            registry: Registry service for module discovery.

        Returns:
            ModuleInfo if resolved, None for DISCOVERABLE mode or if not found.
        """
        if self.config.mode == ToolSelectionMode.DISCOVERABLE:
            return None

        if self.config.mode == ToolSelectionMode.FIXED and self.config.module_id:
            info = registry.discover_by_id(self.config.module_id)
            if info:
                self._cached_info = info
            return info

        if self.config.mode == ToolSelectionMode.TAG and self.config.tag:
            results = registry.search(
                name=self.config.tag,
                module_type="tool",
                organization_id=self.config.organization_id,
            )
            if results:
                self._cached_info = results[0]
                self.config.module_id = results[0].module_id
                return results[0]

        return None
