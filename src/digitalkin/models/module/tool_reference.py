"""Tool reference types for archetype module configuration."""

from enum import Enum
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, PrivateAttr, model_validator

from digitalkin.models.services.registry import ModuleInfo

if TYPE_CHECKING:
    from digitalkin.services.registry import RegistryStrategy


class ToolSelectionMode(str, Enum):
    """Mode for tool selection in archetype setup."""

    FIXED = "fixed"
    TAG = "tag"
    DISCOVERABLE = "discoverable"


class ToolReferenceConfig(BaseModel):
    """Configuration for how a tool should be selected."""

    mode: ToolSelectionMode = Field(default=ToolSelectionMode.FIXED)
    slug: str | None = Field(default=None, description="Unique slug for cache lookup")
    fixed_id: str | None = Field(default=None, description="Module ID for FIXED mode")
    tag: str | None = Field(default=None, description="Search tag for TAG mode")
    organization_id: str | None = Field(default=None, description="Filter by organization")

    @model_validator(mode="after")
    def validate_config(self) -> "ToolReferenceConfig":
        """Validate configuration based on mode.

        Returns:
            Self if validation passes.

        Raises:
            ValueError: If required field is missing for the mode.
        """
        if self.mode == ToolSelectionMode.FIXED and not self.fixed_id:
            msg = "fixed_id required when mode is FIXED"
            raise ValueError(msg)
        if self.mode == ToolSelectionMode.TAG and not self.tag:
            msg = "tag required when mode is TAG"
            raise ValueError(msg)
        return self


class ToolReference(BaseModel):
    """Reference to a tool module for archetype configuration.

    Frontend sets config, backend resolves to actual ModuleInfo.
    The resolved module_id is persisted in selected_module_id for
    subsequent StartModule calls without re-resolution.
    """

    config: ToolReferenceConfig
    selected_module_id: str | None = Field(default=None, description="Resolved module ID after resolution")
    _cached_info: ModuleInfo | None = PrivateAttr(default=None)

    @property
    def slug(self) -> str | None:
        """Get the slug for cache lookup.

        Returns config.slug if set, otherwise falls back to fixed_id or tag.
        """
        if self.config.slug:
            return self.config.slug
        if self.config.mode == ToolSelectionMode.FIXED and self.config.fixed_id:
            return self.config.fixed_id
        if self.config.mode == ToolSelectionMode.TAG and self.config.tag:
            return self.config.tag
        return None

    @property
    def module_info(self) -> ModuleInfo | None:
        """Get cached ModuleInfo if resolved."""
        return self._cached_info

    @property
    def is_resolved(self) -> bool:
        """Check if this reference has been resolved."""
        return self._cached_info is not None or self.selected_module_id is not None

    def resolve(self, registry: "RegistryStrategy") -> ModuleInfo | None:
        """Resolve this reference using the provided registry.

        For FIXED mode, looks up by fixed_id.
        For TAG mode, searches by tag and takes first result.
        For DISCOVERABLE mode, returns None (LLM handles at runtime).

        Args:
            registry: Registry service to use for resolution.

        Returns:
            ModuleInfo if resolved, None if not resolvable or DISCOVERABLE mode.
        """
        if self.config.mode == ToolSelectionMode.DISCOVERABLE:
            return None

        if self.config.mode == ToolSelectionMode.FIXED and self.config.fixed_id:
            info = registry.discover_by_id(self.config.fixed_id)
            if info:
                self._cached_info = info
                self.selected_module_id = self.config.fixed_id
            return info

        if self.config.mode == ToolSelectionMode.TAG and self.config.tag:
            results = registry.search(
                name=self.config.tag,
                module_type="tool",
                organization_id=self.config.organization_id,
            )
            if results:
                self._cached_info = results[0]
                self.selected_module_id = results[0].module_id
                return results[0]

        return None
