"""Tool reference types for module configuration."""

from enum import Enum

from pydantic import BaseModel, Field, PrivateAttr, model_validator

from digitalkin.models.module.tool_cache import ToolModuleInfo, module_info_to_tool_module_info
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.registry import RegistryStrategy


class ToolSelectionMode(str, Enum):
    """Tool selection mode."""

    TAG = "tag"
    FIXED = "fixed"
    DISCOVERABLE = "discoverable"


class ToolReferenceConfig(BaseModel):
    """Tool selection configuration. The module_id serves as both identifier and cache key."""

    mode: ToolSelectionMode = Field(default=ToolSelectionMode.FIXED)
    setup_id: str = Field(default="")
    module_id: str = Field(default="")
    tag: str = Field(default="")
    organization_id: str = Field(default="")

    @model_validator(mode="after")
    def validate_config(self) -> "ToolReferenceConfig":
        """Validate required fields based on mode.

        Returns:
            Self if validation passes.

        Raises:
            ValueError: If required field is missing for the mode.
        """
        if self.mode == ToolSelectionMode.FIXED and not self.setup_id:
            msg = "setup_id required when mode is FIXED"
            raise ValueError(msg)
        if self.mode == ToolSelectionMode.TAG and not self.tag:
            msg = "tag required when mode is TAG"
            raise ValueError(msg)
        return self


class ToolReference(BaseModel):
    """Reference to a tool module, resolved via registry during config setup."""

    config: ToolReferenceConfig
    _cached_info: ToolModuleInfo | None = PrivateAttr(default=None)

    @property
    def slug(self) -> str:
        """Cache key (same as module_id).

        Returns:
            Module ID used as cache key.
        """
        return self.config.setup_id

    @property
    def module_id(self) -> str:
        """Module identifier.

        Returns:
            Module ID or empty string if not set.
        """
        return self.config.module_id

    @property
    def setup_id(self) -> str:
        """Setup identifier.

        Returns:
            Setup ID or empty string if not set.
        """
        return self.config.setup_id

    @property
    def tool_module_info(self) -> ToolModuleInfo | None:
        """Resolved module information.

        Returns:
            ToolModuleInfo if resolved, None otherwise.
        """
        return self._cached_info

    @property
    def is_resolved(self) -> bool:
        """Whether this reference has been resolved.

        Returns:
            True if resolved, False otherwise.
        """
        return self._cached_info is not None

    async def resolve(self, registry: RegistryStrategy, communication: CommunicationStrategy) -> ToolModuleInfo | None:
        """Resolve this reference using the registry.

        Args:
            registry: Registry service for module discovery.
            communication: Communication service for module schemas.

        Returns:
            ToolModuleInfo if resolved, None for DISCOVERABLE mode or if not found.
        """
        if self.config.mode == ToolSelectionMode.DISCOVERABLE:
            return None

        if self.config.mode == ToolSelectionMode.FIXED and self.config.setup_id:
            setup = registry.get_setup(self.config.setup_id)
            if setup and setup.module_id:
                self.config.module_id = setup.module_id
                info = registry.discover_by_id(self.config.module_id)
                if info:
                    tool_module_info = await module_info_to_tool_module_info(info, self.config.setup_id, communication)
                    self._cached_info = tool_module_info
                    return tool_module_info

        if self.config.mode == ToolSelectionMode.TAG and self.config.tag:
            results = registry.search(
                name=self.config.tag,
                module_type="tool",
                organization_id=self.config.organization_id,
            )
            if results:
                tool_module_info = await module_info_to_tool_module_info(
                    results[0], self.config.setup_id, communication
                )
                self._cached_info = tool_module_info
                self.config.module_id = tool_module_info.module_id
                return tool_module_info

        return None
