"""Tool cache for resolved tool references."""

from pydantic import BaseModel, Field

from digitalkin.logger import logger
from digitalkin.models.services.registry import ModuleInfo
from digitalkin.services.registry import RegistryStrategy


class ToolCache(BaseModel):
    """Registry cache storing resolved tool references by setup field name."""

    entries: dict[str, ModuleInfo] = Field(default_factory=dict)

    def add(self, setup_tool_name: str, module_info: ModuleInfo) -> None:
        """Add a tool to the cache.

        Args:
            setup_tool_name: Field name from SetupModel used as cache key.
            module_info: Resolved module information.
        """
        self.entries[setup_tool_name] = module_info
        logger.debug(
            "Tool cached",
            extra={"setup_tool_name": setup_tool_name, "module_id": module_info.module_id},
        )

    def get(
        self,
        setup_tool_name: str,
        *,
        registry: RegistryStrategy | None = None,
    ) -> ModuleInfo | None:
        """Get a tool from cache, optionally querying registry on miss.

        Args:
            setup_tool_name: Field name to look up.
            registry: Optional registry to query on cache miss.

        Returns:
            ModuleInfo if found, None otherwise.
        """
        cached = self.entries.get(setup_tool_name)
        if cached:
            return cached

        if registry:
            try:
                info = registry.discover_by_id(setup_tool_name)
                if info:
                    self.add(setup_tool_name, info)
                    return info
            except Exception:
                logger.exception("Registry lookup failed", extra={"setup_tool_name": setup_tool_name})

        return None

    def clear(self) -> None:
        """Clear all cache entries."""
        self.entries.clear()

    def list_tools(self) -> list[str]:
        """List all cached tool names.

        Returns:
            List of setup field names in cache.
        """
        return list(self.entries.keys())
