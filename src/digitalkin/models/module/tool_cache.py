"""Tool cache for managing resolved tool references.

The ToolCache is a registry that stores resolved ModuleInfo by slug.
It is populated during run_config_setup and validated during initialize.
LLMs check the cache before calling the registry for tool discovery.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from digitalkin.logger import logger
from digitalkin.models.services.registry import ModuleInfo

if TYPE_CHECKING:
    from digitalkin.services.registry import RegistryStrategy


class ToolCacheEntry(BaseModel):
    """Single entry in the tool cache."""

    slug: str = Field(description="Unique identifier/slug for this tool")
    module_id: str = Field(description="Resolved module ID")
    module_info: ModuleInfo = Field(description="Full module information")
    is_valid: bool = Field(default=True, description="Whether this entry is still valid")

    model_config = {"arbitrary_types_allowed": True}


class ToolCache(BaseModel):
    """Registry cache for resolved tools.

    Stores tool references by slug during run_config_setup and provides
    lookup methods for LLMs during execution. Tools must be checked
    against the cache before calling the registry.

    Flow:
    1. run_config_setup: Registry called, results cached by slug
    2. initialize: Cache validated (tools still available)
    3. Runtime: LLM checks cache first, then registry if not found
    """

    entries: dict[str, ToolCacheEntry] = Field(default_factory=dict)

    def add(self, slug: str, module_info: ModuleInfo) -> None:
        """Add a tool to the cache.

        Args:
            slug: Unique identifier for this tool.
            module_info: Resolved module information.
        """
        self.entries[slug] = ToolCacheEntry(
            slug=slug,
            module_id=module_info.module_id,
            module_info=module_info,
            is_valid=True,
        )
        logger.debug("Tool cached", extra={"slug": slug, "module_id": module_info.module_id})

    def get(self, slug: str) -> ModuleInfo | None:
        """Get a tool from the cache by slug.

        Args:
            slug: The tool slug to look up.

        Returns:
            ModuleInfo if found and valid, None otherwise.
        """
        entry = self.entries.get(slug)
        if entry and entry.is_valid:
            return entry.module_info
        return None

    def contains(self, slug: str) -> bool:
        """Check if a tool exists in the cache.

        Args:
            slug: The tool slug to check.

        Returns:
            True if tool exists and is valid.
        """
        entry = self.entries.get(slug)
        return entry is not None and entry.is_valid

    def invalidate(self, slug: str) -> None:
        """Mark a tool as invalid.

        Args:
            slug: The tool slug to invalidate.
        """
        if slug in self.entries:
            self.entries[slug].is_valid = False
            logger.debug("Tool invalidated", extra={"slug": slug})

    def remove(self, slug: str) -> None:
        """Remove a tool from the cache.

        Args:
            slug: The tool slug to remove.
        """
        if slug in self.entries:
            del self.entries[slug]
            logger.debug("Tool removed from cache", extra={"slug": slug})

    def clear(self) -> None:
        """Clear all entries from the cache."""
        self.entries.clear()

    def check_and_get(
        self,
        slug: str,
        registry: RegistryStrategy | None = None,
    ) -> ModuleInfo | None:
        """Check cache first, then optionally query registry.

        This is the primary method for LLMs to discover tools. It:
        1. Checks if the tool is in cache (fast path)
        2. If not in cache and registry provided, queries registry
        3. If found via registry, caches the result

        Args:
            slug: The tool slug to look up.
            registry: Optional registry to query if not in cache.

        Returns:
            ModuleInfo if found, None otherwise.
        """
        # Fast path: check cache
        cached = self.get(slug)
        if cached:
            logger.debug("Tool cache hit", extra={"slug": slug})
            return cached

        # Not in cache - try registry if available
        if registry:
            logger.debug("Tool cache miss, querying registry", extra={"slug": slug})
            try:
                # Try by ID first (slug might be module_id)
                info = registry.discover_by_id(slug)
                if info:
                    self.add(slug, info)
                    return info

                # Try by tag search
                results = registry.search(name=slug, module_type="tool", organization_id=None)
                if results:
                    info = results[0]
                    self.add(slug, info)
                    return info
            except Exception:
                logger.exception("Registry lookup failed", extra={"slug": slug})

        return None

    def validate(self, registry: RegistryStrategy) -> list[str]:
        """Validate all cached tools are still available.

        Checks each cached tool against the registry and marks
        invalid entries. Returns list of invalid slugs.

        Args:
            registry: Registry to validate against.

        Returns:
            List of slugs that are no longer valid.
        """
        invalid: list[str] = []
        for slug, entry in self.entries.items():
            if not entry.is_valid:
                continue
            try:
                info = registry.discover_by_id(entry.module_id)
                if not info:
                    entry.is_valid = False
                    invalid.append(slug)
                    logger.warning("Tool no longer available", extra={"slug": slug, "module_id": entry.module_id})
            except Exception:
                entry.is_valid = False
                invalid.append(slug)
                logger.exception("Tool validation failed", extra={"slug": slug})
        return invalid

    def list_slugs(self) -> list[str]:
        """Get list of all valid tool slugs.

        Returns:
            List of valid tool slugs.
        """
        return [slug for slug, entry in self.entries.items() if entry.is_valid]

    def to_dict(self) -> dict[str, dict]:
        """Serialize cache to dict for storage.

        Returns:
            Dict representation of cache entries.
        """
        return {
            slug: {
                "slug": entry.slug,
                "module_id": entry.module_id,
                "is_valid": entry.is_valid,
                "module_info": entry.module_info.model_dump(),
            }
            for slug, entry in self.entries.items()
        }

    @classmethod
    def from_dict(cls, data: dict[str, dict]) -> ToolCache:
        """Deserialize cache from dict.

        Args:
            data: Dict representation of cache entries.

        Returns:
            ToolCache instance.
        """
        cache = cls()
        for slug, entry_data in data.items():
            cache.entries[slug] = ToolCacheEntry(
                slug=entry_data["slug"],
                module_id=entry_data["module_id"],
                module_info=ModuleInfo(**entry_data["module_info"]),
                is_valid=entry_data.get("is_valid", True),
            )
        return cache
