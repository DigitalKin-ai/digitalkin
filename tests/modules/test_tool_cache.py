"""Tests for ToolCache functionality."""

from unittest.mock import Mock

import pytest

from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_cache import ToolCache, ToolCacheEntry
from digitalkin.models.module.tool_reference import ToolReference, ToolReferenceConfig, ToolSelectionMode
from digitalkin.models.services.registry import ModuleInfo, RegistryModuleType


@pytest.fixture
def sample_module_info() -> ModuleInfo:
    """Create a sample ModuleInfo for testing."""
    return ModuleInfo(
        module_id="tool-123",
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50051,
        version="1.0.0",
        name="TestTool",
        documentation="Test tool documentation",
    )


@pytest.fixture
def sample_module_info_2() -> ModuleInfo:
    """Create a second sample ModuleInfo for testing."""
    return ModuleInfo(
        module_id="tool-456",
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50052,
        version="2.0.0",
        name="AnotherTool",
        documentation="Another test tool",
    )


class TestToolCacheEntry:
    """Tests for ToolCacheEntry."""

    def test_entry_creation(self, sample_module_info: ModuleInfo) -> None:
        """Test creating a cache entry."""
        entry = ToolCacheEntry(
            slug="my-tool",
            module_id="tool-123",
            module_info=sample_module_info,
            is_valid=True,
        )
        assert entry.slug == "my-tool"
        assert entry.module_id == "tool-123"
        assert entry.module_info == sample_module_info
        assert entry.is_valid is True

    def test_entry_defaults_to_valid(self, sample_module_info: ModuleInfo) -> None:
        """Test that is_valid defaults to True."""
        entry = ToolCacheEntry(
            slug="my-tool",
            module_id="tool-123",
            module_info=sample_module_info,
        )
        assert entry.is_valid is True


class TestToolCache:
    """Tests for ToolCache."""

    def test_add_and_get(self, sample_module_info: ModuleInfo) -> None:
        """Test adding and getting a tool."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)

        result = cache.get("my-tool")
        assert result == sample_module_info

    def test_get_nonexistent_returns_none(self) -> None:
        """Test getting a nonexistent tool returns None."""
        cache = ToolCache()
        assert cache.get("nonexistent") is None

    def test_get_invalid_returns_none(self, sample_module_info: ModuleInfo) -> None:
        """Test getting an invalid tool returns None."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)
        cache.invalidate("my-tool")

        assert cache.get("my-tool") is None

    def test_contains(self, sample_module_info: ModuleInfo) -> None:
        """Test contains method."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)

        assert cache.contains("my-tool") is True
        assert cache.contains("nonexistent") is False

    def test_contains_invalid_returns_false(self, sample_module_info: ModuleInfo) -> None:
        """Test contains returns False for invalid entries."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)
        cache.invalidate("my-tool")

        assert cache.contains("my-tool") is False

    def test_invalidate(self, sample_module_info: ModuleInfo) -> None:
        """Test invalidating a tool."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)
        cache.invalidate("my-tool")

        assert cache.entries["my-tool"].is_valid is False

    def test_remove(self, sample_module_info: ModuleInfo) -> None:
        """Test removing a tool."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)
        cache.remove("my-tool")

        assert "my-tool" not in cache.entries

    def test_clear(
        self, sample_module_info: ModuleInfo, sample_module_info_2: ModuleInfo
    ) -> None:
        """Test clearing all tools."""
        cache = ToolCache()
        cache.add("tool1", sample_module_info)
        cache.add("tool2", sample_module_info_2)
        cache.clear()

        assert len(cache.entries) == 0

    def test_list_slugs(
        self, sample_module_info: ModuleInfo, sample_module_info_2: ModuleInfo
    ) -> None:
        """Test listing slugs."""
        cache = ToolCache()
        cache.add("tool1", sample_module_info)
        cache.add("tool2", sample_module_info_2)

        slugs = cache.list_slugs()
        assert "tool1" in slugs
        assert "tool2" in slugs

    def test_list_slugs_excludes_invalid(
        self, sample_module_info: ModuleInfo, sample_module_info_2: ModuleInfo
    ) -> None:
        """Test that list_slugs excludes invalid entries."""
        cache = ToolCache()
        cache.add("tool1", sample_module_info)
        cache.add("tool2", sample_module_info_2)
        cache.invalidate("tool1")

        slugs = cache.list_slugs()
        assert "tool1" not in slugs
        assert "tool2" in slugs

    def test_check_and_get_cache_hit(self, sample_module_info: ModuleInfo) -> None:
        """Test check_and_get returns cached value."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)

        result = cache.check_and_get("my-tool")
        assert result == sample_module_info

    def test_check_and_get_registry_lookup(self, sample_module_info: ModuleInfo) -> None:
        """Test check_and_get queries registry on cache miss."""
        cache = ToolCache()
        mock_registry = Mock()
        mock_registry.discover_by_id.return_value = sample_module_info

        result = cache.check_and_get("tool-123", mock_registry)

        assert result == sample_module_info
        mock_registry.discover_by_id.assert_called_once_with("tool-123")
        # Should be cached now
        assert cache.get("tool-123") == sample_module_info

    def test_check_and_get_registry_search(self, sample_module_info: ModuleInfo) -> None:
        """Test check_and_get searches registry if discover_by_id fails."""
        cache = ToolCache()
        mock_registry = Mock()
        mock_registry.discover_by_id.return_value = None
        mock_registry.search.return_value = [sample_module_info]

        result = cache.check_and_get("search-tool", mock_registry)

        assert result == sample_module_info
        mock_registry.search.assert_called_once_with(
            name="search-tool", module_type="tool", organization_id=None
        )

    def test_check_and_get_no_registry_returns_none(self) -> None:
        """Test check_and_get returns None if no registry and not cached."""
        cache = ToolCache()
        result = cache.check_and_get("nonexistent")
        assert result is None

    def test_validate_all_valid(self, sample_module_info: ModuleInfo) -> None:
        """Test validate when all tools are still available."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)

        mock_registry = Mock()
        mock_registry.discover_by_id.return_value = sample_module_info

        invalid = cache.validate(mock_registry)

        assert invalid == []
        assert cache.entries["my-tool"].is_valid is True

    def test_validate_marks_invalid(self, sample_module_info: ModuleInfo) -> None:
        """Test validate marks unavailable tools as invalid."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)

        mock_registry = Mock()
        mock_registry.discover_by_id.return_value = None

        invalid = cache.validate(mock_registry)

        assert "my-tool" in invalid
        assert cache.entries["my-tool"].is_valid is False

    def test_to_dict(self, sample_module_info: ModuleInfo) -> None:
        """Test serializing cache to dict."""
        cache = ToolCache()
        cache.add("my-tool", sample_module_info)

        data = cache.to_dict()

        assert "my-tool" in data
        assert data["my-tool"]["slug"] == "my-tool"
        assert data["my-tool"]["module_id"] == "tool-123"
        assert data["my-tool"]["is_valid"] is True

    def test_from_dict(self, sample_module_info: ModuleInfo) -> None:
        """Test deserializing cache from dict."""
        data = {
            "my-tool": {
                "slug": "my-tool",
                "module_id": "tool-123",
                "is_valid": True,
                "module_info": sample_module_info.model_dump(),
            }
        }

        cache = ToolCache.from_dict(data)

        assert cache.contains("my-tool")
        assert cache.get("my-tool").module_id == "tool-123"


class TestSetupModelToolCache:
    """Tests for SetupModel tool cache integration."""

    def test_build_tool_cache_from_tool_references(self, sample_module_info: ModuleInfo) -> None:
        """Test building tool cache from resolved tool references."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        # Create setup with resolved tool reference
        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, fixed_id="tool-123"),
            selected_module_id="tool-123",
        )
        tool_ref._cached_info = sample_module_info

        setup = TestSetup(my_tool=tool_ref)
        cache = setup.build_tool_cache()

        # Should use fixed_id as slug (since no explicit slug)
        assert cache.contains("tool-123")
        assert cache.get("tool-123") == sample_module_info

    def test_build_tool_cache_uses_explicit_slug(self, sample_module_info: ModuleInfo) -> None:
        """Test that explicit slug is used when provided."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                slug="custom-slug",
                fixed_id="tool-123",
            ),
            selected_module_id="tool-123",
        )
        tool_ref._cached_info = sample_module_info

        setup = TestSetup(my_tool=tool_ref)
        cache = setup.build_tool_cache()

        assert cache.contains("custom-slug")
        assert not cache.contains("tool-123")

    def test_build_tool_cache_skips_unresolved(self) -> None:
        """Test that unresolved tool references are not cached."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
        )

        setup = TestSetup(my_tool=tool_ref)
        cache = setup.build_tool_cache()

        assert len(cache.entries) == 0

    def test_tool_cache_property(self, sample_module_info: ModuleInfo) -> None:
        """Test accessing tool cache via property."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, fixed_id="tool-123"),
            selected_module_id="tool-123",
        )
        tool_ref._cached_info = sample_module_info

        setup = TestSetup(my_tool=tool_ref)
        setup.build_tool_cache()

        assert setup.tool_cache.contains("tool-123")

    def test_validate_tool_cache(self, sample_module_info: ModuleInfo) -> None:
        """Test validating tool cache via setup model."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, fixed_id="tool-123"),
            selected_module_id="tool-123",
        )
        tool_ref._cached_info = sample_module_info

        setup = TestSetup(my_tool=tool_ref)
        setup.build_tool_cache()

        mock_registry = Mock()
        mock_registry.discover_by_id.return_value = None  # Tool no longer available

        invalid = setup.validate_tool_cache(mock_registry)

        assert "tool-123" in invalid


class TestToolReferenceSlug:
    """Tests for ToolReference slug property."""

    def test_slug_from_config(self) -> None:
        """Test slug is taken from config when provided."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                slug="my-custom-slug",
                fixed_id="tool-123",
            ),
        )
        assert tool_ref.slug == "my-custom-slug"

    def test_slug_fallback_to_fixed_id(self) -> None:
        """Test slug falls back to fixed_id in FIXED mode."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                fixed_id="tool-123",
            ),
        )
        assert tool_ref.slug == "tool-123"

    def test_slug_fallback_to_tag(self) -> None:
        """Test slug falls back to tag in TAG mode."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.TAG,
                tag="search-tool",
            ),
        )
        assert tool_ref.slug == "search-tool"

    def test_slug_none_for_discoverable(self) -> None:
        """Test slug is None for DISCOVERABLE mode without explicit slug."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
        )
        assert tool_ref.slug is None
