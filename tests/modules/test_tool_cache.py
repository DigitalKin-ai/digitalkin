"""Tests for ToolCache functionality."""

from unittest.mock import Mock

import pytest

from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_cache import ToolCache
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


class TestToolCache:
    """Tests for ToolCache."""

    def test_add_and_get(self, sample_module_info: ModuleInfo) -> None:
        """Test adding and getting a tool."""
        cache = ToolCache()
        cache.add("my_tool", sample_module_info)

        result = cache.get("my_tool")
        assert result == sample_module_info

    def test_get_nonexistent_returns_none(self) -> None:
        """Test getting a nonexistent tool returns None."""
        cache = ToolCache()
        assert cache.get("nonexistent") is None

    def test_clear(
        self, sample_module_info: ModuleInfo, sample_module_info_2: ModuleInfo
    ) -> None:
        """Test clearing all tools."""
        cache = ToolCache()
        cache.add("tool1", sample_module_info)
        cache.add("tool2", sample_module_info_2)
        cache.clear()

        assert len(cache.entries) == 0

    def test_list_tools(
        self, sample_module_info: ModuleInfo, sample_module_info_2: ModuleInfo
    ) -> None:
        """Test listing tool names."""
        cache = ToolCache()
        cache.add("tool1", sample_module_info)
        cache.add("tool2", sample_module_info_2)

        tools = cache.list_tools()
        assert "tool1" in tools
        assert "tool2" in tools

    def test_get_with_registry_on_cache_hit(self, sample_module_info: ModuleInfo) -> None:
        """Test get returns cached value without querying registry."""
        cache = ToolCache()
        cache.add("my_tool", sample_module_info)

        mock_registry = Mock()
        result = cache.get("my_tool", registry=mock_registry)

        assert result == sample_module_info
        mock_registry.discover_by_id.assert_not_called()

    def test_get_with_registry_on_cache_miss(self, sample_module_info: ModuleInfo) -> None:
        """Test get queries registry on cache miss."""
        cache = ToolCache()
        mock_registry = Mock()
        mock_registry.discover_by_id.return_value = sample_module_info

        result = cache.get("tool-123", registry=mock_registry)

        assert result == sample_module_info
        mock_registry.discover_by_id.assert_called_once_with("tool-123")
        # Should be cached now
        assert cache.get("tool-123") == sample_module_info

    def test_get_without_registry_returns_none(self) -> None:
        """Test get returns None if no registry and not cached."""
        cache = ToolCache()
        result = cache.get("nonexistent")
        assert result is None


class TestSetupModelToolCache:
    """Tests for SetupModel tool cache integration."""

    def test_build_tool_cache_from_tool_references(self, sample_module_info: ModuleInfo) -> None:
        """Test building tool cache from resolved tool references."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        # Create setup with resolved tool reference
        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, module_id="tool-123"),
        )
        tool_ref._cached_info = sample_module_info

        setup = TestSetup(my_tool=tool_ref)
        cache = setup.build_tool_cache()

        # module_id is used as cache key
        assert "tool-123" in cache.entries
        assert cache.get("tool-123") == sample_module_info

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

    def test_companion_field_populated(self, sample_module_info: ModuleInfo) -> None:
        """Test companion field is populated after build_tool_cache."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, module_id="tool-123"),
        )
        tool_ref._cached_info = sample_module_info

        setup = TestSetup(my_tool=tool_ref)
        cache = setup.build_tool_cache()

        assert setup.my_tool_cache == sample_module_info
        assert cache.get("tool-123") == sample_module_info


class TestCompanionFieldGeneration:
    """Tests for automatic companion field generation."""

    def test_companion_field_generated(self) -> None:
        """Test companion field is auto-generated for ToolReference."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        assert "my_tool_cache" in TestSetup.model_fields
        field_info = TestSetup.model_fields["my_tool_cache"]
        assert field_info.json_schema_extra == {"hidden": True}

    def test_companion_field_default_none(self) -> None:
        """Test companion field defaults to None."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
        )
        setup = TestSetup(my_tool=tool_ref)
        assert setup.my_tool_cache is None

    def test_optional_tool_reference_generates_companion(self) -> None:
        """Test Optional[ToolReference] also generates companion field."""

        class TestSetup(SetupModel):
            optional_tool: ToolReference | None = None

        assert "optional_tool_cache" in TestSetup.model_fields

    def test_multiple_tool_references(self) -> None:
        """Test multiple ToolReference fields each get companion fields."""

        class TestSetup(SetupModel):
            tool_a: ToolReference
            tool_b: ToolReference

        assert "tool_a_cache" in TestSetup.model_fields
        assert "tool_b_cache" in TestSetup.model_fields

    def test_non_tool_reference_no_companion(self) -> None:
        """Test non-ToolReference fields don't generate companions."""

        class TestSetup(SetupModel):
            name: str = "test"

        assert "name_cache" not in TestSetup.model_fields


class TestToolReferenceModuleId:
    """Tests for ToolReference module_id property."""

    def test_module_id_in_fixed_mode(self) -> None:
        """Test module_id is set in FIXED mode."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                module_id="tool-123",
            ),
        )
        assert tool_ref.module_id == "tool-123"
        assert tool_ref.slug == "tool-123"

    def test_module_id_none_in_tag_mode_before_resolution(self) -> None:
        """Test module_id is None in TAG mode before resolution."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.TAG,
                tag="search-tool",
            ),
        )
        assert tool_ref.module_id is None
        assert tool_ref.slug is None

    def test_module_id_none_for_discoverable(self) -> None:
        """Test module_id is None for DISCOVERABLE mode."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
        )
        assert tool_ref.module_id is None
        assert tool_ref.slug is None
