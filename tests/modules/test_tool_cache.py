"""Tests for ToolCache functionality."""

from unittest.mock import AsyncMock, Mock

import pytest

from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_cache import ToolCache, ToolDefinition, ToolModuleInfo, ToolParameter
from digitalkin.models.module.tool_reference import ToolReference, ToolReferenceConfig, ToolSelectionMode
from digitalkin.models.services.registry import ModuleInfo, RegistryModuleType, SetupInfo


@pytest.fixture
def sample_tool_module_info() -> ToolModuleInfo:
    """Create a sample ToolModuleInfo for testing."""
    return ToolModuleInfo(
        module_id="tool-123",
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50051,
        version="1.0.0",
        name="TestTool",
        documentation="Test tool documentation",
        setup_id="setup-123",
        tools=[
            ToolDefinition(
                name="search",
                description="Search for items",
                parameters=[
                    ToolParameter(name="query", type="string", description="Search query", required=True),
                ],
            ),
        ],
    )


@pytest.fixture
def sample_tool_module_info_2() -> ToolModuleInfo:
    """Create a second sample ToolModuleInfo for testing."""
    return ToolModuleInfo(
        module_id="tool-456",
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50052,
        version="2.0.0",
        name="AnotherTool",
        documentation="Another test tool",
        setup_id="setup-456",
        tools=[
            ToolDefinition(
                name="analyze",
                description="Analyze data",
                parameters=[
                    ToolParameter(name="data", type="string", description="Data to analyze", required=True),
                ],
            ),
        ],
    )


class TestToolCache:
    """Tests for ToolCache."""

    def test_add_and_get(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test adding and getting a tool."""
        cache = ToolCache()
        cache.add("my_tool", sample_tool_module_info)

        # Direct access from entries (sync)
        assert cache.entries.get("my_tool") == sample_tool_module_info

    @pytest.mark.asyncio
    async def test_get_nonexistent_returns_none(self) -> None:
        """Test getting a nonexistent tool returns None."""
        cache = ToolCache()
        assert await cache.get("nonexistent") is None

    def test_clear(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test clearing all tools."""
        cache = ToolCache()
        cache.add("tool1", sample_tool_module_info)
        cache.add("tool2", sample_tool_module_info_2)
        cache.clear()

        assert len(cache.entries) == 0

    def test_list_tools(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test listing tool names."""
        cache = ToolCache()
        cache.add("tool1", sample_tool_module_info)
        cache.add("tool2", sample_tool_module_info_2)

        tools = cache.list_tools()
        assert "tool1" in tools
        assert "tool2" in tools

    @pytest.mark.asyncio
    async def test_get_with_registry_on_cache_hit(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test get returns cached value without querying registry."""
        cache = ToolCache()
        cache.add("my_tool", sample_tool_module_info)

        mock_registry = Mock()
        mock_communication = AsyncMock()
        result = await cache.get("my_tool", registry=mock_registry, communication=mock_communication)

        assert result == sample_tool_module_info
        mock_registry.get_setup.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_with_registry_on_cache_miss(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test get queries registry on cache miss."""
        cache = ToolCache()

        mock_registry = Mock()
        mock_registry.get_setup.return_value = SetupInfo(
            setup_id="setup-123",
            name="Test Setup",
            module_id="tool-123",
        )
        mock_registry.discover_by_id.return_value = ModuleInfo(
            module_id="tool-123",
            module_type=RegistryModuleType.TOOL,
            address="localhost",
            port=50051,
            version="1.0.0",
            name="TestTool",
            documentation="Test tool documentation",
        )

        mock_communication = AsyncMock()
        mock_communication.get_module_schemas.return_value = {
            "input": {
                "json_schema": {
                    "$defs": {
                        "SearchInput": {
                            "properties": {
                                "protocol": {"const": "search"},
                                "query": {"type": "string", "description": "Search query"},
                            },
                            "required": ["protocol", "query"],
                            "description": "Search for items",
                        },
                    },
                },
            },
        }

        result = await cache.get("setup-123", registry=mock_registry, communication=mock_communication)

        assert result is not None
        assert result.module_id == "tool-123"
        mock_registry.get_setup.assert_called_once_with("setup-123")
        mock_registry.discover_by_id.assert_called_once_with("tool-123")
        # Should be cached now
        assert cache.entries.get("setup-123") is not None

    @pytest.mark.asyncio
    async def test_get_without_registry_returns_none(self) -> None:
        """Test get returns None if no registry and not cached."""
        cache = ToolCache()
        result = await cache.get("nonexistent")
        assert result is None


class TestSetupModelToolCache:
    """Tests for SetupModel tool cache integration."""

    def test_build_tool_cache_from_tool_references(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test building tool cache from resolved tool references."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        # Create setup with resolved tool reference
        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123"),
        )
        tool_ref._cached_info = sample_tool_module_info

        setup = TestSetup(my_tool=tool_ref)
        cache = setup.build_tool_cache()

        # module_id is used as cache key
        assert "tool-123" in cache.entries
        assert cache.entries.get("tool-123") == sample_tool_module_info

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

    def test_companion_field_populated(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test companion field is populated after build_tool_cache."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123"),
        )
        tool_ref._cached_info = sample_tool_module_info

        setup = TestSetup(my_tool=tool_ref)
        cache = setup.build_tool_cache()

        assert setup.my_tool_cache == sample_tool_module_info
        assert cache.entries.get("tool-123") == sample_tool_module_info


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
        """Test setup_id is set in FIXED mode."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                setup_id="setup-123",
            ),
        )
        assert tool_ref.setup_id == "setup-123"
        assert tool_ref.slug == "setup-123"

    def test_module_id_none_in_tag_mode_before_resolution(self) -> None:
        """Test module_id is empty in TAG mode before resolution."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.TAG,
                tag="search-tool",
            ),
        )
        assert tool_ref.module_id == ""
        assert tool_ref.slug == ""

    def test_module_id_none_for_discoverable(self) -> None:
        """Test module_id is empty for DISCOVERABLE mode."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
        )
        assert tool_ref.module_id == ""
        assert tool_ref.slug == ""
