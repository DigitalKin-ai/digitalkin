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

    def test_clear(self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo) -> None:
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

    def test_resolved_tools_populated(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test resolved_tools dict is populated after build_tool_cache."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123"),
        )
        tool_ref._cached_info = sample_tool_module_info

        setup = TestSetup(my_tool=tool_ref)
        cache = setup.build_tool_cache()

        # resolved_tools uses setup_id as key, cache uses module_id
        assert "setup-123" in setup.resolved_tools
        assert setup.resolved_tools["setup-123"] == sample_tool_module_info
        assert cache.entries.get("tool-123") == sample_tool_module_info


class TestResolvedToolsField:
    """Tests for resolved_tools field on SetupModel."""

    def test_resolved_tools_field_exists(self) -> None:
        """Test resolved_tools field exists on SetupModel subclass."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        assert "resolved_tools" in TestSetup.model_fields
        field_info = TestSetup.model_fields["resolved_tools"]
        assert field_info.json_schema_extra == {"hidden": True}

    def test_resolved_tools_default_empty(self) -> None:
        """Test resolved_tools defaults to empty dict."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
        )
        setup = TestSetup(my_tool=tool_ref)
        assert setup.resolved_tools == {}

    def test_multiple_tool_references_in_resolved_tools(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test multiple resolved tools stored in resolved_tools dict."""

        class TestSetup(SetupModel):
            tool_a: ToolReference
            tool_b: ToolReference

        tool_ref_a = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123"),
        )
        tool_ref_a._cached_info = sample_tool_module_info

        tool_ref_b = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-456"),
        )
        tool_ref_b._cached_info = sample_tool_module_info_2

        setup = TestSetup(tool_a=tool_ref_a, tool_b=tool_ref_b)
        cache = setup.build_tool_cache()

        # resolved_tools uses setup_id as key
        assert len(setup.resolved_tools) == 2
        assert setup.resolved_tools["setup-123"] == sample_tool_module_info
        assert setup.resolved_tools["setup-456"] == sample_tool_module_info_2
        assert len(cache.entries) == 2

    def test_non_tool_reference_fields_unchanged(self) -> None:
        """Test non-ToolReference fields work normally."""

        class TestSetup(SetupModel):
            name: str = "test"

        setup = TestSetup()
        assert setup.name == "test"
        assert setup.resolved_tools == {}


class TestToolReferenceSetupId:
    """Tests for ToolReference setup_id property."""

    def test_setup_id_in_fixed_mode(self) -> None:
        """Test setup_id is set in FIXED mode."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                setup_id="setup-123",
            ),
        )
        assert tool_ref.setup_id == "setup-123"
        assert tool_ref.slug == "setup-123"

    def test_setup_id_empty_in_tag_mode_before_resolution(self) -> None:
        """Test setup_id is empty in TAG mode before resolution."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.TAG,
                tag="search-tool",
            ),
        )
        assert not tool_ref.setup_id
        assert not tool_ref.slug

    def test_setup_id_empty_for_discoverable(self) -> None:
        """Test setup_id is empty for DISCOVERABLE mode."""
        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
        )
        assert not tool_ref.setup_id
        assert not tool_ref.slug


class TestResolvedToolsCacheBehavior:
    """Tests for resolved_tools cache preventing unnecessary registry calls."""

    @pytest.mark.asyncio
    async def test_first_resolution_calls_registry(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test first resolve_tool_references calls the registry."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123"),
        )
        setup = TestSetup(my_tool=tool_ref)

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
            "input": {"json_schema": {"$defs": {}}},
        }

        await setup.resolve_tool_references(mock_registry, mock_communication)

        mock_registry.get_setup.assert_called_once_with("setup-123")
        mock_registry.discover_by_id.assert_called_once_with("tool-123")
        assert setup.my_tool.tool_module_info is not None
        assert setup.my_tool.tool_module_info.setup_id == "setup-123"

    @pytest.mark.asyncio
    async def test_second_resolution_uses_cache_skips_registry(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test second resolve_tool_references uses cache, does not call registry."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123"),
        )
        setup = TestSetup(my_tool=tool_ref)

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
            "input": {"json_schema": {"$defs": {}}},
        }

        # First resolution - registry called
        await setup.resolve_tool_references(mock_registry, mock_communication)
        assert mock_registry.get_setup.call_count == 1

        # Simulate serialization: _cached_info is lost but resolved_tools persists
        setup.my_tool._cached_info = None

        # Second resolution - should use cache, not registry
        await setup.resolve_tool_references(mock_registry, mock_communication)

        # Registry still only called once (from first resolution)
        assert mock_registry.get_setup.call_count == 1
        # But tool_ref should be resolved from cache
        assert setup.my_tool.tool_module_info is not None

    @pytest.mark.asyncio
    async def test_serialization_preserves_resolved_tools(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test resolved_tools survives JSON serialization while _cached_info is lost."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123"),
        )
        setup = TestSetup(my_tool=tool_ref)

        # Manually set resolved state
        setup.my_tool._cached_info = sample_tool_module_info
        setup.resolved_tools["setup-123"] = sample_tool_module_info

        # Serialize and deserialize
        json_data = setup.model_dump_json()
        restored_setup = TestSetup.model_validate_json(json_data)

        # resolved_tools persists, _cached_info is lost
        assert "setup-123" in restored_setup.resolved_tools
        assert restored_setup.resolved_tools["setup-123"] == sample_tool_module_info
        assert restored_setup.my_tool._cached_info is None

        # Second resolution uses cache, registry not called
        mock_registry = Mock()
        mock_communication = AsyncMock()

        await restored_setup.resolve_tool_references(mock_registry, mock_communication)

        mock_registry.get_setup.assert_not_called()
        mock_registry.discover_by_id.assert_not_called()
        assert restored_setup.my_tool.tool_module_info == sample_tool_module_info

    @pytest.mark.asyncio
    async def test_multiple_tools_cache_behavior(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test cache behavior with multiple tools."""

        class TestSetup(SetupModel):
            tool_a: ToolReference
            tool_b: ToolReference

        setup = TestSetup(
            tool_a=ToolReference(
                config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123"),
            ),
            tool_b=ToolReference(
                config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-456"),
            ),
        )

        mock_registry = Mock()
        mock_registry.get_setup.side_effect = lambda setup_id: (
            SetupInfo(setup_id="setup-123", name="Tool A", module_id="tool-123")
            if setup_id == "setup-123"
            else SetupInfo(setup_id="setup-456", name="Tool B", module_id="tool-456")
        )
        mock_registry.discover_by_id.side_effect = lambda module_id: (
            ModuleInfo(
                module_id="tool-123",
                module_type=RegistryModuleType.TOOL,
                address="localhost",
                port=50051,
                version="1.0.0",
                name="ToolA",
                documentation="Tool A",
            )
            if module_id == "tool-123"
            else ModuleInfo(
                module_id="tool-456",
                module_type=RegistryModuleType.TOOL,
                address="localhost",
                port=50052,
                version="1.0.0",
                name="ToolB",
                documentation="Tool B",
            )
        )

        mock_communication = AsyncMock()
        mock_communication.get_module_schemas.return_value = {
            "input": {"json_schema": {"$defs": {}}},
        }

        # First resolution - both tools resolved via registry
        await setup.resolve_tool_references(mock_registry, mock_communication)

        assert mock_registry.get_setup.call_count == 2
        assert "setup-123" in setup.resolved_tools
        assert "setup-456" in setup.resolved_tools

        # Clear _cached_info to simulate deserialization
        setup.tool_a._cached_info = None
        setup.tool_b._cached_info = None

        # Second resolution - both tools resolved from cache
        mock_registry.reset_mock()
        await setup.resolve_tool_references(mock_registry, mock_communication)

        mock_registry.get_setup.assert_not_called()
        mock_registry.discover_by_id.assert_not_called()
        assert setup.tool_a.tool_module_info is not None
        assert setup.tool_b.tool_module_info is not None

    @pytest.mark.asyncio
    async def test_partial_cache_only_queries_missing(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test that only uncached tools trigger registry calls."""

        class TestSetup(SetupModel):
            tool_a: ToolReference
            tool_b: ToolReference

        setup = TestSetup(
            tool_a=ToolReference(
                config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123"),
            ),
            tool_b=ToolReference(
                config=ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-456"),
            ),
        )

        # Pre-populate cache with only tool_a
        setup.resolved_tools["setup-123"] = sample_tool_module_info

        mock_registry = Mock()
        mock_registry.get_setup.return_value = SetupInfo(
            setup_id="setup-456",
            name="Tool B",
            module_id="tool-456",
        )
        mock_registry.discover_by_id.return_value = ModuleInfo(
            module_id="tool-456",
            module_type=RegistryModuleType.TOOL,
            address="localhost",
            port=50052,
            version="1.0.0",
            name="ToolB",
            documentation="Tool B",
        )

        mock_communication = AsyncMock()
        mock_communication.get_module_schemas.return_value = {
            "input": {"json_schema": {"$defs": {}}},
        }

        await setup.resolve_tool_references(mock_registry, mock_communication)

        # Only tool_b should trigger registry call
        mock_registry.get_setup.assert_called_once_with("setup-456")
        assert setup.tool_a.tool_module_info == sample_tool_module_info
        assert setup.tool_b.tool_module_info is not None
