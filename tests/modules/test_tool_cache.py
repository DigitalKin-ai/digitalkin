"""Tests for ToolCache functionality."""

from unittest.mock import AsyncMock, Mock

import pytest

from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_cache import ToolCache, ToolDefinition, ToolModuleInfo, ToolParameter
from digitalkin.models.module.tool_reference import ToolReference
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
        module_name="TestTool",
        documentation="Test tool documentation",
        setup_id="setup-123",
        tool_name="TestTool",
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
        module_name="AnotherTool",
        documentation="Another test tool",
        setup_id="setup-456",
        tool_name="AnotherTool",
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
        cache.add(sample_tool_module_info)

        # Access via slug (setup_id + "_" + tool_name)
        slug = sample_tool_module_info.slug
        assert cache.get(slug) == sample_tool_module_info

    def test_get_nonexistent_returns_none(self) -> None:
        """Test getting a nonexistent tool returns None."""
        cache = ToolCache()
        assert cache.get("nonexistent") is None

    def test_clear(self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo) -> None:
        """Test clearing all tools."""
        cache = ToolCache()
        cache.add(sample_tool_module_info)
        cache.add(sample_tool_module_info_2)
        cache.clear()

        assert len(cache.entries) == 0

    def test_list_tools(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test listing tool names."""
        cache = ToolCache()
        cache.add(sample_tool_module_info)
        cache.add(sample_tool_module_info_2)

        tools = cache.list_tools()
        assert sample_tool_module_info.slug in tools
        assert sample_tool_module_info_2.slug in tools

    def test_get_returns_cached_value(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test get returns cached value."""
        cache = ToolCache()
        cache.add(sample_tool_module_info)

        result = cache.get(sample_tool_module_info.slug)
        assert result == sample_tool_module_info

    def test_get_without_cache_returns_none(self) -> None:
        """Test get returns None if not cached."""
        cache = ToolCache()
        result = cache.get("nonexistent")
        assert result is None


class TestSetupModelToolCache:
    """Tests for SetupModel tool cache integration."""

    def test_build_tool_cache_from_resolved_tools(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test building tool cache from resolved tool references."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        # Create setup with tool reference
        tool_ref = ToolReference(selected_tools=["setup-123"])

        setup = TestSetup(my_tool=tool_ref)
        # Pre-populate resolved_tools using setup_id as key (matches caching logic)
        setup.resolved_tools["setup-123"] = sample_tool_module_info
        cache = setup.build_tool_cache()

        # ToolCache uses ToolModuleInfo.slug as key
        assert sample_tool_module_info.slug in cache.entries
        assert cache.entries[sample_tool_module_info.slug] == sample_tool_module_info

    def test_build_tool_cache_skips_unresolved(self) -> None:
        """Test that unresolved tool references are not cached."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=[])

        setup = TestSetup(my_tool=tool_ref)
        cache = setup.build_tool_cache()

        assert len(cache.entries) == 0

    def test_resolved_tools_populated(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test resolved_tools dict is populated after build_tool_cache."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=["setup-123"])

        setup = TestSetup(my_tool=tool_ref)
        # Pre-populate resolved_tools using setup_id as key
        setup.resolved_tools["setup-123"] = sample_tool_module_info
        cache = setup.build_tool_cache()

        # resolved_tools uses setup_id as key, ToolCache uses slug
        assert "setup-123" in setup.resolved_tools
        assert setup.resolved_tools["setup-123"] == sample_tool_module_info
        assert cache.entries[sample_tool_module_info.slug] == sample_tool_module_info


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

        tool_ref = ToolReference(selected_tools=[])
        setup = TestSetup(my_tool=tool_ref)
        assert setup.resolved_tools == {}

    def test_multiple_tool_references_in_resolved_tools(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test multiple resolved tools stored in resolved_tools dict."""

        class TestSetup(SetupModel):
            tool_a: ToolReference
            tool_b: ToolReference

        tool_ref_a = ToolReference(selected_tools=["setup-123"])
        tool_ref_b = ToolReference(selected_tools=["setup-456"])

        setup = TestSetup(tool_a=tool_ref_a, tool_b=tool_ref_b)
        # Pre-populate resolved_tools using setup_id as key
        setup.resolved_tools["setup-123"] = sample_tool_module_info
        setup.resolved_tools["setup-456"] = sample_tool_module_info_2
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


class TestToolReferenceSelectedTools:
    """Tests for ToolReference selected_tools property."""

    def test_selected_tools_with_setup_id(self) -> None:
        """Test selected_tools is set correctly."""
        tool_ref = ToolReference(selected_tools=["setup-123"])
        assert len(tool_ref.selected_tools) == 1
        assert tool_ref.selected_tools[0] == "setup-123"

    def test_selected_tools_empty_by_default(self) -> None:
        """Test selected_tools is empty by default."""
        tool_ref = ToolReference()
        assert len(tool_ref.selected_tools) == 0


class TestResolvedToolsCacheBehavior:
    """Tests for resolved_tools cache preventing unnecessary registry calls."""

    @pytest.mark.asyncio
    async def test_first_resolution_calls_registry(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test first resolve_tool_references calls the registry."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=["setup-123"])
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
            module_name="TestTool",
            documentation="Test tool documentation",
        )

        mock_communication = AsyncMock()
        mock_communication.get_module_schemas.return_value = {
            "input": {"json_schema": {"$defs": {}}},
        }

        await setup.resolve_tool_references(mock_registry, mock_communication)

        mock_registry.get_setup.assert_called_once_with("setup-123")
        mock_registry.discover_by_id.assert_called_once_with("tool-123")
        assert len(setup.resolved_tools) == 1

    @pytest.mark.asyncio
    async def test_second_resolution_uses_cache_skips_registry(
        self, sample_tool_module_info: ToolModuleInfo
    ) -> None:
        """Test second resolve_tool_references uses cache, does not call registry."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=["setup-123"])
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
            module_name="TestTool",
            documentation="Test tool documentation",
        )

        mock_communication = AsyncMock()
        mock_communication.get_module_schemas.return_value = {
            "input": {"json_schema": {"$defs": {}}},
        }

        # First resolution - registry called
        await setup.resolve_tool_references(mock_registry, mock_communication)
        assert mock_registry.get_setup.call_count == 1

        # Second resolution - should use cache, not registry
        await setup.resolve_tool_references(mock_registry, mock_communication)

        # Registry still only called once (from first resolution)
        assert mock_registry.get_setup.call_count == 1
        # resolved_tools still has the info
        assert len(setup.resolved_tools) == 1

    @pytest.mark.asyncio
    async def test_serialization_preserves_resolved_tools(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test resolved_tools survives JSON serialization."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=["setup-123"])
        setup = TestSetup(my_tool=tool_ref)

        # Manually set resolved state using setup_id as key
        setup.resolved_tools["setup-123"] = sample_tool_module_info

        # Serialize and deserialize
        json_data = setup.model_dump_json()
        restored_setup = TestSetup.model_validate_json(json_data)

        # resolved_tools persists
        assert "setup-123" in restored_setup.resolved_tools
        assert restored_setup.resolved_tools["setup-123"] == sample_tool_module_info

        # Second resolution uses cache, registry not called
        mock_registry = Mock()
        mock_communication = AsyncMock()

        await restored_setup.resolve_tool_references(mock_registry, mock_communication)

        mock_registry.get_setup.assert_not_called()
        mock_registry.discover_by_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_tools_cache_behavior(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test cache behavior with multiple tools."""

        class TestSetup(SetupModel):
            tool_a: ToolReference
            tool_b: ToolReference

        setup = TestSetup(
            tool_a=ToolReference(selected_tools=["setup-123"]),
            tool_b=ToolReference(selected_tools=["setup-456"]),
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
                module_name="ToolA",
                documentation="Tool A",
            )
            if module_id == "tool-123"
            else ModuleInfo(
                module_id="tool-456",
                module_type=RegistryModuleType.TOOL,
                address="localhost",
                port=50052,
                version="1.0.0",
                module_name="ToolB",
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
        assert len(setup.resolved_tools) == 2

        # Second resolution - both tools resolved from cache
        mock_registry.reset_mock()
        await setup.resolve_tool_references(mock_registry, mock_communication)

        mock_registry.get_setup.assert_not_called()
        mock_registry.discover_by_id.assert_not_called()
        assert len(setup.resolved_tools) == 2

    @pytest.mark.asyncio
    async def test_partial_cache_only_queries_missing(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test that only uncached tools trigger registry calls."""

        class TestSetup(SetupModel):
            tool_a: ToolReference
            tool_b: ToolReference

        setup = TestSetup(
            tool_a=ToolReference(selected_tools=["setup-123"]),
            tool_b=ToolReference(selected_tools=["setup-456"]),
        )

        # Pre-populate cache with only tool_a using setup_id as key
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
            module_name="ToolB",
            documentation="Tool B",
        )

        mock_communication = AsyncMock()
        mock_communication.get_module_schemas.return_value = {
            "input": {"json_schema": {"$defs": {}}},
        }

        await setup.resolve_tool_references(mock_registry, mock_communication)

        # Only tool_b should trigger registry call
        mock_registry.get_setup.assert_called_once_with("setup-456")
        assert "setup-123" in setup.resolved_tools
        assert len(setup.resolved_tools) == 2
