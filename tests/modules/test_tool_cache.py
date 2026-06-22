"""Tests for ToolCache functionality."""

from unittest.mock import AsyncMock

import pytest

from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_cache import ToolCache, ToolDefinition, ToolModuleInfo
from digitalkin.models.module.tool_reference import ToolReference, ToolSelection
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
                parameters_schema={
                    "type": "object",
                    "properties": {"query": {"type": "string", "description": "Search query"}},
                    "required": ["query"],
                },
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
                parameters_schema={
                    "type": "object",
                    "properties": {"data": {"type": "string", "description": "Data to analyze"}},
                    "required": ["data"],
                },
            ),
        ],
    )


class TestToolCache:
    """Tests for ToolCache."""

    def test_add_and_get(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test adding and getting a tool."""
        cache = ToolCache()
        cache.add(sample_tool_module_info)

        assert cache.get(sample_tool_module_info.setup_id) == sample_tool_module_info

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
        assert sample_tool_module_info.setup_id in tools
        assert sample_tool_module_info_2.setup_id in tools

    def test_get_returns_cached_value(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test get returns cached value."""
        cache = ToolCache()
        cache.add(sample_tool_module_info)

        result = cache.get(sample_tool_module_info.setup_id)
        assert result == sample_tool_module_info

    def test_get_without_cache_returns_none(self) -> None:
        """Test get returns None if not cached."""
        cache = ToolCache()
        result = cache.get("nonexistent")
        assert result is None


class TestSetupModelToolCache:
    """Tests for SetupModel tool cache integration."""

    @pytest.mark.asyncio
    async def test_build_tool_cache_from_resolved_tools(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test building tool cache from resolved tool references."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        # Create setup with tool reference
        tool_ref = ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})])

        setup = TestSetup(my_tool=tool_ref)
        # Pre-populate resolved_tools using setup_id as key (matches caching logic)
        setup.resolved_tools["setup-123"] = sample_tool_module_info
        cache = await setup.build_tool_cache()

        assert sample_tool_module_info.setup_id in cache.entries
        assert cache.entries[sample_tool_module_info.setup_id] == sample_tool_module_info

    @pytest.mark.asyncio
    async def test_build_tool_cache_skips_unresolved(self) -> None:
        """Test that unresolved tool references are not cached."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=[])

        setup = TestSetup(my_tool=tool_ref)
        cache = await setup.build_tool_cache()

        assert len(cache.entries) == 0

    @pytest.mark.asyncio
    async def test_resolved_tools_populated(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test resolved_tools dict is populated after build_tool_cache."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})])

        setup = TestSetup(my_tool=tool_ref)
        # Pre-populate resolved_tools using setup_id as key
        setup.resolved_tools["setup-123"] = sample_tool_module_info
        cache = await setup.build_tool_cache()

        assert "setup-123" in setup.resolved_tools
        assert setup.resolved_tools["setup-123"] == sample_tool_module_info
        assert cache.entries[sample_tool_module_info.setup_id] == sample_tool_module_info


class TestResolvedToolsField:
    """Tests for resolved_tools field on SetupModel."""

    def test_resolved_tools_field_exists(self) -> None:
        """Test resolved_tools field exists on SetupModel subclass."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        assert "resolved_tools" in TestSetup.model_fields
        field_info = TestSetup.model_fields["resolved_tools"]
        assert field_info.json_schema_extra == {"ui:widget": "hidden"}

    def test_resolved_tools_default_empty(self) -> None:
        """Test resolved_tools defaults to empty dict."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=[])
        setup = TestSetup(my_tool=tool_ref)
        assert setup.resolved_tools == {}

    @pytest.mark.asyncio
    async def test_multiple_tool_references_in_resolved_tools(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """Test multiple resolved tools stored in resolved_tools dict."""

        class TestSetup(SetupModel):
            tool_a: ToolReference
            tool_b: ToolReference

        tool_ref_a = ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})])
        tool_ref_b = ToolReference(selected_tools=[ToolSelection(setup_id="setup-456", triggers={"analyze": True})])

        setup = TestSetup(tool_a=tool_ref_a, tool_b=tool_ref_b)
        # Pre-populate resolved_tools using setup_id as key
        setup.resolved_tools["setup-123"] = sample_tool_module_info
        setup.resolved_tools["setup-456"] = sample_tool_module_info_2
        cache = await setup.build_tool_cache()

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


def _registry_resolving(setup_id: str, module_id: str, name: str) -> AsyncMock:
    """Mock registry that resolves ``setup_id`` to a module with one ``search`` trigger."""
    registry = AsyncMock()
    registry.get_setup.return_value = SetupInfo(setup_id=setup_id, name=name, module_id=module_id)
    registry.discover_by_id.return_value = ModuleInfo(
        module_id=module_id,
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50051,
        version="1.0.0",
        module_name=name,
    )
    return registry


def _communication_with_search() -> AsyncMock:
    """Mock communication whose module exposes a single ``search`` protocol."""
    comm = AsyncMock()
    comm.get_module_schemas.return_value = {
        "input": {
            "json_schema": {
                "$defs": {
                    "SearchInput": {
                        "properties": {
                            "protocol": {"const": "search"},
                            "query": {"type": "string"},
                        },
                        "required": ["protocol", "query"],
                    },
                },
            },
        },
    }
    return comm


class TestResolvedToolsNotPersisted:
    """The stale-resolution fix: resolved_tools is runtime-only and never trusted across builds."""

    @pytest.mark.asyncio
    async def test_build_with_registry_ignores_stale_resolved_tools(self) -> None:
        """A pre-populated empty entry must be discarded and re-resolved when a registry is present."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})])
        setup = TestSetup(my_tool=tool_ref)
        # Stale empty entry, as would be loaded from persisted content.
        setup.resolved_tools["setup-123"] = ToolModuleInfo(
            module_id="tool-123",
            module_type=RegistryModuleType.TOOL,
            address="localhost",
            port=50051,
            version="1.0.0",
            module_name="TestTool",
            setup_id="setup-123",
            tool_name="TestTool",
            tools=[],
        )

        registry = _registry_resolving("setup-123", "tool-123", "TestTool")
        communication = _communication_with_search()
        cache = await setup.build_tool_cache(registry, communication)

        # Fresh resolution ran: the stale empty entry was discarded.
        assert "setup-123" in cache.entries
        assert [t.name for t in cache.entries["setup-123"].tools] == ["search"]
        registry.get_setup.assert_awaited()

    @pytest.mark.asyncio
    async def test_resolved_tools_excluded_from_model_dump(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """resolved_tools is runtime state and must never serialize into persisted content."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        setup = TestSetup(my_tool=ToolReference(selected_tools=[]))
        setup.resolved_tools["setup-123"] = sample_tool_module_info

        assert "resolved_tools" not in setup.model_dump()
        assert "resolved_tools" not in setup.model_dump(mode="json")

    @pytest.mark.asyncio
    async def test_resolved_tools_not_reloaded_from_content(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Round-trip: dumped content carries no resolved_tools, so a reload starts empty."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        setup = TestSetup(my_tool=ToolReference(selected_tools=[]))
        setup.resolved_tools["setup-123"] = sample_tool_module_info

        content = setup.model_dump(mode="json")
        reloaded = TestSetup(**content)
        assert reloaded.resolved_tools == {}


class TestToolReferenceSelectedTools:
    """Tests for ToolReference selected_tools property."""

    def test_selected_tools_with_setup_id(self) -> None:
        """Test selected_tools is set correctly."""
        tool_ref = ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})])
        assert len(tool_ref.selected_tools) == 1
        assert tool_ref.selected_tools[0].setup_id == "setup-123"

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

        tool_ref = ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})])
        setup = TestSetup(my_tool=tool_ref)

        mock_registry = AsyncMock()
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

        mock_communication = _communication_with_search()

        await setup.build_tool_cache(mock_registry, mock_communication)

        mock_registry.get_setup.assert_called_once_with("setup-123")
        mock_registry.discover_by_id.assert_called_once_with("tool-123")
        assert len(setup.resolved_tools) == 1

    @pytest.mark.asyncio
    async def test_second_build_with_registry_reresolves(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """With a registry present, every build re-resolves — resolved_tools is NOT a cross-request cache.

        Cross-request efficiency is the servicer-level ``_tool_cache_by_setup`` TTL cache's job;
        ``resolved_tools`` must never short-circuit a fresh build, or stale/empty entries get frozen.
        """

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})])
        setup = TestSetup(my_tool=tool_ref)

        mock_registry = _registry_resolving("setup-123", "tool-123", "TestTool")
        mock_communication = _communication_with_search()

        await setup.build_tool_cache(mock_registry, mock_communication)
        assert mock_registry.get_setup.call_count == 1

        # Second build re-resolves (cache cleared at build start).
        await setup.build_tool_cache(mock_registry, mock_communication)
        assert mock_registry.get_setup.call_count == 2
        assert len(setup.resolved_tools) == 1

    @pytest.mark.asyncio
    async def test_serialization_drops_resolved_tools(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """resolved_tools must NOT survive JSON serialization (it's runtime state, not config)."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        tool_ref = ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})])
        setup = TestSetup(my_tool=tool_ref)
        setup.resolved_tools["setup-123"] = sample_tool_module_info

        json_data = setup.model_dump_json()
        assert "resolved_tools" not in json_data

        restored_setup = TestSetup.model_validate_json(json_data)
        assert restored_setup.resolved_tools == {}

        # A reloaded setup re-resolves from the registry (no frozen cache).
        mock_registry = _registry_resolving("setup-123", "tool-123", "TestTool")
        mock_communication = _communication_with_search()
        await restored_setup.build_tool_cache(mock_registry, mock_communication)
        mock_registry.get_setup.assert_awaited_once_with("setup-123")

    @pytest.mark.asyncio
    async def test_multiple_tools_reresolve_each_build(
        self, sample_tool_module_info: ToolModuleInfo, sample_tool_module_info_2: ToolModuleInfo
    ) -> None:
        """With a registry, both tools re-resolve on every build (no cross-request reuse)."""

        class TestSetup(SetupModel):
            tool_a: ToolReference
            tool_b: ToolReference

        setup = TestSetup(
            tool_a=ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})]),
            tool_b=ToolReference(selected_tools=[ToolSelection(setup_id="setup-456", triggers={"search": True})]),
        )

        mock_registry = AsyncMock()
        mock_registry.get_setup.side_effect = lambda setup_id: (
            SetupInfo(setup_id="setup-123", name="Tool A", module_id="tool-123")
            if setup_id == "setup-123"
            else SetupInfo(setup_id="setup-456", name="Tool B", module_id="tool-456")
        )
        mock_registry.discover_by_id.side_effect = lambda module_id: ModuleInfo(
            module_id=module_id,
            module_type=RegistryModuleType.TOOL,
            address="localhost",
            port=50051,
            version="1.0.0",
            module_name=module_id,
        )
        mock_communication = _communication_with_search()

        await setup.build_tool_cache(mock_registry, mock_communication)
        assert mock_registry.get_setup.call_count == 2
        assert len(setup.resolved_tools) == 2

        # Second build re-resolves both (cache cleared, not reused).
        mock_registry.reset_mock()
        await setup.build_tool_cache(mock_registry, mock_communication)
        assert mock_registry.get_setup.call_count == 2
        assert len(setup.resolved_tools) == 2

    @pytest.mark.asyncio
    async def test_no_registry_keeps_prepopulated_resolved_tools(
        self, sample_tool_module_info: ToolModuleInfo
    ) -> None:
        """Embedded/degraded path: with no registry, a pre-populated entry is kept and served."""

        class TestSetup(SetupModel):
            my_tool: ToolReference

        setup = TestSetup(
            my_tool=ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})]),
        )
        setup.resolved_tools["setup-123"] = sample_tool_module_info

        # No registry/communication → resolved_tools is NOT cleared, entry is reused.
        cache = await setup.build_tool_cache()
        assert "setup-123" in setup.resolved_tools
        assert cache.entries["setup-123"] == sample_tool_module_info


class TestSlugify:
    """Tests for ToolModuleInfo._slugify and slug property."""

    def test_slugify_simple_name(self) -> None:
        """Test slugify with a simple name."""
        assert ToolModuleInfo._slugify("Google Search") == "google_search"

    def test_slugify_already_lowercase(self) -> None:
        """Test slugify with already lowercase name."""
        assert ToolModuleInfo._slugify("my_tool") == "my_tool"

    def test_slugify_special_characters(self) -> None:
        """Test slugify strips special characters."""
        assert ToolModuleInfo._slugify("Tool (v2.0)") == "tool_v2_0"

    def test_slugify_multiple_spaces(self) -> None:
        """Test slugify collapses multiple spaces."""
        assert ToolModuleInfo._slugify("My   Tool   Name") == "my_tool_name"

    def test_slugify_leading_trailing(self) -> None:
        """Test slugify strips leading/trailing underscores."""
        assert ToolModuleInfo._slugify("  Tool  ") == "tool"

    def test_slugify_camelcase(self) -> None:
        """Test slugify lowercases CamelCase."""
        assert ToolModuleInfo._slugify("DuckDuckGo") == "duckduckgo"

    def test_slug_property_uses_tool_name(self) -> None:
        """Test slug property returns slugified tool_name."""
        info = ToolModuleInfo(
            module_id="m1",
            setup_id="setup-abc",
            tool_name="Google Search",
        )
        assert info.slug == "google_search"

    def test_slug_no_setup_id(self) -> None:
        """Test slug does not contain setup_id."""
        info = ToolModuleInfo(
            module_id="m1",
            setup_id="setup-abc-123",
            tool_name="My Tool",
        )
        assert "setup" not in info.slug
        assert info.slug == "my_tool"


class TestToolCacheCollision:
    """Tests for ToolCache setup_id-based keying."""

    def test_different_setup_ids_coexist(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test that tools with different setup_ids coexist even with same tool_name."""
        cache = ToolCache()
        cache.add(sample_tool_module_info)

        other = ToolModuleInfo(
            module_id="tool-other",
            setup_id="setup-other",
            tool_name="TestTool",
            tools=[],
        )
        cache.add(other)

        assert cache.get("setup-123") == sample_tool_module_info
        assert cache.get("setup-other") == other
        assert len(cache.entries) == 2

    def test_same_setup_id_overwrites(self, sample_tool_module_info: ToolModuleInfo) -> None:
        """Test that re-adding same setup_id overwrites the entry."""
        cache = ToolCache()
        cache.add(sample_tool_module_info)
        cache.add(sample_tool_module_info)

        assert cache.get("setup-123") == sample_tool_module_info
        assert len(cache.entries) == 1
