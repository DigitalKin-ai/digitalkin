"""Tests for ToolReference resolution in SetupModel.

Tests the complete flow from ToolReference definition to resolution via registry,
including recursive resolution in nested structures.
"""

from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_cache import SelectedTool, ToolDefinition, ToolModuleInfo, ToolParameter
from digitalkin.models.module.tool_reference import ToolReference, tool_reference_input
from digitalkin.models.services.registry import (
    ModuleInfo,
    RegistryModuleStatus,
    RegistryModuleType,
    SetupInfo,
)
from digitalkin.services.registry import RegistryStrategy


class FakeRegistry(RegistryStrategy):
    """Fake registry for testing tool resolution."""

    def __init__(self, modules: dict[str, ModuleInfo] | None = None) -> None:
        self._modules = modules or {}
        self._setups: dict[str, SetupInfo] = {}
        self._search_results: dict[str, list[ModuleInfo]] = {}

    def add_module(self, info: ModuleInfo) -> None:
        self._modules[info.module_id] = info

    def add_setup(self, setup_id: str, module_id: str, name: str = "") -> None:
        self._setups[setup_id] = SetupInfo(
            setup_id=setup_id,
            name=name or f"Setup {setup_id}",
            module_id=module_id,
        )

    def add_search_result(self, tag: str, results: list[ModuleInfo]) -> None:
        self._search_results[tag] = results

    def discover_by_id(self, module_id: str) -> ModuleInfo | None:
        return self._modules.get(module_id)

    def get_setup(self, setup_id: str) -> SetupInfo | None:
        return self._setups.get(setup_id)

    def search(
        self,
        name: str | None = None,
        module_type: str | None = None,
        organization_id: str | None = None,
    ) -> list[ModuleInfo]:
        if name and name in self._search_results:
            return self._search_results[name]
        return []

    def get_status(self, module_id: str) -> None:
        return None

    def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
    ) -> ModuleInfo | None:
        return None

    def heartbeat(self, module_id: str) -> RegistryModuleStatus:
        return RegistryModuleStatus.ACTIVE


def create_mock_communication() -> AsyncMock:
    """Create a mock communication strategy that returns tool schemas."""
    mock = AsyncMock()
    mock.get_module_schemas.return_value = {
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
    return mock


def create_tool_module_info(
    module_id: str,
    name: str,
    port: int = 50051,
    setup_id: str = "",
    tool_name: str = "",
) -> ToolModuleInfo:
    """Create a ToolModuleInfo for testing."""
    return ToolModuleInfo(
        module_id=module_id,
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=port,
        version="1.0.0",
        module_name=name,
        setup_id=setup_id,
        tool_name=tool_name,
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
def search_tool_info() -> ModuleInfo:
    return ModuleInfo(
        module_id="tool-search-001",
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50051,
        version="1.0.0",
        module_name="SearchTool",
    )


@pytest.fixture
def analyzer_tool_info() -> ModuleInfo:
    return ModuleInfo(
        module_id="tool-analyzer-002",
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50052,
        version="2.0.0",
        module_name="AnalyzerTool",
    )


@pytest.fixture
def writer_tool_info() -> ModuleInfo:
    return ModuleInfo(
        module_id="tool-writer-003",
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50053,
        version="1.5.0",
        module_name="WriterTool",
    )


@pytest.fixture
def registry(
    search_tool_info: ModuleInfo,
    analyzer_tool_info: ModuleInfo,
    writer_tool_info: ModuleInfo,
) -> FakeRegistry:
    reg = FakeRegistry()
    reg.add_module(search_tool_info)
    reg.add_module(analyzer_tool_info)
    reg.add_module(writer_tool_info)
    # Add setup mappings
    reg.add_setup("setup-search-001", "tool-search-001", "SearchTool")
    reg.add_setup("setup-analyzer-002", "tool-analyzer-002", "AnalyzerTool")
    reg.add_setup("setup-writer-003", "tool-writer-003", "WriterTool")
    reg.add_search_result("search", [search_tool_info])
    reg.add_search_result("analyzer", [analyzer_tool_info])
    return reg


class TestToolReferenceValidation:
    """Tests for ToolReference validation."""

    def test_empty_tool_reference_is_valid(self) -> None:
        """Empty ToolReference is valid (represents 'no tools configured')."""
        ref = ToolReference()
        assert ref.selected_tools == []
        assert ref.setup_ids == []
        assert ref.module_ids == []
        assert ref.tags == []

    def test_setup_ids_only_valid(self) -> None:
        """ToolReference with only setup_ids is valid."""
        ref = ToolReference(setup_ids=["setup-123"])
        assert ref.setup_ids == ["setup-123"]
        assert ref.selected_tools == []

    def test_module_ids_only_valid(self) -> None:
        """ToolReference with only module_ids is valid."""
        ref = ToolReference(module_ids=["module-123"])
        assert ref.module_ids == ["module-123"]

    def test_tags_only_valid(self) -> None:
        """ToolReference with only tags is valid."""
        ref = ToolReference(tags=["search"])
        assert ref.tags == ["search"]

    def test_selected_tools_valid(self) -> None:
        """ToolReference with selected_tools is valid."""
        ref = ToolReference(
            selected_tools=[SelectedTool(setup_id="setup-123", slug="setup-123")]
        )
        assert len(ref.selected_tools) == 1
        assert ref.selected_tools[0].setup_id == "setup-123"

    def test_string_list_creates_selected_tools(self) -> None:
        """List of strings input creates selected_tools via tool_reference_input."""
        adapter = TypeAdapter(tool_reference_input())
        ref = adapter.validate_python(["setup-123", "setup-456"])
        assert len(ref.selected_tools) == 2
        assert ref.selected_tools[0].setup_id == "setup-123"
        assert ref.selected_tools[1].setup_id == "setup-456"

    def test_combined_constraints_valid(self) -> None:
        """ToolReference with multiple constraint types is valid."""
        ref = ToolReference(
            setup_ids=["setup-123"],
            module_ids=["module-456"],
            tags=["search"],
        )
        assert ref.setup_ids == ["setup-123"]
        assert ref.module_ids == ["module-456"]
        assert ref.tags == ["search"]


class TestToolReferenceResolution:
    """Tests for ToolReference.resolve() method."""

    @pytest.mark.asyncio
    async def test_selected_tools_resolve_by_setup_id(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """Selected tools resolve via registry lookup."""
        ref = ToolReference(
            selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
        )

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert len(result) == 1
        assert result[0].module_id == "tool-search-001"

    @pytest.mark.asyncio
    async def test_multiple_selected_tools_resolve(
        self,
        registry: FakeRegistry,
    ) -> None:
        """Multiple selected tools all resolve."""
        ref = ToolReference(
            selected_tools=[
                SelectedTool(setup_id="setup-search-001", slug="setup-search-001"),
                SelectedTool(setup_id="setup-analyzer-002", slug="setup-analyzer-002"),
            ]
        )

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert len(result) == 2
        module_ids = {r.module_id for r in result}
        assert "tool-search-001" in module_ids
        assert "tool-analyzer-002" in module_ids

    @pytest.mark.asyncio
    async def test_nonexistent_setup_returns_empty(self, registry: FakeRegistry) -> None:
        """Selected tool with nonexistent setup_id is not resolved."""
        ref = ToolReference(
            selected_tools=[SelectedTool(setup_id="nonexistent-setup", slug="nonexistent-setup")]
        )

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_empty_selected_tools_returns_empty(self, registry: FakeRegistry) -> None:
        """ToolReference with no selected_tools returns empty list."""
        ref = ToolReference(setup_ids=["setup-123"])

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert result == []

    @pytest.mark.asyncio
    async def test_constraint_only_returns_empty(self, registry: FakeRegistry) -> None:
        """ToolReference with only constraints (no selected_tools) returns empty."""
        ref = ToolReference(
            module_ids=["tool-search-001"],
            tags=["search"],
        )

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert result == []


class TestSetupModelToolResolution:
    """Tests for SetupModel.resolve_tool_references() method."""

    @pytest.mark.asyncio
    async def test_single_tool_reference_resolved(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """Single ToolReference field gets resolved."""

        class ArchetypeSetup(SetupModel):
            search_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
                ),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.resolved_tools) == 1
        tool_info = next(iter(setup.resolved_tools.values()))
        assert tool_info.module_id == "tool-search-001"

    @pytest.mark.asyncio
    async def test_multiple_tool_references_resolved(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
        analyzer_tool_info: ModuleInfo,
    ) -> None:
        """Multiple ToolReference fields all get resolved."""

        class ArchetypeSetup(SetupModel):
            search_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
                ),
            )
            analyzer_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="setup-analyzer-002", slug="setup-analyzer-002")]
                ),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.resolved_tools) == 2
        module_ids = {info.module_id for info in setup.resolved_tools.values()}
        assert "tool-search-001" in module_ids
        assert "tool-analyzer-002" in module_ids

    @pytest.mark.asyncio
    async def test_constraint_only_tool_reference_not_resolved(
        self,
        registry: FakeRegistry,
    ) -> None:
        """ToolReference with only constraints (no selected_tools) is not resolved."""

        class ArchetypeSetup(SetupModel):
            search_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    module_ids=["tool-search-001"],
                ),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.resolved_tools) == 0

    @pytest.mark.asyncio
    async def test_none_tool_reference_skipped(self, registry: FakeRegistry) -> None:
        """None values for ToolReference fields are safely skipped."""

        class ArchetypeSetup(SetupModel):
            optional_tool: ToolReference | None = Field(default=None)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)  # Should not raise


class TestNestedToolReferenceResolution:
    """Tests for recursive ToolReference resolution in nested structures."""

    @pytest.mark.asyncio
    async def test_nested_model_tool_resolved(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """ToolReference in nested BaseModel gets resolved."""

        class ToolConfig(BaseModel):
            tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
                ),
            )

        class ArchetypeSetup(SetupModel):
            name: str = "test"
            config: ToolConfig = Field(default_factory=ToolConfig)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.resolved_tools) == 1
        tool_info = next(iter(setup.resolved_tools.values()))
        assert tool_info.module_id == "tool-search-001"

    @pytest.mark.asyncio
    async def test_deeply_nested_tool_resolved(
        self,
        registry: FakeRegistry,
        analyzer_tool_info: ModuleInfo,
    ) -> None:
        """ToolReference in deeply nested structure gets resolved."""

        class DeepConfig(BaseModel):
            analyzer: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="setup-analyzer-002", slug="setup-analyzer-002")]
                ),
            )

        class MiddleConfig(BaseModel):
            deep: DeepConfig = Field(default_factory=DeepConfig)

        class ArchetypeSetup(SetupModel):
            middle: MiddleConfig = Field(default_factory=MiddleConfig)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.resolved_tools) == 1
        tool_info = next(iter(setup.resolved_tools.values()))
        assert tool_info.module_id == "tool-analyzer-002"

    @pytest.mark.asyncio
    async def test_list_of_tool_references_resolved(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
        analyzer_tool_info: ModuleInfo,
    ) -> None:
        """ToolReferences in list are all resolved."""

        class ArchetypeSetup(SetupModel):
            tools: list[ToolReference] = Field(
                default_factory=lambda: [
                    ToolReference(
                        selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
                    ),
                    ToolReference(
                        selected_tools=[SelectedTool(setup_id="setup-analyzer-002", slug="setup-analyzer-002")]
                    ),
                ],
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.tools) == 2
        assert len(setup.resolved_tools) == 2
        module_ids = {info.module_id for info in setup.resolved_tools.values()}
        assert "tool-search-001" in module_ids
        assert "tool-analyzer-002" in module_ids

    @pytest.mark.asyncio
    async def test_list_of_nested_models_with_tools_resolved(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
        writer_tool_info: ModuleInfo,
    ) -> None:
        """ToolReferences in list of nested BaseModels are resolved."""

        class ToolWrapper(BaseModel):
            name: str
            tool: ToolReference

        class ArchetypeSetup(SetupModel):
            wrappers: list[ToolWrapper] = Field(
                default_factory=lambda: [
                    ToolWrapper(
                        name="search",
                        tool=ToolReference(
                            selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
                        ),
                    ),
                    ToolWrapper(
                        name="writer",
                        tool=ToolReference(
                            selected_tools=[SelectedTool(setup_id="setup-writer-003", slug="setup-writer-003")]
                        ),
                    ),
                ],
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.resolved_tools) == 2
        module_ids = {info.module_id for info in setup.resolved_tools.values()}
        assert "tool-search-001" in module_ids
        assert "tool-writer-003" in module_ids

    @pytest.mark.asyncio
    async def test_dict_of_tool_references_resolved(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
        analyzer_tool_info: ModuleInfo,
    ) -> None:
        """ToolReferences in dict values are all resolved."""

        class ArchetypeSetup(SetupModel):
            tools_by_name: dict[str, ToolReference] = Field(
                default_factory=lambda: {
                    "search": ToolReference(
                        selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
                    ),
                    "analyzer": ToolReference(
                        selected_tools=[SelectedTool(setup_id="setup-analyzer-002", slug="setup-analyzer-002")]
                    ),
                },
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.resolved_tools) == 2
        module_ids = {info.module_id for info in setup.resolved_tools.values()}
        assert "tool-search-001" in module_ids
        assert "tool-analyzer-002" in module_ids

    @pytest.mark.asyncio
    async def test_dict_of_nested_models_with_tools_resolved(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
        writer_tool_info: ModuleInfo,
    ) -> None:
        """ToolReferences in dict of nested BaseModels are resolved."""

        class ToolWrapper(BaseModel):
            tool: ToolReference

        class ArchetypeSetup(SetupModel):
            wrappers_by_name: dict[str, ToolWrapper] = Field(
                default_factory=lambda: {
                    "search": ToolWrapper(
                        tool=ToolReference(
                            selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
                        ),
                    ),
                    "writer": ToolWrapper(
                        tool=ToolReference(
                            selected_tools=[SelectedTool(setup_id="setup-writer-003", slug="setup-writer-003")]
                        ),
                    ),
                },
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.resolved_tools) == 2
        module_ids = {info.module_id for info in setup.resolved_tools.values()}
        assert "tool-search-001" in module_ids
        assert "tool-writer-003" in module_ids


class TestComplexArchetypeSetup:
    """Integration tests for realistic archetype setup scenarios."""

    @pytest.mark.asyncio
    async def test_research_archetype_with_multiple_tools(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
        analyzer_tool_info: ModuleInfo,
        writer_tool_info: ModuleInfo,
    ) -> None:
        """Test realistic research archetype setup with diverse tool configurations."""

        class ResearchConfig(BaseModel):
            max_depth: int = 3
            search_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
                ),
            )

        class OutputConfig(BaseModel):
            format: str = "markdown"
            writer: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="setup-writer-003", slug="setup-writer-003")]
                ),
            )

        class ResearchArchetypeSetup(SetupModel):
            name: str = Field(default="Research Agent")
            research: ResearchConfig = Field(default_factory=ResearchConfig)
            output: OutputConfig = Field(default_factory=OutputConfig)
            analyzer: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="setup-analyzer-002", slug="setup-analyzer-002")]
                ),
            )
            additional_tools: list[ToolReference] = Field(default_factory=list)

        setup = ResearchArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        # All tools resolved correctly
        assert len(setup.resolved_tools) == 3
        module_ids = {info.module_id for info in setup.resolved_tools.values()}
        assert "tool-search-001" in module_ids
        assert "tool-writer-003" in module_ids
        assert "tool-analyzer-002" in module_ids

    @pytest.mark.asyncio
    async def test_setup_with_partially_resolved_tools(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """Test setup where some tools resolve and others don't."""

        class ArchetypeSetup(SetupModel):
            existing_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="setup-search-001", slug="setup-search-001")]
                ),
            )
            missing_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    selected_tools=[SelectedTool(setup_id="nonexistent-setup", slug="nonexistent-setup")]
                ),
            )
            constraint_only: ToolReference = Field(
                default_factory=lambda: ToolReference(module_ids=["some-module"]),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        # Only existing_tool resolved
        assert len(setup.resolved_tools) == 1
        tool_info = next(iter(setup.resolved_tools.values()))
        assert tool_info.module_id == "tool-search-001"


class TestToolReferenceJsonSchema:
    """Tests for tool_reference_input JSON schema generation."""

    def test_schema_has_anyof_with_array_option(self) -> None:
        """Schema has anyOf with array and ToolReference options."""
        adapter = TypeAdapter(tool_reference_input())
        schema = adapter.json_schema()
        assert "anyOf" in schema
        assert len(schema["anyOf"]) == 2
        assert schema["anyOf"][0]["type"] == "array"

    def test_schema_array_has_no_maxitems_when_unlimited(self) -> None:
        """Array option has no maxItems when max_tools is 0."""
        adapter = TypeAdapter(tool_reference_input())
        schema = adapter.json_schema()
        assert "maxItems" not in schema["anyOf"][0]

    def test_factory_adds_maxitems_to_array_option(self) -> None:
        """Factory function creates type with maxItems constraint."""
        adapter = TypeAdapter(tool_reference_input(max_tools=5))
        schema = adapter.json_schema()
        assert schema["anyOf"][0]["maxItems"] == 5

    def test_factory_adds_ui_options(self) -> None:
        """Factory function includes ui:options in schema."""
        adapter = TypeAdapter(
            tool_reference_input(
                setup_ids=["setup-1", "setup-2"],
                module_ids=["module-1"],
                tags=["tag-a"],
                max_tools=3,
            )
        )
        schema = adapter.json_schema()
        assert schema["ui:options"]["setup_ids"] == ["setup-1", "setup-2"]
        assert schema["ui:options"]["module_ids"] == ["module-1"]
        assert schema["ui:options"]["tags"] == ["tag-a"]
        assert schema["ui:options"]["max_tools"] == 3

    def test_factory_validation_still_works(self) -> None:
        """Factory type still accepts list input."""
        adapter = TypeAdapter(tool_reference_input(max_tools=3))
        ref = adapter.validate_python(["setup-1", "setup-2"])
        assert len(ref.selected_tools) == 2

    def test_converted_reference_preserves_config(self) -> None:
        """List input preserves setup_ids, module_ids, tags from factory."""
        adapter = TypeAdapter(
            tool_reference_input(
                setup_ids=["setup-a"],
                module_ids=["module-b"],
                tags=["tag-c"],
                max_tools=5,
                min_tools=1,
            )
        )
        ref = adapter.validate_python(["setup-1"])
        assert ref.setup_ids == ["setup-a"]
        assert ref.module_ids == ["module-b"]
        assert ref.tags == ["tag-c"]
        assert ref.max_tools == 5
        assert ref.min_tools == 1

    def test_min_tools_validation_raises_error(self) -> None:
        """Factory type raises error when too few tools selected."""
        adapter = TypeAdapter(tool_reference_input(min_tools=2, max_tools=5))
        with pytest.raises(ValidationError):
            adapter.validate_python(["setup-1"])

    def test_max_tools_validation_raises_error(self) -> None:
        """Factory type raises error when too many tools selected."""
        adapter = TypeAdapter(tool_reference_input(max_tools=2))
        with pytest.raises(ValidationError):
            adapter.validate_python(["setup-1", "setup-2", "setup-3"])

    def test_valid_tools_count_passes(self) -> None:
        """Valid tool count within range passes validation."""
        adapter = TypeAdapter(tool_reference_input(min_tools=1, max_tools=3))
        ref = adapter.validate_python(["setup-1", "setup-2"])
        assert len(ref.selected_tools) == 2
