"""Tests for ToolReference resolution in SetupModel.

Tests the complete flow from ToolReference definition to resolution via registry,
including recursive resolution in nested structures.
"""

from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_cache import ToolDefinition, ToolModuleInfo
from digitalkin.models.module.tool_reference import ToolReference, ToolSelection, tool_reference_input
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

    async def discover_by_id(self, module_id: str) -> ModuleInfo | None:
        return self._modules.get(module_id)

    async def get_setup(self, setup_id: str) -> SetupInfo | None:
        return self._setups.get(setup_id)

    async def search(
        self,
        name: str | None = None,
        module_type: str | None = None,
        organization_id: str | None = None,
    ) -> list[ModuleInfo]:
        if name and name in self._search_results:
            return self._search_results[name]
        return []

    async def get_status(self, module_id: str) -> None:
        return None

    async def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
    ) -> ModuleInfo | None:
        return None

    async def heartbeat(self, module_id: str) -> RegistryModuleStatus:
        return RegistryModuleStatus.ACTIVE

    async def deregister(self, module_id: str) -> bool:
        return True


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
                parameters_schema={
                    "type": "object",
                    "properties": {"query": {"type": "string", "description": "Search query"}},
                    "required": ["query"],
                },
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

    def test_selected_tools_valid(self) -> None:
        """ToolReference with selected_tools is valid."""
        ref = ToolReference(selected_tools=[ToolSelection(setup_id="setup-123", triggers={"search": True})])
        assert len(ref.selected_tools) == 1
        assert ref.selected_tools[0].setup_id == "setup-123"
        assert ref.selected_tools[0].triggers == {"search": True}

    def test_list_input_creates_selected_tools(self) -> None:
        """List of dicts input creates selected_tools via tool_reference_input."""
        adapter = TypeAdapter(tool_reference_input())
        ref = adapter.validate_python([
            {"setupId": "setup-123", "triggers": {"search": True}},
            {"setupId": "setup-456", "triggers": {"analyze": True}},
        ])
        assert len(ref.selected_tools) == 2
        assert ref.selected_tools[0].setup_id == "setup-123"
        assert ref.selected_tools[1].setup_id == "setup-456"

    def test_object_input_accepted(self) -> None:
        """ToolReference object input is also accepted via tool_reference_input."""
        adapter = TypeAdapter(tool_reference_input())
        ref = adapter.validate_python({
            "selected_tools": [
                {"setup_id": "setup-123", "triggers": {"search": True}},
                {"setup_id": "setup-456", "triggers": {"analyze": True}},
            ],
        })
        assert len(ref.selected_tools) == 2
        assert ref.selected_tools[0].setup_id == "setup-123"
        assert ref.selected_tools[1].setup_id == "setup-456"


class TestToolReferenceResolution:
    """Tests for ToolReference.resolve() method."""

    @pytest.mark.asyncio
    async def test_selected_tools_resolve_by_setup_id(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """Selected tools resolve via registry lookup."""
        ref = ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})])

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert len(result) == 1
        assert result[0].module_id == "tool-search-001"
        assert len(result[0].tools) == 1
        assert result[0].tools[0].name == "search"

    @pytest.mark.asyncio
    async def test_multiple_selected_tools_resolve(
        self,
        registry: FakeRegistry,
    ) -> None:
        """Multiple selected tools all resolve."""
        ref = ToolReference(selected_tools=[
            ToolSelection(setup_id="setup-search-001", triggers={"search": True}),
            ToolSelection(setup_id="setup-analyzer-002", triggers={"search": True}),
        ])

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert len(result) == 2
        module_ids = {r.module_id for r in result}
        assert "tool-search-001" in module_ids
        assert "tool-analyzer-002" in module_ids

    @pytest.mark.asyncio
    async def test_nonexistent_setup_returns_empty(self, registry: FakeRegistry) -> None:
        """Selected tool with nonexistent setup_id is not resolved."""
        ref = ToolReference(selected_tools=[ToolSelection(setup_id="nonexistent-setup", triggers={"search": True})])

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_unknown_trigger_names_warned_and_filtered(self, registry: FakeRegistry) -> None:
        """Triggers naming protocols the module does not expose are warned and dropped."""
        ref = ToolReference(
            selected_tools=[
                ToolSelection(
                    setup_id="setup-search-001",
                    triggers={"search": True, "healthcheck_ping": True, "bogus": True},
                ),
            ],
        )
        communication = create_mock_communication()

        with patch("digitalkin.models.module.tool_reference.logger") as mock_logger:
            result = await ref.resolve(registry, communication)

        # The known trigger 'search' survives; unknown names are filtered out.
        assert len(result) == 1
        assert [t.name for t in result[0].tools] == ["search"]
        # The unknown names are surfaced in a single warning.
        mock_logger.warning.assert_called_once()
        assert mock_logger.warning.call_args.args[2] == ["bogus", "healthcheck_ping"]

    @pytest.mark.asyncio
    async def test_known_triggers_emit_no_warning(self, registry: FakeRegistry) -> None:
        """When every enabled trigger matches a real protocol, nothing is warned."""
        ref = ToolReference(
            selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})],
        )
        communication = create_mock_communication()

        with patch("digitalkin.models.module.tool_reference.logger") as mock_logger:
            result = await ref.resolve(registry, communication)

        assert [t.name for t in result[0].tools] == ["search"]
        mock_logger.warning.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_selected_tools_returns_empty(self, registry: FakeRegistry) -> None:
        """ToolReference with no selected_tools returns empty list."""
        ref = ToolReference()

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert result == []


class TestSetupModelToolResolution:
    """Tests for SetupModel.build_tool_cache() method."""

    @pytest.mark.asyncio
    async def test_single_tool_reference_resolved(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """Single ToolReference field gets resolved."""

        class ArchetypeSetup(SetupModel):
            search_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})]),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

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
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})]),
            )
            analyzer_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="setup-analyzer-002", triggers={"search": True})]),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

        assert len(setup.resolved_tools) == 2
        module_ids = {info.module_id for info in setup.resolved_tools.values()}
        assert "tool-search-001" in module_ids
        assert "tool-analyzer-002" in module_ids

    @pytest.mark.asyncio
    async def test_empty_tool_reference_not_resolved(
        self,
        registry: FakeRegistry,
    ) -> None:
        """ToolReference with no selected_tools is not resolved."""

        class ArchetypeSetup(SetupModel):
            search_tool: ToolReference = Field(default_factory=ToolReference)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

        assert len(setup.resolved_tools) == 0

    @pytest.mark.asyncio
    async def test_none_tool_reference_skipped(self, registry: FakeRegistry) -> None:
        """None values for ToolReference fields are safely skipped."""

        class ArchetypeSetup(SetupModel):
            optional_tool: ToolReference | None = Field(default=None)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)  # Should not raise


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
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})]),
            )

        class ArchetypeSetup(SetupModel):
            name: str = "test"
            config: ToolConfig = Field(default_factory=ToolConfig)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

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
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="setup-analyzer-002", triggers={"search": True})]),
            )

        class MiddleConfig(BaseModel):
            deep: DeepConfig = Field(default_factory=DeepConfig)

        class ArchetypeSetup(SetupModel):
            middle: MiddleConfig = Field(default_factory=MiddleConfig)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

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
                    ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})]),
                    ToolReference(selected_tools=[ToolSelection(setup_id="setup-analyzer-002", triggers={"search": True})]),
                ],
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

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
                        tool=ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})]),
                    ),
                    ToolWrapper(
                        name="writer",
                        tool=ToolReference(selected_tools=[ToolSelection(setup_id="setup-writer-003", triggers={"search": True})]),
                    ),
                ],
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

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
                    "search": ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})]),
                    "analyzer": ToolReference(selected_tools=[ToolSelection(setup_id="setup-analyzer-002", triggers={"search": True})]),
                },
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

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
                    "search": ToolWrapper(tool=ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})])),
                    "writer": ToolWrapper(tool=ToolReference(selected_tools=[ToolSelection(setup_id="setup-writer-003", triggers={"search": True})])),
                },
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

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
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})]),
            )

        class OutputConfig(BaseModel):
            format: str = "markdown"
            writer: ToolReference = Field(
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="setup-writer-003", triggers={"search": True})]),
            )

        class ResearchArchetypeSetup(SetupModel):
            name: str = Field(default="Research Agent")
            research: ResearchConfig = Field(default_factory=ResearchConfig)
            output: OutputConfig = Field(default_factory=OutputConfig)
            analyzer: ToolReference = Field(
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="setup-analyzer-002", triggers={"search": True})]),
            )
            additional_tools: list[ToolReference] = Field(default_factory=list)

        setup = ResearchArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

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
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="setup-search-001", triggers={"search": True})]),
            )
            missing_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(selected_tools=[ToolSelection(setup_id="nonexistent-setup", triggers={"search": True})]),
            )
            empty_tool: ToolReference = Field(default_factory=ToolReference)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.build_tool_cache(registry, communication)

        # Only existing_tool resolved
        assert len(setup.resolved_tools) == 1
        tool_info = next(iter(setup.resolved_tools.values()))
        assert tool_info.module_id == "tool-search-001"


class TestToolReferenceJsonSchema:
    """Tests for tool_reference_input JSON schema generation."""

    def test_schema_is_array_of_objects(self) -> None:
        """Schema is array of objects with setupId and triggers."""
        adapter = TypeAdapter(tool_reference_input())
        schema = adapter.json_schema()
        assert schema["type"] == "array"
        items = schema["items"]
        assert items["type"] == "object"
        assert items["properties"]["setupId"]["type"] == "string"
        assert items["properties"]["triggers"]["type"] == "object"
        assert items["required"] == ["setupId", "triggers"]

    def test_schema_has_ui_widget(self) -> None:
        """Schema has ui:widget set to toolSelect."""
        adapter = TypeAdapter(tool_reference_input())
        schema = adapter.json_schema()
        assert schema["ui:widget"] == "toolSelect"

    def test_schema_has_no_max_items_when_zero(self) -> None:
        """Schema has no maxItems when max_tools is 0."""
        adapter = TypeAdapter(tool_reference_input())
        schema = adapter.json_schema()
        assert "maxItems" not in schema

    def test_schema_has_no_min_items_when_zero(self) -> None:
        """Schema has no minItems when min_tools is 0."""
        adapter = TypeAdapter(tool_reference_input())
        schema = adapter.json_schema()
        assert "minItems" not in schema

    def test_factory_adds_max_items(self) -> None:
        """Factory function adds maxItems to schema when > 0."""
        adapter = TypeAdapter(tool_reference_input(max_tools=5))
        schema = adapter.json_schema()
        assert schema["maxItems"] == 5

    def test_factory_adds_min_items(self) -> None:
        """Factory function adds minItems to schema when > 0."""
        adapter = TypeAdapter(tool_reference_input(min_tools=2))
        schema = adapter.json_schema()
        assert schema["minItems"] == 2

    def test_factory_adds_ui_options(self) -> None:
        """Factory function includes ui:options in schema."""
        adapter = TypeAdapter(
            tool_reference_input(
                setup_ids=["setup-1", "setup-2"],
                module_ids=["module-1"],
                tag_ids=["tag-a"],
            )
        )
        schema = adapter.json_schema()
        assert schema["ui:options"]["setupIds"] == ["setup-1", "setup-2"]
        assert schema["ui:options"]["moduleIds"] == ["module-1"]
        assert schema["ui:options"]["tagIds"] == ["tag-a"]

    def test_list_input_creates_tool_reference(self) -> None:
        """List of dicts input creates ToolReference with selected_tools."""
        adapter = TypeAdapter(tool_reference_input())
        ref = adapter.validate_python([
            {"setupId": "setup-1", "triggers": {"search": True}},
            {"setupId": "setup-2", "triggers": {"analyze": True}},
        ])
        assert len(ref.selected_tools) == 2
        assert ref.selected_tools[0].setup_id == "setup-1"
        assert ref.selected_tools[1].setup_id == "setup-2"

    def test_object_input_still_works(self) -> None:
        """ToolReference object input is also accepted."""
        adapter = TypeAdapter(tool_reference_input(max_tools=3))
        ref = adapter.validate_python({
            "selected_tools": [
                {"setup_id": "setup-1", "triggers": {"search": True}},
                {"setup_id": "setup-2", "triggers": {"analyze": True}},
            ],
        })
        assert len(ref.selected_tools) == 2

    def test_min_tools_validation_raises_error(self) -> None:
        """Factory type raises error when too few tools selected."""
        adapter = TypeAdapter(tool_reference_input(min_tools=2, max_tools=5))
        with pytest.raises(ValidationError):
            adapter.validate_python([{"setupId": "setup-1", "triggers": {"search": True}}])

    def test_max_tools_validation_raises_error(self) -> None:
        """Factory type raises error when too many tools selected."""
        adapter = TypeAdapter(tool_reference_input(max_tools=2))
        with pytest.raises(ValidationError):
            adapter.validate_python([
                {"setupId": "setup-1", "triggers": {"search": True}},
                {"setupId": "setup-2", "triggers": {"analyze": True}},
                {"setupId": "setup-3", "triggers": {"write": True}},
            ])

    def test_valid_tools_count_passes(self) -> None:
        """Valid tool count within range passes validation."""
        adapter = TypeAdapter(tool_reference_input(min_tools=1, max_tools=3))
        ref = adapter.validate_python([
            {"setupId": "setup-1", "triggers": {"search": True}},
            {"setupId": "setup-2", "triggers": {"analyze": True}},
        ])
        assert len(ref.selected_tools) == 2
