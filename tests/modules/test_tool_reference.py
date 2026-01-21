"""Tests for ToolReference resolution in SetupModel.

Tests the complete flow from ToolReference definition to resolution via registry,
including recursive resolution in nested structures.
"""

from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, Field

from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_cache import ToolDefinition, ToolModuleInfo, ToolParameter
from digitalkin.models.module.tool_reference import (
    ToolReference,
    ToolReferenceConfig,
    ToolSelectionMode,
)
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

    def add_setup(self, setup_id: str, module_id: str) -> None:
        self._setups[setup_id] = SetupInfo(
            setup_id=setup_id,
            name=f"Setup {setup_id}",
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


def create_tool_module_info(module_id: str, name: str, port: int = 50051, setup_id: str = "") -> ToolModuleInfo:
    """Create a ToolModuleInfo for testing."""
    return ToolModuleInfo(
        module_id=module_id,
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=port,
        version="1.0.0",
        name=name,
        setup_id=setup_id,
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
        name="SearchTool",
    )


@pytest.fixture
def analyzer_tool_info() -> ModuleInfo:
    return ModuleInfo(
        module_id="tool-analyzer-002",
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50052,
        version="2.0.0",
        name="AnalyzerTool",
    )


@pytest.fixture
def writer_tool_info() -> ModuleInfo:
    return ModuleInfo(
        module_id="tool-writer-003",
        module_type=RegistryModuleType.TOOL,
        address="localhost",
        port=50053,
        version="1.5.0",
        name="WriterTool",
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
    reg.add_setup("setup-search-001", "tool-search-001")
    reg.add_setup("setup-analyzer-002", "tool-analyzer-002")
    reg.add_setup("setup-writer-003", "tool-writer-003")
    reg.add_search_result("search", [search_tool_info])
    reg.add_search_result("analyzer", [analyzer_tool_info])
    return reg


class TestToolReferenceValidation:
    """Tests for ToolReferenceConfig validation."""

    def test_fixed_mode_requires_module_id(self) -> None:
        """FIXED mode without setup_id raises ValueError."""
        with pytest.raises(ValueError, match="setup_id required"):
            ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="")

    def test_tag_mode_requires_tag(self) -> None:
        """TAG mode without tag raises ValueError."""
        with pytest.raises(ValueError, match="tag required"):
            ToolReferenceConfig(mode=ToolSelectionMode.TAG, tag="")

    def test_discoverable_mode_no_requirements(self) -> None:
        """DISCOVERABLE mode has no field requirements."""
        config = ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE)
        assert config.mode == ToolSelectionMode.DISCOVERABLE

    def test_fixed_mode_valid(self) -> None:
        """FIXED mode with setup_id is valid."""
        config = ToolReferenceConfig(mode=ToolSelectionMode.FIXED, setup_id="setup-123")
        assert config.setup_id == "setup-123"

    def test_tag_mode_valid(self) -> None:
        """TAG mode with tag is valid."""
        config = ToolReferenceConfig(mode=ToolSelectionMode.TAG, tag="search")
        assert config.tag == "search"


class TestToolReferenceResolution:
    """Tests for ToolReference.resolve() method."""

    @pytest.mark.asyncio
    async def test_fixed_mode_resolves_by_id(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """FIXED mode resolves module by setup_id."""
        ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                setup_id="setup-search-001",
            ),
        )

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert result is not None
        assert result.module_id == "tool-search-001"
        assert ref.tool_module_info is not None
        assert ref.tool_module_info.module_id == "tool-search-001"
        assert ref.module_id == "tool-search-001"
        assert ref.is_resolved

    @pytest.mark.asyncio
    async def test_fixed_mode_not_found_returns_none(self, registry: FakeRegistry) -> None:
        """FIXED mode returns None when setup not found."""
        ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                setup_id="nonexistent-setup",
            ),
        )

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert result is None
        assert ref.tool_module_info is None

    @pytest.mark.asyncio
    async def test_tag_mode_resolves_by_search(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """TAG mode resolves module by searching with tag."""
        ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.TAG,
                tag="search",
            ),
        )

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert result is not None
        assert result.module_id == "tool-search-001"
        assert ref.tool_module_info is not None
        assert ref.module_id == "tool-search-001"

    @pytest.mark.asyncio
    async def test_tag_mode_not_found_returns_none(self, registry: FakeRegistry) -> None:
        """TAG mode returns None when no search results."""
        ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.TAG,
                tag="nonexistent-tag",
            ),
        )

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert result is None
        assert ref.tool_module_info is None

    @pytest.mark.asyncio
    async def test_discoverable_mode_returns_none(self, registry: FakeRegistry) -> None:
        """DISCOVERABLE mode always returns None (LLM handles at runtime)."""
        ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
        )

        communication = create_mock_communication()
        result = await ref.resolve(registry, communication)

        assert result is None
        assert not ref.is_resolved


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
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        setup_id="setup-search-001",
                    ),
                ),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert setup.search_tool.is_resolved
        assert setup.search_tool.tool_module_info is not None
        assert setup.search_tool.tool_module_info.module_id == "tool-search-001"

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
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        setup_id="setup-search-001",
                    ),
                ),
            )
            analyzer_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.TAG,
                        tag="analyzer",
                    ),
                ),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert setup.search_tool.tool_module_info is not None
        assert setup.search_tool.tool_module_info.module_id == "tool-search-001"
        assert setup.analyzer_tool.tool_module_info is not None
        assert setup.analyzer_tool.tool_module_info.module_id == "tool-analyzer-002"

    @pytest.mark.asyncio
    async def test_mixed_tool_modes_resolved(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """Mix of FIXED, TAG, and DISCOVERABLE modes work together."""

        class ArchetypeSetup(SetupModel):
            fixed_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        setup_id="setup-search-001",
                    ),
                ),
            )
            tag_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.TAG,
                        tag="search",
                    ),
                ),
            )
            discoverable_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
                ),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert setup.fixed_tool.is_resolved
        assert setup.tag_tool.is_resolved
        assert not setup.discoverable_tool.is_resolved

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
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        setup_id="setup-search-001",
                    ),
                ),
            )

        class ArchetypeSetup(SetupModel):
            name: str = "test"
            config: ToolConfig = Field(default_factory=ToolConfig)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert setup.config.tool.is_resolved
        assert setup.config.tool.tool_module_info is not None
        assert setup.config.tool.tool_module_info.module_id == "tool-search-001"

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
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        setup_id="setup-analyzer-002",
                    ),
                ),
            )

        class MiddleConfig(BaseModel):
            deep: DeepConfig = Field(default_factory=DeepConfig)

        class ArchetypeSetup(SetupModel):
            middle: MiddleConfig = Field(default_factory=MiddleConfig)

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert setup.middle.deep.analyzer.is_resolved
        assert setup.middle.deep.analyzer.tool_module_info is not None
        assert setup.middle.deep.analyzer.tool_module_info.module_id == "tool-analyzer-002"

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
                        config=ToolReferenceConfig(
                            mode=ToolSelectionMode.FIXED,
                            setup_id="setup-search-001",
                        ),
                    ),
                    ToolReference(
                        config=ToolReferenceConfig(
                            mode=ToolSelectionMode.FIXED,
                            setup_id="setup-analyzer-002",
                        ),
                    ),
                ],
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert len(setup.tools) == 2
        assert setup.tools[0].tool_module_info is not None
        assert setup.tools[0].tool_module_info.module_id == "tool-search-001"
        assert setup.tools[1].tool_module_info is not None
        assert setup.tools[1].tool_module_info.module_id == "tool-analyzer-002"

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
                            config=ToolReferenceConfig(
                                mode=ToolSelectionMode.FIXED,
                                setup_id="setup-search-001",
                            ),
                        ),
                    ),
                    ToolWrapper(
                        name="writer",
                        tool=ToolReference(
                            config=ToolReferenceConfig(
                                mode=ToolSelectionMode.FIXED,
                                setup_id="setup-writer-003",
                            ),
                        ),
                    ),
                ],
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert setup.wrappers[0].tool.tool_module_info is not None
        assert setup.wrappers[0].tool.tool_module_info.module_id == "tool-search-001"
        assert setup.wrappers[1].tool.tool_module_info is not None
        assert setup.wrappers[1].tool.tool_module_info.module_id == "tool-writer-003"

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
                        config=ToolReferenceConfig(
                            mode=ToolSelectionMode.FIXED,
                            setup_id="setup-search-001",
                        ),
                    ),
                    "analyzer": ToolReference(
                        config=ToolReferenceConfig(
                            mode=ToolSelectionMode.FIXED,
                            setup_id="setup-analyzer-002",
                        ),
                    ),
                },
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert setup.tools_by_name["search"].tool_module_info is not None
        assert setup.tools_by_name["search"].tool_module_info.module_id == "tool-search-001"
        assert setup.tools_by_name["analyzer"].tool_module_info is not None
        assert setup.tools_by_name["analyzer"].tool_module_info.module_id == "tool-analyzer-002"

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
                            config=ToolReferenceConfig(
                                mode=ToolSelectionMode.FIXED,
                                setup_id="setup-search-001",
                            ),
                        ),
                    ),
                    "writer": ToolWrapper(
                        tool=ToolReference(
                            config=ToolReferenceConfig(
                                mode=ToolSelectionMode.FIXED,
                                setup_id="setup-writer-003",
                            ),
                        ),
                    ),
                },
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert setup.wrappers_by_name["search"].tool.tool_module_info is not None
        assert setup.wrappers_by_name["search"].tool.tool_module_info.module_id == "tool-search-001"
        assert setup.wrappers_by_name["writer"].tool.tool_module_info is not None
        assert setup.wrappers_by_name["writer"].tool.tool_module_info.module_id == "tool-writer-003"


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
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        setup_id="setup-search-001",
                    ),
                ),
            )

        class OutputConfig(BaseModel):
            format: str = "markdown"
            writer: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        setup_id="setup-writer-003",
                    ),
                ),
            )

        class ResearchArchetypeSetup(SetupModel):
            name: str = Field(default="Research Agent")
            research: ResearchConfig = Field(default_factory=ResearchConfig)
            output: OutputConfig = Field(default_factory=OutputConfig)
            analyzer: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.TAG,
                        tag="analyzer",
                    ),
                ),
            )
            additional_tools: list[ToolReference] = Field(default_factory=list)

        setup = ResearchArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        # All tools resolved correctly
        assert setup.research.search_tool.tool_module_info is not None
        assert setup.research.search_tool.tool_module_info.module_id == "tool-search-001"
        assert setup.output.writer.tool_module_info is not None
        assert setup.output.writer.tool_module_info.module_id == "tool-writer-003"
        assert setup.analyzer.tool_module_info is not None
        assert setup.analyzer.tool_module_info.module_id == "tool-analyzer-002"

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
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        setup_id="setup-search-001",
                    ),
                ),
            )
            missing_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        setup_id="nonexistent-setup",
                    ),
                ),
            )
            discoverable: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
                ),
            )

        setup = ArchetypeSetup()
        communication = create_mock_communication()
        await setup.resolve_tool_references(registry, communication)

        assert setup.existing_tool.is_resolved
        assert setup.existing_tool.tool_module_info is not None
        assert setup.existing_tool.tool_module_info.module_id == "tool-search-001"
        assert not setup.missing_tool.is_resolved
        assert setup.missing_tool.tool_module_info is None
        assert not setup.discoverable.is_resolved
