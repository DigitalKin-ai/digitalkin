"""Tests for ToolReference resolution in SetupModel.

Tests the complete flow from ToolReference definition to resolution via registry,
including recursive resolution in nested structures.
"""

import pytest
from pydantic import BaseModel, Field

from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_reference import (
    ToolReference,
    ToolReferenceConfig,
    ToolSelectionMode,
)
from digitalkin.models.services.registry import (
    ModuleInfo,
    RegistryModuleStatus,
    RegistryModuleType,
)
from digitalkin.services.registry import RegistryStrategy


class FakeRegistry(RegistryStrategy):
    """Fake registry for testing tool resolution."""

    def __init__(self, modules: dict[str, ModuleInfo] | None = None) -> None:
        self._modules = modules or {}
        self._search_results: dict[str, list[ModuleInfo]] = {}

    def add_module(self, info: ModuleInfo) -> None:
        self._modules[info.module_id] = info

    def add_search_result(self, tag: str, results: list[ModuleInfo]) -> None:
        self._search_results[tag] = results

    def discover_by_id(self, module_id: str) -> ModuleInfo | None:
        return self._modules.get(module_id)

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
    reg.add_search_result("search", [search_tool_info])
    reg.add_search_result("analyzer", [analyzer_tool_info])
    return reg


class TestToolReferenceValidation:
    """Tests for ToolReferenceConfig validation."""

    def test_fixed_mode_requires_module_id(self) -> None:
        """FIXED mode without module_id raises ValueError."""
        with pytest.raises(ValueError, match="module_id required"):
            ToolReferenceConfig(mode=ToolSelectionMode.FIXED, module_id=None)

    def test_tag_mode_requires_tag(self) -> None:
        """TAG mode without tag raises ValueError."""
        with pytest.raises(ValueError, match="tag required"):
            ToolReferenceConfig(mode=ToolSelectionMode.TAG, tag=None)

    def test_discoverable_mode_no_requirements(self) -> None:
        """DISCOVERABLE mode has no field requirements."""
        config = ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE)
        assert config.mode == ToolSelectionMode.DISCOVERABLE

    def test_fixed_mode_valid(self) -> None:
        """FIXED mode with module_id is valid."""
        config = ToolReferenceConfig(mode=ToolSelectionMode.FIXED, module_id="tool-123")
        assert config.module_id == "tool-123"

    def test_tag_mode_valid(self) -> None:
        """TAG mode with tag is valid."""
        config = ToolReferenceConfig(mode=ToolSelectionMode.TAG, tag="search")
        assert config.tag == "search"


class TestToolReferenceResolution:
    """Tests for ToolReference.resolve() method."""

    def test_fixed_mode_resolves_by_id(
        self,
        registry: FakeRegistry,
        search_tool_info: ModuleInfo,
    ) -> None:
        """FIXED mode resolves module by module_id."""
        ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                module_id="tool-search-001",
            ),
        )

        result = ref.resolve(registry)

        assert result is not None
        assert result.module_id == "tool-search-001"
        assert ref.module_info == search_tool_info
        assert ref.module_id == "tool-search-001"
        assert ref.is_resolved

    def test_fixed_mode_not_found_returns_none(self, registry: FakeRegistry) -> None:
        """FIXED mode returns None when module not found."""
        ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                module_id="nonexistent-tool",
            ),
        )

        result = ref.resolve(registry)

        assert result is None
        assert ref.module_info is None
        assert ref.module_id == "nonexistent-tool"

    def test_tag_mode_resolves_by_search(
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

        result = ref.resolve(registry)

        assert result is not None
        assert result.module_id == "tool-search-001"
        assert ref.module_info == search_tool_info
        assert ref.module_id == "tool-search-001"

    def test_tag_mode_not_found_returns_none(self, registry: FakeRegistry) -> None:
        """TAG mode returns None when no search results."""
        ref = ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.TAG,
                tag="nonexistent-tag",
            ),
        )

        result = ref.resolve(registry)

        assert result is None
        assert ref.module_info is None

    def test_discoverable_mode_returns_none(self, registry: FakeRegistry) -> None:
        """DISCOVERABLE mode always returns None (LLM handles at runtime)."""
        ref = ToolReference(
            config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
        )

        result = ref.resolve(registry)

        assert result is None
        assert not ref.is_resolved


class TestSetupModelToolResolution:
    """Tests for SetupModel.resolve_tool_references() method."""

    def test_single_tool_reference_resolved(
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
                        module_id="tool-search-001",
                    ),
                ),
            )

        setup = ArchetypeSetup()
        setup.resolve_tool_references(registry)

        assert setup.search_tool.is_resolved
        assert setup.search_tool.module_info == search_tool_info
        assert setup.search_tool.module_id == "tool-search-001"

    def test_multiple_tool_references_resolved(
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
                        module_id="tool-search-001",
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
        setup.resolve_tool_references(registry)

        assert setup.search_tool.module_info == search_tool_info
        assert setup.analyzer_tool.module_info == analyzer_tool_info

    def test_mixed_tool_modes_resolved(
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
                        module_id="tool-search-001",
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
        setup.resolve_tool_references(registry)

        assert setup.fixed_tool.is_resolved
        assert setup.tag_tool.is_resolved
        assert not setup.discoverable_tool.is_resolved

    def test_none_tool_reference_skipped(self, registry: FakeRegistry) -> None:
        """None values for ToolReference fields are safely skipped."""

        class ArchetypeSetup(SetupModel):
            optional_tool: ToolReference | None = Field(default=None)

        setup = ArchetypeSetup()
        setup.resolve_tool_references(registry)  # Should not raise


class TestNestedToolReferenceResolution:
    """Tests for recursive ToolReference resolution in nested structures."""

    def test_nested_model_tool_resolved(
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
                        module_id="tool-search-001",
                    ),
                ),
            )

        class ArchetypeSetup(SetupModel):
            name: str = "test"
            config: ToolConfig = Field(default_factory=ToolConfig)

        setup = ArchetypeSetup()
        setup.resolve_tool_references(registry)

        assert setup.config.tool.is_resolved
        assert setup.config.tool.module_info == search_tool_info

    def test_deeply_nested_tool_resolved(
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
                        module_id="tool-analyzer-002",
                    ),
                ),
            )

        class MiddleConfig(BaseModel):
            deep: DeepConfig = Field(default_factory=DeepConfig)

        class ArchetypeSetup(SetupModel):
            middle: MiddleConfig = Field(default_factory=MiddleConfig)

        setup = ArchetypeSetup()
        setup.resolve_tool_references(registry)

        assert setup.middle.deep.analyzer.is_resolved
        assert setup.middle.deep.analyzer.module_info == analyzer_tool_info

    def test_list_of_tool_references_resolved(
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
                            module_id="tool-search-001",
                        ),
                    ),
                    ToolReference(
                        config=ToolReferenceConfig(
                            mode=ToolSelectionMode.FIXED,
                            module_id="tool-analyzer-002",
                        ),
                    ),
                ],
            )

        setup = ArchetypeSetup()
        setup.resolve_tool_references(registry)

        assert len(setup.tools) == 2
        assert setup.tools[0].module_info == search_tool_info
        assert setup.tools[1].module_info == analyzer_tool_info

    def test_list_of_nested_models_with_tools_resolved(
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
                                module_id="tool-search-001",
                            ),
                        ),
                    ),
                    ToolWrapper(
                        name="writer",
                        tool=ToolReference(
                            config=ToolReferenceConfig(
                                mode=ToolSelectionMode.FIXED,
                                module_id="tool-writer-003",
                            ),
                        ),
                    ),
                ],
            )

        setup = ArchetypeSetup()
        setup.resolve_tool_references(registry)

        assert setup.wrappers[0].tool.module_info == search_tool_info
        assert setup.wrappers[1].tool.module_info == writer_tool_info

    def test_dict_of_tool_references_resolved(
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
                            module_id="tool-search-001",
                        ),
                    ),
                    "analyzer": ToolReference(
                        config=ToolReferenceConfig(
                            mode=ToolSelectionMode.FIXED,
                            module_id="tool-analyzer-002",
                        ),
                    ),
                },
            )

        setup = ArchetypeSetup()
        setup.resolve_tool_references(registry)

        assert setup.tools_by_name["search"].module_info == search_tool_info
        assert setup.tools_by_name["analyzer"].module_info == analyzer_tool_info

    def test_dict_of_nested_models_with_tools_resolved(
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
                                module_id="tool-search-001",
                            ),
                        ),
                    ),
                    "writer": ToolWrapper(
                        tool=ToolReference(
                            config=ToolReferenceConfig(
                                mode=ToolSelectionMode.FIXED,
                                module_id="tool-writer-003",
                            ),
                        ),
                    ),
                },
            )

        setup = ArchetypeSetup()
        setup.resolve_tool_references(registry)

        assert setup.wrappers_by_name["search"].tool.module_info == search_tool_info
        assert setup.wrappers_by_name["writer"].tool.module_info == writer_tool_info


class TestComplexArchetypeSetup:
    """Integration tests for realistic archetype setup scenarios."""

    def test_research_archetype_with_multiple_tools(
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
                        module_id="tool-search-001",
                    ),
                ),
            )

        class OutputConfig(BaseModel):
            format: str = "markdown"
            writer: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        module_id="tool-writer-003",
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
        setup.resolve_tool_references(registry)

        # All tools resolved correctly
        assert setup.research.search_tool.module_info == search_tool_info
        assert setup.output.writer.module_info == writer_tool_info
        assert setup.analyzer.module_info == analyzer_tool_info

    def test_setup_with_partially_resolved_tools(
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
                        module_id="tool-search-001",
                    ),
                ),
            )
            missing_tool: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(
                        mode=ToolSelectionMode.FIXED,
                        module_id="nonexistent-tool",
                    ),
                ),
            )
            discoverable: ToolReference = Field(
                default_factory=lambda: ToolReference(
                    config=ToolReferenceConfig(mode=ToolSelectionMode.DISCOVERABLE),
                ),
            )

        setup = ArchetypeSetup()
        setup.resolve_tool_references(registry)

        assert setup.existing_tool.is_resolved
        assert setup.existing_tool.module_info == search_tool_info
        assert not setup.missing_tool.is_resolved
        assert setup.missing_tool.module_info is None
        assert not setup.discoverable.is_resolved
