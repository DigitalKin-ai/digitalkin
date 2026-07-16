"""Tests for DefaultRegistry module_type handling and module class registry markers."""

import pytest

from digitalkin.models.services.registry import (
    RegistryModuleType,
    RegistrySetupStatus,
    SetupInfo,
)
from digitalkin.modules import ArchetypeModule, ToolModule
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.registry import DefaultRegistry


class TestDefaultRegistryModuleType:
    """Tests for module_type storage in DefaultRegistry.register()."""

    async def test_register_stores_declared_type(self) -> None:
        """Registering with an explicit type stores it."""
        registry = DefaultRegistry("", "", "")
        result = await registry.register(
            module_id="modules:tool1",
            address="localhost",
            port=50051,
            version="1.0.0",
            module_type=RegistryModuleType.TOOL_MODULE,
        )
        assert result is not None
        assert result.module_type == RegistryModuleType.TOOL_MODULE

    async def test_register_unspecified_preserves_existing_type(self) -> None:
        """Re-registering with UNSPECIFIED keeps the previously declared type."""
        registry = DefaultRegistry("", "", "")
        await registry.register(
            module_id="modules:kin1",
            address="localhost",
            port=50051,
            version="1.0.0",
            module_type=RegistryModuleType.ARCHETYPE,
        )
        result = await registry.register(
            module_id="modules:kin1",
            address="localhost",
            port=50052,
            version="1.0.1",
        )
        assert result is not None
        assert result.module_type == RegistryModuleType.ARCHETYPE
        assert result.port == 50052

    @pytest.mark.parametrize(
        ("view", "expected_type"),
        [
            ("search_tools", RegistryModuleType.TOOL_MODULE),
            ("search_kins", RegistryModuleType.ARCHETYPE),
        ],
    )
    async def test_typed_views_filter_by_type(self, view: str, expected_type: RegistryModuleType) -> None:
        """search_tools/search_kins return only modules of the matching type."""
        registry = DefaultRegistry("", "", "")
        await registry.register(
            module_id="modules:tool1",
            address="localhost",
            port=50051,
            version="1.0.0",
            module_type=RegistryModuleType.TOOL_MODULE,
        )
        await registry.register(
            module_id="modules:kin1",
            address="localhost",
            port=50052,
            version="1.0.0",
            module_type=RegistryModuleType.ARCHETYPE,
        )
        search_view = registry.search_tools if view == "search_tools" else registry.search_kins
        results = await search_view()
        assert len(results) == 1
        assert results[0].module_type == expected_type


class TestDefaultRegistrySetups:
    """Tests for the in-memory setup store and search_setups()."""

    def _seed(self, registry: DefaultRegistry) -> None:
        """Store one tool setup and one archetype setup."""
        registry.add_setup(
            SetupInfo(
                setup_id="setups:duda",
                name="Duda Builder",
                documentation="Builds websites on the Duda platform",
                status=RegistrySetupStatus.READY,
                module_id="modules:duda",
                module_name="tool-duda",
                module_type=RegistryModuleType.TOOL_MODULE,
            )
        )
        registry.add_setup(
            SetupInfo(
                setup_id="setups:isaac",
                name="Isaac",
                documentation="Multi-agent orchestration kin",
                status=RegistrySetupStatus.DRAFT,
                module_id="modules:isaac",
                module_name="archetype-isaac",
                module_type=RegistryModuleType.ARCHETYPE,
            )
        )

    async def test_add_and_get_setup_roundtrip(self) -> None:
        """add_setup stores and get_setup retrieves; missing id returns None."""
        registry = DefaultRegistry("", "", "")
        self._seed(registry)
        setup = await registry.get_setup("setups:duda")
        assert setup is not None
        assert setup.name == "Duda Builder"
        assert await registry.get_setup("setups:unknown") is None

    async def test_search_setups_query_matches_name_and_documentation(self) -> None:
        """Query matches case-insensitively on name and documentation."""
        registry = DefaultRegistry("", "", "")
        self._seed(registry)
        by_name = await registry.search_setups(query="DUDA")
        assert [s.setup_id for s in by_name] == ["setups:duda"]
        by_doc = await registry.search_setups(query="orchestration")
        assert [s.setup_id for s in by_doc] == ["setups:isaac"]

    async def test_search_setups_facet_filters(self) -> None:
        """module_types and statuses filters narrow results."""
        registry = DefaultRegistry("", "", "")
        self._seed(registry)
        tools = await registry.search_setups(module_types=[RegistryModuleType.TOOL_MODULE])
        assert [s.setup_id for s in tools] == ["setups:duda"]
        ready = await registry.search_setups(statuses=[RegistrySetupStatus.READY])
        assert [s.setup_id for s in ready] == ["setups:duda"]

    async def test_search_setups_pagination(self) -> None:
        """offset/limit slice the result list."""
        registry = DefaultRegistry("", "", "")
        self._seed(registry)
        page1 = await registry.search_setups(limit=1)
        page2 = await registry.search_setups(limit=1, offset=1)
        assert len(page1) == len(page2) == 1
        assert page1[0].setup_id != page2[0].setup_id

    async def test_search_setups_no_match(self) -> None:
        """Unknown query returns an empty list."""
        registry = DefaultRegistry("", "", "")
        self._seed(registry)
        assert await registry.search_setups(query="nothing") == []

    async def test_search_setups_returns_config_free_summary(self) -> None:
        """search_setups yields SetupSummary — a stored setup's config can never be serialized."""
        registry = DefaultRegistry("", "", "")
        self._seed(registry)
        results = await registry.search_setups()
        assert results
        assert all("config" not in type(s).model_fields for s in results)


class TestRegistryTypeMarkers:
    """Tests for the registry_type ClassVar on module base classes."""

    def test_module_class_markers(self) -> None:
        """Module base classes declare their registry type."""
        assert BaseModule.registry_type == RegistryModuleType.UNSPECIFIED
        assert ToolModule.registry_type == RegistryModuleType.TOOL_MODULE
        assert ArchetypeModule.registry_type == RegistryModuleType.ARCHETYPE
