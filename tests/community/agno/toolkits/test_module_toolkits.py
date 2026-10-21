"""Tests for ModuleToolkits: per-agent trigger filtering and runtime-loaded tool rehydration.

``tool_cache.entries`` holds a single ``ToolModuleInfo`` per setup, shared across every agent that
references it. Each toolkit must filter that shared catalog to its own triggers without mutating
the cache, so a head agent, its team members and workflow steps all see exactly their own triggers.
"""

from unittest.mock import Mock

from agno.tools import Toolkit

from digitalkin.community.agno.toolkits import ModuleToolkits
from digitalkin.models.module.tool_cache import ToolCache, ToolDefinition, ToolModuleInfo
from digitalkin.models.module.tool_reference import ToolSelection
from digitalkin.models.services.registry import RegistryModuleType

SETUP_ID = "setups:kg"
TOOL_NAMES = ("search", "edit", "schema", "import", "export")


def _info(setup_id: str = SETUP_ID, tool_names: tuple[str, ...] = TOOL_NAMES) -> ToolModuleInfo:
    return ToolModuleInfo(
        module_id="modules:kg",
        module_type=RegistryModuleType.TOOL_MODULE,
        address="tool-kg.internal",
        port=50055,
        version="0.1.0",
        module_name="Knowledge Graph",
        setup_id=setup_id,
        tool_name="Knowledge Graph",
        tools=[
            ToolDefinition(
                name=name,
                description=f"{name} tool",
                parameters_schema={"type": "object", "properties": {}, "required": []},
            )
            for name in tool_names
        ],
    )


def _context(*infos: ToolModuleInfo, dynamic: tuple[ToolModuleInfo, ...] = ()) -> Mock:
    """Mock ModuleContext with a real tool cache; create_tool_functions returns every cached tool.

    The tool callables are never invoked here, so a bare Mock stands in for each.
    """

    cache = ToolCache()
    for info in infos:
        cache.add(info)
    for info in dynamic:
        cache.add_dynamic(info)
    context = Mock()
    context.tool_cache = cache
    context.create_tool_functions = Mock(
        side_effect=lambda setup_id: [(td, Mock()) for td in cache.entries[setup_id].tools],
    )
    return context


def _names(toolkit: Toolkit) -> set[str]:
    """SDK tool names of a toolkit (function names are ``{slug}__{tool}``)."""
    return {name.split("__", 1)[1] for name in {**toolkit.functions, **toolkit.async_functions}}


def _select(setup_id: str = SETUP_ID, **triggers: bool) -> ToolSelection:
    return ToolSelection(setup_id=setup_id, triggers=triggers)


class TestForSelections:
    def test_only_enabled_triggers_become_tools(self) -> None:
        (toolkit,) = ModuleToolkits.for_selections(_context(_info()), [_select(search=True, edit=False)])
        assert _names(toolkit) == {"search"}

    def test_all_triggers_enabled_yields_full_catalog(self) -> None:
        (toolkit,) = ModuleToolkits.for_selections(_context(_info()), [_select(**dict.fromkeys(TOOL_NAMES, True))])
        assert _names(toolkit) == set(TOOL_NAMES)

    def test_all_triggers_disabled_yields_nothing(self) -> None:
        assert ModuleToolkits.for_selections(_context(_info()), [_select(**dict.fromkeys(TOOL_NAMES, False))]) == []

    def test_missing_setup_id_yields_nothing(self) -> None:
        assert ModuleToolkits.for_selections(_context(_info()), [_select("setups:ghost", search=True)]) == []

    def test_unknown_trigger_names_are_ignored(self) -> None:
        (toolkit,) = ModuleToolkits.for_selections(_context(_info()), [_select(search=True, ghost_tool=True)])
        assert _names(toolkit) == {"search"}

    def test_a_toolkit_that_fails_to_build_is_skipped(self) -> None:
        context = _context(_info())
        context.create_tool_functions = Mock(side_effect=RuntimeError("boom"))
        assert ModuleToolkits.for_selections(context, [_select(search=True)]) == []


class TestSharedSetupAcrossAgents:
    def test_each_agent_keeps_its_own_triggers_and_the_cache_is_untouched(self) -> None:
        info = _info()
        context = _context(info)

        head = ModuleToolkits.for_selections(context, [_select(**dict.fromkeys(TOOL_NAMES, True))])
        searcher = ModuleToolkits.for_selections(context, [_select(search=True)])
        editor = ModuleToolkits.for_selections(context, [_select(edit=True)])

        assert _names(head[0]) == set(TOOL_NAMES)
        assert _names(searcher[0]) == {"search"}
        assert _names(editor[0]) == {"edit"}
        assert head[0] is not searcher[0]
        assert [tool.name for tool in context.tool_cache.entries[SETUP_ID].tools] == list(TOOL_NAMES)


class TestForLoaded:
    def test_loaded_tools_are_attached_whole(self) -> None:
        (toolkit,) = ModuleToolkits.for_loaded(_context(dynamic=(_info("setups:loaded", ("a", "b")),)))
        assert _names(toolkit) == {"a", "b"}

    def test_declared_tools_are_not_loaded_tools(self) -> None:
        assert ModuleToolkits.for_loaded(_context(_info())) == []

    def test_excluded_setups_are_skipped(self) -> None:
        context = _context(dynamic=(_info("setups:loaded"),))
        assert ModuleToolkits.for_loaded(context, exclude={"setups:loaded"}) == []


class TestBuild:
    def test_selected_then_loaded_without_duplicating_a_selected_setup(self) -> None:
        context = _context(_info(), dynamic=(_info(SETUP_ID), _info("setups:loaded", ("a",))))

        toolkits = ModuleToolkits.build(context, [_select(search=True)])

        assert [_names(toolkit) for toolkit in toolkits] == [{"search"}, {"a"}]
