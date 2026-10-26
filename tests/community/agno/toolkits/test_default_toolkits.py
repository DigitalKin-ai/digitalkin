"""Tests for the DefaultToolkits assembler."""

from types import SimpleNamespace
from typing import Any

from agno.agent import Agent

from digitalkin.community.agno import AguiTools
from digitalkin.community.agno.toolkits import (
    ChatHistoryTools,
    DefaultToolkits,
    KinsManager,
    LoadManager,
    ServicesManager,
    ToolsManager,
    UserProfileTools,
)
from digitalkin.services.registry import DefaultRegistry
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.user_profile import DefaultUserProfile


def _context(setup: Any = None) -> SimpleNamespace:
    """Fake ModuleContext — build() touches user_profile, registry and setup."""
    return SimpleNamespace(
        user_profile=DefaultUserProfile("missions:m1", "", ""),
        registry=DefaultRegistry("", "", ""),
        setup=setup,
    )


def test_build_without_setup_omits_registry_managers() -> None:
    tools = DefaultToolkits.build(_context(), session_id="s1")  # type: ignore[arg-type]
    assert [type(t) for t in tools] == [ChatHistoryTools, UserProfileTools, LoadManager]


def test_build_with_setup_includes_three_managers_before_loader() -> None:
    tools = DefaultToolkits.build(_context(setup=DefaultSetup()), session_id="s1")  # type: ignore[arg-type]
    assert [type(t) for t in tools] == [
        ChatHistoryTools,
        UserProfileTools,
        ToolsManager,
        ServicesManager,
        KinsManager,
        LoadManager,
    ]


def test_build_binds_loader_to_the_live_tool_list() -> None:
    tools = DefaultToolkits.build(_context())  # type: ignore[arg-type]
    loader = tools[-1]
    assert isinstance(loader, LoadManager)
    # The loader must append to the exact list the agent's factory closes over.
    assert loader._base_tools is tools


def test_bind_host_wires_chat_history() -> None:
    tools = DefaultToolkits.build(_context())  # type: ignore[arg-type]
    host = object()
    DefaultToolkits.bind_host(tools, host)
    assert tools[0].host is host  # type: ignore[union-attr]


def test_build_appends_to_the_given_list_in_place() -> None:
    own = object()
    given: list[Any] = [own]
    tools = DefaultToolkits.build(_context(), session_id="s1", tools=given)  # type: ignore[arg-type]
    assert tools is given
    assert tools[0] is own
    assert [type(t) for t in tools[1:]] == [ChatHistoryTools, UserProfileTools, LoadManager]
    assert tools[-1]._base_tools is given  # type: ignore[union-attr]


def test_build_without_chat_history_for_sub_agents() -> None:
    tools = DefaultToolkits.build(_context(setup=DefaultSetup()), chat_history=False)  # type: ignore[arg-type]
    assert ChatHistoryTools not in {type(t) for t in tools}
    assert isinstance(tools[-1], LoadManager)


def test_build_skips_profile_when_no_user_profile_is_wired() -> None:
    context = _context()
    context.user_profile = None
    tools = DefaultToolkits.build(context, session_id="s1")  # type: ignore[arg-type]
    assert [type(t) for t in tools] == [ChatHistoryTools, LoadManager]


def test_bind_host_rebinds_the_loader_to_a_sub_agents_own_list() -> None:
    tools = DefaultToolkits.build(_context(), chat_history=False)  # type: ignore[arg-type]
    agent = Agent(tools=tools)
    DefaultToolkits.bind_host(tools, agent)
    # Agent copies a plain list; a load must land in the list its runs read.
    assert tools[-1]._base_tools is agent.tools  # type: ignore[union-attr]


def test_bind_host_keeps_the_factory_binding() -> None:
    tools = DefaultToolkits.build(_context())  # type: ignore[arg-type]
    agent = Agent(tools=AguiTools.make_tools_factory(tools), cache_callables=False)
    DefaultToolkits.bind_host(tools, agent)
    assert tools[-1]._base_tools is tools  # type: ignore[union-attr]
    assert tools[0].host is agent  # type: ignore[union-attr]
