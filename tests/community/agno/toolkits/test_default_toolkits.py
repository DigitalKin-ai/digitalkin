"""Tests for the DefaultToolkits assembler."""

from types import SimpleNamespace
from typing import Any

from digitalkin.community.agno.toolkits import (
    ChatHistoryTools,
    DefaultToolkits,
    RegistryTools,
    SetupTools,
    ToolLoaderTools,
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


def test_build_without_setup_omits_setup_tools() -> None:
    tools = DefaultToolkits.build(_context(), session_id="s1")  # type: ignore[arg-type]
    assert [type(t) for t in tools] == [ChatHistoryTools, UserProfileTools, RegistryTools, ToolLoaderTools]


def test_build_with_setup_includes_setup_tools_before_loader() -> None:
    tools = DefaultToolkits.build(_context(setup=DefaultSetup()), session_id="s1")  # type: ignore[arg-type]
    assert [type(t) for t in tools] == [
        ChatHistoryTools,
        UserProfileTools,
        RegistryTools,
        SetupTools,
        ToolLoaderTools,
    ]


def test_build_binds_loader_to_the_live_tool_list() -> None:
    tools = DefaultToolkits.build(_context())  # type: ignore[arg-type]
    loader = tools[-1]
    assert isinstance(loader, ToolLoaderTools)
    # The loader must append to the exact list the agent's factory closes over.
    assert loader._base_tools is tools


def test_bind_host_wires_chat_history() -> None:
    tools = DefaultToolkits.build(_context())  # type: ignore[arg-type]
    host = object()
    DefaultToolkits.bind_host(tools, host)
    assert tools[0].host is host  # type: ignore[union-attr]
