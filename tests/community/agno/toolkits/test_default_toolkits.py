"""Tests for the DefaultToolkits assembler."""

from types import SimpleNamespace

from digitalkin.community.agno.toolkits import (
    ChatHistoryTools,
    DefaultToolkits,
    RegistryTools,
    UserProfileTools,
)
from digitalkin.services.registry import DefaultRegistry
from digitalkin.services.user_profile import DefaultUserProfile


def _context() -> SimpleNamespace:
    """Fake ModuleContext — build() only touches user_profile and registry."""
    return SimpleNamespace(
        user_profile=DefaultUserProfile("missions:m1", "", ""),
        registry=DefaultRegistry("", "", ""),
    )


def test_build_returns_three_toolkits_in_order() -> None:
    tools = DefaultToolkits.build(_context(), session_id="s1")  # type: ignore[arg-type]
    assert [type(t) for t in tools] == [ChatHistoryTools, UserProfileTools, RegistryTools]


def test_bind_host_wires_chat_history() -> None:
    tools = DefaultToolkits.build(_context())  # type: ignore[arg-type]
    host = object()
    DefaultToolkits.bind_host(tools, host)
    assert tools[0].host is host  # type: ignore[union-attr]
