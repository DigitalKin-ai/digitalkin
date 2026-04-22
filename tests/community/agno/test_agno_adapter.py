"""Full coverage tests for AgnoStreamAdapter.

The ``agno`` package is an optional dependency and is not installed in the
test environment. These tests inject fake ``agno.run.agent`` and
``agno.run.team`` modules into ``sys.modules`` so the adapter's lazy import
resolves to controllable enum members and namespace objects.
"""
# pyright: reportArgumentType=false, reportPrivateUsage=false, reportAttributeAccessIssue=false

from __future__ import annotations

import sys
import types
from enum import Enum
from typing import Any

import pytest

from digitalkin.models.events import (
    AgentRunEvent,
    ReasoningCompletedEvent,
    ReasoningContentDeltaEvent,
    ReasoningStartedEvent,
    ReasoningStepEvent,
    RunCompletedEvent,
    RunContentEvent,
    RunErrorEvent,
    RunStartedEvent,
    TextMessageCompletedEvent,
    TextMessageStartedEvent,
    ToolCallCompletedEvent,
    ToolCallErrorEvent,
    ToolCallStartedEvent,
)


class _FakeRunEvent(str, Enum):
    """Mirror of ``agno.run.agent.RunEvent`` for tests."""

    run_started = "RunStarted"
    run_content = "RunContent"
    run_completed = "RunCompleted"
    run_error = "RunError"
    run_paused = "RunPaused"
    reasoning_started = "ReasoningStarted"
    reasoning_content_delta = "ReasoningContentDelta"
    reasoning_step = "ReasoningStep"
    reasoning_completed = "ReasoningCompleted"
    tool_call_started = "ToolCallStarted"
    tool_call_completed = "ToolCallCompleted"
    tool_call_error = "ToolCallError"


class _FakeTeamRunEvent(str, Enum):
    """Mirror of ``agno.run.team.TeamRunEvent`` for tests."""

    run_started = "TeamRunStarted"
    run_content = "TeamRunContent"
    run_completed = "TeamRunCompleted"
    run_error = "TeamRunError"
    run_paused = "TeamRunPaused"
    reasoning_started = "TeamReasoningStarted"
    reasoning_content_delta = "TeamReasoningContentDelta"
    reasoning_step = "TeamReasoningStep"
    reasoning_completed = "TeamReasoningCompleted"
    tool_call_started = "TeamToolCallStarted"
    tool_call_completed = "TeamToolCallCompleted"
    tool_call_error = "TeamToolCallError"


@pytest.fixture(autouse=True)
def fake_agno_modules() -> Any:
    """Install fake ``agno.run.agent`` and ``agno.run.team`` modules.

    Yields:
        Tuple ``(_FakeRunEvent, _FakeTeamRunEvent)`` for convenience.
    """
    saved = {k: sys.modules.get(k) for k in ("agno", "agno.run", "agno.run.agent", "agno.run.team")}

    agno_pkg = types.ModuleType("agno")
    agno_run_pkg = types.ModuleType("agno.run")
    agno_run_agent = types.ModuleType("agno.run.agent")
    agno_run_agent.RunEvent = _FakeRunEvent  # type: ignore[attr-defined]
    agno_run_team = types.ModuleType("agno.run.team")
    agno_run_team.TeamRunEvent = _FakeTeamRunEvent  # type: ignore[attr-defined]

    sys.modules["agno"] = agno_pkg
    sys.modules["agno.run"] = agno_run_pkg
    sys.modules["agno.run.agent"] = agno_run_agent
    sys.modules["agno.run.team"] = agno_run_team

    try:
        yield _FakeRunEvent, _FakeTeamRunEvent
    finally:
        for key, mod in saved.items():
            if mod is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = mod


_EVENT_DEFAULTS: dict[str, Any] = {
    "timestamp": 1234.5,
    "run_id": None,
    "session_id": None,
    "parent_run_id": None,
    "content": None,
    "reasoning_content": None,
    "tool": None,
    "tools": None,
    "requirements": None,
    "error_type": None,
    "team_name": None,
    "team_id": None,
    "agent_name": None,
    "agent_id": None,
}

_TOOL_DEFAULTS: dict[str, Any] = {
    "tool_call_id": None,
    "tool_name": None,
    "tool_args": None,
    "result": None,
}

_TOOL_EXEC_DEFAULTS: dict[str, Any] = {
    "tool_call_id": None,
    "tool_name": None,
    "tool_args": None,
    "external_execution_required": False,
}


def _make_event(event: Any, **attrs: Any) -> types.SimpleNamespace:
    """Build a namespace mimicking an Agno event object with Pydantic-like defaults."""
    data = {**_EVENT_DEFAULTS, **attrs}
    return types.SimpleNamespace(event=event, **data)


def _make_tool(**attrs: Any) -> types.SimpleNamespace:
    """Build a namespace mimicking an Agno ``ToolExecution`` (for tool_call_* events)."""
    data = {**_TOOL_DEFAULTS, **attrs}
    return types.SimpleNamespace(**data)


def _make_tool_execution(**attrs: Any) -> types.SimpleNamespace:
    """Build a namespace mimicking an Agno ``ToolExecution`` attached to ``RunPausedEvent``."""
    data = {**_TOOL_EXEC_DEFAULTS, **attrs}
    return types.SimpleNamespace(**data)


# ── Import / ImportError ────────────────────────────────────────────────────


def test_import_error_when_agno_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """First conversion raises ImportError with install hint if agno absent."""
    for key in ("agno", "agno.run", "agno.run.agent", "agno.run.team"):
        monkeypatch.delitem(sys.modules, key, raising=False)

    real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__  # type: ignore[index]

    def _blocked_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("agno"):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _blocked_import)

    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    event = _make_event("anything")

    with pytest.raises(ImportError, match="agno"):
        adapter.to_digitalkin_events(event)


def test_dispatch_is_built_once() -> None:
    """Dispatch table is lazily initialized on first call, reused on subsequent ones."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    assert adapter._dispatch is None

    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))
    dispatch_first = adapter._dispatch
    assert dispatch_first is not None

    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_error))
    assert adapter._dispatch is dispatch_first


def test_unhandled_event_returns_empty() -> None:
    """An event type absent from the dispatch table yields no DigitalKin events."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))

    result = adapter.to_digitalkin_events(_make_event("unknown_event_type"))
    assert result == []


# ── Run lifecycle ───────────────────────────────────────────────────────────


def test_run_started_emits_event() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_started, run_id="r1", session_id="t1"),
    )

    assert len(result) == 1
    event = result[0]
    assert isinstance(event, RunStartedEvent)
    assert event.run_id == "r1"
    assert event.thread_id == "t1"
    assert event.timestamp == 1234.5
    assert adapter._active_run_id == "r1"


def test_run_started_deduplicates_same_run_id() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))

    duplicate = adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))
    assert duplicate == []


def test_run_started_without_run_id_is_not_deduped() -> None:
    """``run_id is None`` bypasses the dedup guard and always emits."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    first = adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id=None))
    second = adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id=None))
    assert len(first) == 1
    assert len(second) == 1


def test_run_completed_closes_active_sequences_and_emits() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="think", content=None),
    )
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hi"),
    )
    assert adapter._content_active is True
    assert adapter._reasoning_active is False

    # Re-open reasoning so run_completed has both to close.
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="think2", content=None),
    )
    assert adapter._content_active is False
    assert adapter._reasoning_active is True

    completed = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_completed, run_id="r1", content="final"),
    )

    kinds = [type(e) for e in completed]
    assert ReasoningCompletedEvent in kinds
    assert RunCompletedEvent in kinds
    final = next(e for e in completed if isinstance(e, RunCompletedEvent))
    assert final.final_content == "final"
    assert final.run_id == "r1"
    assert "r1" in adapter._completed_run_ids
    assert adapter._active_run_id is None


def test_run_completed_closes_only_active_text() -> None:
    """When only text is active at run end, run_completed closes it then emits RunCompleted."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hi"),
    )
    assert adapter._content_active is True
    assert adapter._reasoning_active is False

    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_completed, run_id="r1", content="final"),
    )
    kinds = [type(e) for e in result]
    assert TextMessageCompletedEvent in kinds
    assert RunCompletedEvent in kinds


def test_run_completed_without_content() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_completed, run_id="r1", content=None),
    )
    event = next(e for e in result if isinstance(e, RunCompletedEvent))
    assert event.final_content is None


def test_run_completed_deduplicates() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_completed, run_id="r1", content=None))

    duplicate = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_completed, run_id="r1", content=None),
    )
    assert duplicate == []


def test_run_completed_without_run_id_still_emits() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id=None))
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_completed, run_id=None, content=None),
    )
    assert any(isinstance(e, RunCompletedEvent) for e in result)


def test_run_error_emits_error_event() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_error, error_type="ValueError", content="boom"),
    )
    assert len(result) == 1
    event = result[0]
    assert isinstance(event, RunErrorEvent)
    assert event.error_type == "ValueError"
    assert event.content == "boom"


def test_run_error_without_content() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_error, error_type=None, content=None),
    )
    event = result[0]
    assert isinstance(event, RunErrorEvent)
    assert event.content is None


# ── Reasoning explicit handlers ─────────────────────────────────────────────


def test_reasoning_started_closes_active_content() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hello"),
    )
    assert adapter._content_active is True

    result = adapter.to_digitalkin_events(_make_event(_FakeRunEvent.reasoning_started))

    kinds = [type(e) for e in result]
    assert TextMessageCompletedEvent in kinds
    assert ReasoningStartedEvent in kinds
    assert adapter._reasoning_active is True
    assert adapter._current_reasoning_id is not None


def test_reasoning_content_delta_passes_through() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.reasoning_started))
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.reasoning_content_delta, reasoning_content="step"),
    )
    assert len(result) == 1
    assert isinstance(result[0], ReasoningContentDeltaEvent)
    assert result[0].delta == "step"
    assert result[0].reasoning_id == adapter._current_reasoning_id


def test_reasoning_content_delta_without_content_defaults_to_empty() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.reasoning_started))
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.reasoning_content_delta, reasoning_content=None),
    )
    assert isinstance(result[0], ReasoningContentDeltaEvent)
    assert result[0].delta == ""


def test_reasoning_step_reuses_active_reasoning() -> None:
    """When reasoning is already active, reasoning_step appends without reopening."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.reasoning_started))
    rid = adapter._current_reasoning_id
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.reasoning_step, reasoning_content="step body"),
    )
    assert len(result) == 1
    assert isinstance(result[0], ReasoningStepEvent)
    assert result[0].delta == "step body"
    assert result[0].reasoning_id == rid


def test_reasoning_step_auto_opens_lifecycle() -> None:
    """A reasoning_step without prior reasoning_started must auto-open the lifecycle."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.reasoning_step, reasoning_content="step"),
    )
    kinds = [type(e) for e in result]
    assert kinds == [ReasoningStartedEvent, ReasoningStepEvent]
    assert adapter._reasoning_active is True


def test_reasoning_step_closes_active_content() -> None:
    """A reasoning_step while text is active closes the text message first."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hi"),
    )
    assert adapter._content_active is True

    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.reasoning_step, reasoning_content="step"),
    )
    kinds = [type(e) for e in result]
    assert kinds[0] is TextMessageCompletedEvent
    assert ReasoningStartedEvent in kinds
    assert ReasoningStepEvent in kinds


def test_reasoning_step_empty_content_ignored() -> None:
    """A reasoning_step with empty/absent content produces no events."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.reasoning_step, reasoning_content=""),
    )
    assert result == []
    assert adapter._reasoning_active is False


def test_multiple_reasoning_steps_share_lifecycle() -> None:
    """Two consecutive reasoning_steps produce one lifecycle."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    first = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.reasoning_step, reasoning_content="a"),
    )
    second = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.reasoning_step, reasoning_content="b"),
    )
    kinds_first = [type(e) for e in first]
    kinds_second = [type(e) for e in second]
    assert kinds_first == [ReasoningStartedEvent, ReasoningStepEvent]
    assert kinds_second == [ReasoningStepEvent]


def test_reasoning_completed_when_active() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.reasoning_started))
    result = adapter.to_digitalkin_events(_make_event(_FakeRunEvent.reasoning_completed))
    assert len(result) == 1
    assert isinstance(result[0], ReasoningCompletedEvent)
    assert adapter._reasoning_active is False


def test_reasoning_completed_when_inactive_returns_empty() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(_make_event(_FakeRunEvent.reasoning_completed))
    assert result == []


# ── Tool call handlers ──────────────────────────────────────────────────────


def test_tool_call_started_closes_reasoning_and_content() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hi"),
    )
    adapter._reasoning_active = True
    adapter._current_reasoning_id = "rid"

    tool = _make_tool(tool_call_id="tc1", tool_name="search", tool_args={"q": "x"})
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.tool_call_started, tool=tool),
    )

    kinds = [type(e) for e in result]
    assert ReasoningCompletedEvent in kinds
    assert TextMessageCompletedEvent in kinds
    assert ToolCallStartedEvent in kinds
    started = next(e for e in result if isinstance(e, ToolCallStartedEvent))
    assert started.tool is not None
    assert started.tool.tool_call_id == "tc1"
    assert started.tool.tool_name == "search"
    assert started.tool.tool_args == {"q": "x"}


def test_tool_call_started_without_tool() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.tool_call_started, tool=None),
    )
    assert len(result) == 1
    assert isinstance(result[0], ToolCallStartedEvent)
    assert result[0].tool is None


def test_tool_call_completed() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    tool = _make_tool(tool_call_id="tc1", tool_name="search", tool_args={"q": "x"}, result="ok")
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.tool_call_completed, tool=tool, content="ok"),
    )
    assert len(result) == 1
    event = result[0]
    assert isinstance(event, ToolCallCompletedEvent)
    assert event.tool is not None
    assert event.tool.result == "ok"
    assert event.content == "ok"
    assert "tc1" in adapter._closed_tool_call_ids


def test_tool_call_completed_without_tool_or_content() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.tool_call_completed, tool=None, content=None),
    )
    event = result[0]
    assert isinstance(event, ToolCallCompletedEvent)
    assert event.tool is None
    assert event.content is None


def test_tool_call_error_first_time() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    tool = _make_tool(tool_call_id="tc1", tool_name="search")
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.tool_call_error, tool=tool, content="boom"),
    )
    assert len(result) == 1
    event = result[0]
    assert isinstance(event, ToolCallErrorEvent)
    assert event.error_message == "boom"
    assert "tc1" in adapter._closed_tool_call_ids


def test_tool_call_error_after_completed_is_deduped() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    tool = _make_tool(tool_call_id="tc1", tool_name="search", tool_args=None, result="ok")
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.tool_call_completed, tool=tool, content="ok"),
    )
    duplicate = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.tool_call_error, tool=tool, content="boom"),
    )
    assert duplicate == []


def test_tool_call_error_without_tool() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.tool_call_error, tool=None, content="boom"),
    )
    event = result[0]
    assert isinstance(event, ToolCallErrorEvent)
    assert event.tool is None
    assert event.error_message == "boom"


def test_tool_call_error_without_tool_call_id_and_without_content() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    tool = _make_tool(tool_call_id=None, tool_name="search")
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.tool_call_error, tool=tool, content=None),
    )
    event = result[0]
    assert isinstance(event, ToolCallErrorEvent)
    assert event.tool is not None
    assert event.tool.tool_call_id is None
    assert event.error_message is None


# ── HITL pause (run_paused) ─────────────────────────────────────────────────


def test_run_paused_synthesizes_tool_events_for_external_tool() -> None:
    """A single external tool yields a synthesized ToolCallStarted + ToolCallCompleted pair."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    tool = _make_tool_execution(
        tool_call_id="ext1",
        tool_name="get_weather",
        tool_args={"city": "Lyon"},
        external_execution_required=True,
    )
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_paused, tools=[tool], requirements=[]),
    )
    kinds = [type(e) for e in result]
    assert kinds == [ToolCallStartedEvent, ToolCallCompletedEvent]
    started = result[0]
    completed = result[1]
    assert isinstance(started, ToolCallStartedEvent)
    assert isinstance(completed, ToolCallCompletedEvent)
    assert started.tool is not None
    assert started.tool.tool_name == "get_weather"
    assert completed.content is None
    assert completed.tool is not None
    assert completed.tool.result is None
    assert adapter.is_paused is True
    assert "ext1" in adapter._closed_tool_call_ids


def test_run_paused_skips_backend_only_tools() -> None:
    """Server-side tools (external_execution_required=False) must not be synthesized."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    backend = _make_tool_execution(
        tool_call_id="tc-think",
        tool_name="think",
        external_execution_required=False,
    )
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_paused, tools=[backend], requirements=[]),
    )
    tool_events = [e for e in result if isinstance(e, (ToolCallStartedEvent, ToolCallCompletedEvent))]
    assert tool_events == []
    assert adapter.is_paused is True


def test_run_paused_deduplicates_repeated_tool_call_ids() -> None:
    """Agno may accumulate tools across yields; duplicate ids emit only one pair."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    ext1 = _make_tool_execution(
        tool_call_id="ext-ask",
        tool_name="ask_question",
        external_execution_required=True,
    )
    ext1_dup = _make_tool_execution(
        tool_call_id="ext-ask",
        tool_name="ask_question",
        external_execution_required=True,
    )
    ext2 = _make_tool_execution(
        tool_call_id="ext-map",
        tool_name="show_map",
        external_execution_required=True,
    )
    backend = _make_tool_execution(
        tool_call_id="tc-think",
        tool_name="think",
        external_execution_required=False,
    )
    result = adapter.to_digitalkin_events(
        _make_event(
            _FakeRunEvent.run_paused,
            tools=[backend, ext1, ext1_dup, ext2],
            requirements=[],
        ),
    )
    kinds = [type(e) for e in result]
    assert kinds == [
        ToolCallStartedEvent,
        ToolCallCompletedEvent,
        ToolCallStartedEvent,
        ToolCallCompletedEvent,
    ]
    names = [e.tool.tool_name for e in result if e.tool is not None]
    assert names == ["ask_question", "ask_question", "show_map", "show_map"]


def test_run_paused_skips_external_tool_without_tool_call_id() -> None:
    """External tools missing ``tool_call_id`` cannot be identified; they are skipped."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    tool = _make_tool_execution(
        tool_call_id=None,
        tool_name="anon",
        external_execution_required=True,
    )
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_paused, tools=[tool], requirements=[]),
    )
    tool_events = [e for e in result if isinstance(e, (ToolCallStartedEvent, ToolCallCompletedEvent))]
    assert tool_events == []
    assert adapter.is_paused is True


def test_run_paused_closes_active_content_and_reasoning() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    # Open text content
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="thinking"),
    )
    # Force reasoning active too to exercise both branches
    adapter._reasoning_active = True
    adapter._current_reasoning_id = "rid"

    tool = _make_tool_execution(
        tool_call_id="ext1",
        tool_name="ask",
        external_execution_required=True,
    )
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_paused, tools=[tool], requirements=[]),
    )
    kinds = [type(e) for e in result]
    assert kinds[0] is ReasoningCompletedEvent
    assert TextMessageCompletedEvent in kinds
    idx_close_text = kinds.index(TextMessageCompletedEvent)
    idx_start_tool = kinds.index(ToolCallStartedEvent)
    assert idx_close_text < idx_start_tool


def test_run_paused_records_paused_state_and_properties() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    assert adapter.is_paused is False
    assert adapter.paused_tool_executions == []
    assert adapter.paused_requirements == []

    tool = _make_tool_execution(
        tool_call_id="ext1",
        tool_name="ask",
        external_execution_required=True,
    )
    requirement = types.SimpleNamespace(tool_execution=tool)
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_paused, tools=[tool], requirements=[requirement]),
    )

    assert adapter.is_paused is True
    assert len(adapter.paused_tool_executions) == 1
    assert adapter.paused_tool_executions[0] is tool
    assert len(adapter.paused_requirements) == 1
    assert adapter.paused_requirements[0] is requirement


def test_run_paused_with_null_tools_and_requirements_lists() -> None:
    """``tools=None`` and ``requirements=None`` are normalised to empty lists."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_paused, tools=None, requirements=None),
    )
    assert result == []
    assert adapter.is_paused is True
    assert adapter.paused_tool_executions == []
    assert adapter.paused_requirements == []


# ── run_content state machine ───────────────────────────────────────────────


def test_run_content_text_auto_opens_and_deltas() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hello"),
    )
    assert isinstance(result[0], TextMessageStartedEvent)
    assert isinstance(result[1], RunContentEvent)
    assert result[1].content == "hello"
    assert result[1].message_id == adapter._current_message_id

    result2 = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content=" world"),
    )
    assert len(result2) == 1
    assert isinstance(result2[0], RunContentEvent)


def test_run_content_reasoning_auto_opens_and_deltas() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="think", content=None),
    )
    assert isinstance(result[0], ReasoningStartedEvent)
    assert isinstance(result[1], ReasoningContentDeltaEvent)

    result2 = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="more", content=None),
    )
    assert len(result2) == 1
    assert isinstance(result2[0], ReasoningContentDeltaEvent)


def test_run_content_transition_reasoning_to_text_closes_reasoning() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="think", content=None),
    )
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hi"),
    )
    kinds = [type(e) for e in result]
    assert ReasoningCompletedEvent in kinds
    assert TextMessageStartedEvent in kinds
    assert RunContentEvent in kinds


def test_run_content_transition_text_to_reasoning_closes_text() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hi"),
    )
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="think", content=None),
    )
    kinds = [type(e) for e in result]
    assert TextMessageCompletedEvent in kinds
    assert ReasoningStartedEvent in kinds
    assert ReasoningContentDeltaEvent in kinds


def test_run_content_empty_content_closes_active_text() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hi"),
    )
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content=""),
    )
    assert len(result) == 1
    assert isinstance(result[0], TextMessageCompletedEvent)
    assert adapter._content_active is False


def test_run_content_empty_content_when_inactive_is_noop() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content=""),
    )
    assert result == []


def test_run_content_empty_reasoning_closes_active_reasoning() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="think", content=None),
    )
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="", content=None),
    )
    assert len(result) == 1
    assert isinstance(result[0], ReasoningCompletedEvent)
    assert adapter._reasoning_active is False


def test_run_content_empty_reasoning_when_inactive_is_noop() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="", content=None),
    )
    assert result == []


def test_run_content_both_none_is_debug_noop() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content=None),
    )
    assert result == []


def test_run_content_reasoning_after_explicit_started() -> None:
    """When reasoning_started already fired, a reasoning delta must not re-open."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeRunEvent.reasoning_started))
    rid = adapter._current_reasoning_id
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="step", content=None),
    )
    assert len(result) == 1
    assert isinstance(result[0], ReasoningContentDeltaEvent)
    assert adapter._current_reasoning_id == rid


# ── flush() ─────────────────────────────────────────────────────────────────


def test_close_content_noop_when_inactive() -> None:
    """Direct call returns empty list (defensive early exit in ``_close_content``)."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    assert adapter._close_content(None) == []


def test_close_reasoning_noop_when_inactive() -> None:
    """Direct call returns empty list (defensive early exit in ``_close_reasoning``)."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    assert adapter._close_reasoning(None) == []


def test_flush_empty() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    assert adapter.flush() == []


def test_flush_closes_active_content() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hi"),
    )
    result = adapter.flush()
    assert len(result) == 1
    assert isinstance(result[0], TextMessageCompletedEvent)
    assert adapter._content_active is False


def test_flush_closes_active_reasoning() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="think", content=None),
    )
    result = adapter.flush()
    assert len(result) == 1
    assert isinstance(result[0], ReasoningCompletedEvent)
    assert adapter._reasoning_active is False


def test_flush_closes_both_when_both_forced() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content="think", content=None),
    )
    # Force content active too (normally mutually exclusive) to exercise both branches
    adapter._content_active = True
    adapter._current_message_id = "m1"

    result = adapter.flush()
    kinds = [type(e) for e in result]
    assert TextMessageCompletedEvent in kinds
    assert ReasoningCompletedEvent in kinds


def test_double_flush_is_idempotent() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="hi"),
    )
    adapter.flush()
    assert adapter.flush() == []


# ── Team events share the same dispatch ─────────────────────────────────────


def test_team_run_started_dispatches() -> None:
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeTeamRunEvent.run_started, run_id="team-r1", session_id="t-t1"),
    )
    assert isinstance(result[0], RunStartedEvent)
    assert result[0].run_id == "team-r1"
    assert result[0].thread_id == "t-t1"


def test_team_events_route_like_agent_events() -> None:
    """Smoke-test that each TeamRunEvent key resolves to the matching handler."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(_make_event(_FakeTeamRunEvent.run_started, run_id="r1"))
    adapter.to_digitalkin_events(
        _make_event(_FakeTeamRunEvent.run_content, reasoning_content="r", content=None),
    )
    adapter.to_digitalkin_events(
        _make_event(_FakeTeamRunEvent.reasoning_step, reasoning_content="s"),
    )
    adapter.to_digitalkin_events(_make_event(_FakeTeamRunEvent.reasoning_completed))
    tool = _make_tool(tool_call_id="tc", tool_name="t", tool_args=None, result="r")
    adapter.to_digitalkin_events(_make_event(_FakeTeamRunEvent.tool_call_started, tool=tool))
    adapter.to_digitalkin_events(
        _make_event(_FakeTeamRunEvent.tool_call_completed, tool=tool, content=None),
    )
    err_tool = _make_tool(tool_call_id="tc2", tool_name="t")
    adapter.to_digitalkin_events(
        _make_event(_FakeTeamRunEvent.tool_call_error, tool=err_tool, content="x"),
    )
    adapter.to_digitalkin_events(
        _make_event(_FakeTeamRunEvent.run_error, error_type=None, content=None),
    )
    ext_tool = _make_tool_execution(
        tool_call_id="ext-team",
        tool_name="frontend",
        external_execution_required=True,
    )
    paused = adapter.to_digitalkin_events(
        _make_event(_FakeTeamRunEvent.run_paused, tools=[ext_tool], requirements=[]),
    )
    assert any(isinstance(e, ToolCallStartedEvent) for e in paused)
    completed = adapter.to_digitalkin_events(
        _make_event(_FakeTeamRunEvent.run_completed, run_id="r1", content=None),
    )
    assert any(isinstance(e, RunCompletedEvent) for e in completed)


# ── Metadata propagation ────────────────────────────────────────────────────


def test_metadata_agent_event_has_source_and_identity() -> None:
    """Agent-scoped events populate ``metadata`` with agent identity."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(
            _FakeRunEvent.run_started,
            run_id="r1",
            agent_id="a1",
            agent_name="Alice",
            parent_run_id="team-r1",
        ),
    )
    assert result[0].metadata == {
        "source": "agent",
        "name": "Alice",
        "id": "a1",
        "parent_run_id": "team-r1",
    }


def test_metadata_team_event_has_source_and_identity() -> None:
    """Team-scoped events populate ``metadata`` with team identity."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(
            _FakeTeamRunEvent.run_started,
            run_id="tr1",
            team_id="t1",
            team_name="CrewA",
        ),
    )
    assert result[0].metadata == {
        "source": "team",
        "name": "CrewA",
        "id": "t1",
        "parent_run_id": None,
    }


def test_metadata_does_not_duplicate_run_id() -> None:
    """``run_id`` is already on typed fields and must not appear in metadata."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_started, run_id="r1", agent_name="Alice"),
    )
    event = result[0]
    metadata = event.metadata or {}
    assert "run_id" not in metadata
    assert isinstance(event, RunStartedEvent)
    assert event.run_id == "r1"


def test_metadata_not_polluted_by_unhandled_event() -> None:
    """Unhandled events must not overwrite the cached ``_last_metadata``."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_started, run_id="r1", agent_name="Alice", agent_id="a1"),
    )
    snapshot = dict(adapter._last_metadata or {})
    assert snapshot["name"] == "Alice"

    adapter.to_digitalkin_events(_make_event("completely_unknown_event"))

    assert adapter._last_metadata == snapshot


def test_metadata_propagates_to_text_message_events() -> None:
    """Text message sequence events inherit the emitter's metadata."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    adapter.to_digitalkin_events(
        _make_event(_FakeRunEvent.run_started, run_id="r1", agent_name="Alice", agent_id="a1"),
    )
    result = adapter.to_digitalkin_events(
        _make_event(
            _FakeRunEvent.run_content,
            agent_name="Alice",
            agent_id="a1",
            content="hello",
        ),
    )
    for event in result:
        assert (event.metadata or {}).get("name") == "Alice"
        assert (event.metadata or {}).get("source") == "agent"


def test_metadata_switches_between_team_and_agent_events() -> None:
    """Switching emitters between events yields distinct metadata dicts."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    team_result = adapter.to_digitalkin_events(
        _make_event(_FakeTeamRunEvent.run_started, run_id="tr1", team_id="t1", team_name="CrewA"),
    )
    assert (team_result[0].metadata or {})["source"] == "team"

    agent_result = adapter.to_digitalkin_events(
        _make_event(
            _FakeRunEvent.run_content,
            agent_id="a1",
            agent_name="Alice",
            parent_run_id="tr1",
            content="hi",
        ),
    )
    for event in agent_result:
        metadata = event.metadata or {}
        assert metadata["source"] == "agent"
        assert metadata["name"] == "Alice"
        assert metadata["parent_run_id"] == "tr1"


# ── Full realistic sequences ────────────────────────────────────────────────


def test_realistic_sequence_with_reasoning_text_tool_and_pause() -> None:
    """think → reasoning_step → text → search → analyze → reasoning_step → text → frontend pause."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    events: list[Any] = []

    events.extend(adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1")))
    events.extend(
        adapter.to_digitalkin_events(
            _make_event(
                _FakeRunEvent.tool_call_started,
                tool=_make_tool(tool_call_id="tc-think", tool_name="think"),
            ),
        ),
    )
    events.extend(
        adapter.to_digitalkin_events(
            _make_event(
                _FakeRunEvent.tool_call_completed,
                tool=_make_tool(tool_call_id="tc-think", tool_name="think", result="planned"),
                content="planned",
            ),
        ),
    )
    events.extend(
        adapter.to_digitalkin_events(
            _make_event(_FakeRunEvent.reasoning_step, reasoning_content="## Plan"),
        ),
    )
    events.extend(
        adapter.to_digitalkin_events(
            _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="Searching..."),
        ),
    )
    events.extend(
        adapter.to_digitalkin_events(
            _make_event(
                _FakeRunEvent.tool_call_started,
                tool=_make_tool(tool_call_id="tc-search", tool_name="web_search"),
            ),
        ),
    )
    events.extend(
        adapter.to_digitalkin_events(
            _make_event(
                _FakeRunEvent.tool_call_completed,
                tool=_make_tool(tool_call_id="tc-search", tool_name="web_search", result="results"),
                content="results",
            ),
        ),
    )
    events.extend(
        adapter.to_digitalkin_events(
            _make_event(_FakeRunEvent.reasoning_step, reasoning_content="## Analysis"),
        ),
    )
    events.extend(
        adapter.to_digitalkin_events(
            _make_event(_FakeRunEvent.run_content, reasoning_content=None, content="Here!"),
        ),
    )
    backend = _make_tool_execution(
        tool_call_id="tc-think",
        tool_name="think",
        external_execution_required=False,
    )
    frontend = _make_tool_execution(
        tool_call_id="ext-show",
        tool_name="show_sources",
        external_execution_required=True,
    )
    events.extend(
        adapter.to_digitalkin_events(
            _make_event(_FakeRunEvent.run_paused, tools=[backend, frontend], requirements=[]),
        ),
    )

    kinds = [type(e) for e in events]

    # Reasoning lifecycle: each REASONING_STARTED has a matching REASONING_COMPLETED
    starts = [i for i, k in enumerate(kinds) if k is ReasoningStartedEvent]
    ends = [i for i, k in enumerate(kinds) if k is ReasoningCompletedEvent]
    assert len(starts) == len(ends)
    for start, end in zip(starts, ends, strict=True):
        assert start < end

    # Text message lifecycle balanced
    text_starts = [i for i, k in enumerate(kinds) if k is TextMessageStartedEvent]
    text_ends = [i for i, k in enumerate(kinds) if k is TextMessageCompletedEvent]
    assert len(text_starts) == len(text_ends)

    # Paused at the end with exactly one synthesised external tool pair
    assert adapter.is_paused is True
    synthesised = [
        e for e in events[-2:] if isinstance(e, (ToolCallStartedEvent, ToolCallCompletedEvent))
    ]
    assert len(synthesised) == 2


# ── Enum value exposure ─────────────────────────────────────────────────────


def test_event_types_use_enum_values_in_serialization() -> None:
    """Pydantic config ``use_enum_values`` keeps payloads as plain strings."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    result = adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_started, run_id="r1"))
    assert result[0].event == AgentRunEvent.RUN_STARTED.value
