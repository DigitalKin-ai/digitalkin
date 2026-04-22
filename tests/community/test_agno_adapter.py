"""Tests for AgnoStreamAdapter — Agno events → DigitalKin events.

Covers:
- Run lifecycle (started, completed, error, duplicates)
- Reasoning lifecycle (native: started/delta/completed)
- Reasoning via ReasoningTools (reasoning_step auto-wrapping)
- Text content (auto-open/close text messages)
- Tool calls (started, completed, error, deduplication)
- HITL pause (run_paused → synthesized tool calls + is_paused)
- State transitions & overlaps (reasoning→content, content→tool, reasoning→tool, etc.)
- Flush (close dangling sequences)
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from digitalkin.models.events import AgentRunEvent

# Lazy-import guard: the adapter imports agno at first use.
# We mock RunEvent with a SimpleNamespace so tests don't need agno installed.


def _make_event(event_type: str, **kwargs: Any) -> SimpleNamespace:
    """Build a fake Agno event (duck-typed)."""
    return SimpleNamespace(event=event_type, timestamp=1234, **kwargs)


# ── Agno RunEvent values (mirrors agno.run.agent.RunEvent enum) ──────────

# We patch the dispatch dict directly so we don't need the real agno package.


class _FakeRunEvent:
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


def _create_adapter():
    """Create an adapter with the dispatch table pre-initialized (no agno import)."""
    from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

    adapter = AgnoStreamAdapter()
    # Force-init the dispatch table without importing agno
    adapter._dispatch = {
        _FakeRunEvent.run_started: adapter._handle_run_started,
        _FakeRunEvent.run_content: adapter._handle_run_content,
        _FakeRunEvent.run_completed: adapter._handle_run_completed,
        _FakeRunEvent.run_error: adapter._handle_run_error,
        _FakeRunEvent.run_paused: adapter._handle_run_paused,
        _FakeRunEvent.reasoning_started: adapter._handle_reasoning_started,
        _FakeRunEvent.reasoning_content_delta: adapter._handle_reasoning_content_delta,
        _FakeRunEvent.reasoning_step: adapter._handle_reasoning_step,
        _FakeRunEvent.reasoning_completed: adapter._handle_reasoning_completed,
        _FakeRunEvent.tool_call_started: adapter._handle_tool_call_started,
        _FakeRunEvent.tool_call_completed: adapter._handle_tool_call_completed,
        _FakeRunEvent.tool_call_error: adapter._handle_tool_call_error,
    }
    return adapter


def _event_types(events) -> list[str]:
    """Extract event type strings for easy assertion."""
    return [e.event.value if hasattr(e.event, "value") else str(e.event) for e in events]


# ═══════════════════════════════════════════════════════════════════════════
# 1. RUN LIFECYCLE
# ═══════════════════════════════════════════════════════════════════════════


class TestRunLifecycle:
    def test_run_started(self):
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(_make_event("RunStarted", run_id="r1", thread_id="t1"))
        assert _event_types(events) == [AgentRunEvent.RUN_STARTED]
        assert events[0].run_id == "r1"
        assert events[0].thread_id == "t1"

    def test_run_started_duplicate_skipped(self):
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("RunStarted", run_id="r1", thread_id="t1"))
        events = adapter.to_digitalkin_events(_make_event("RunStarted", run_id="r1", thread_id="t1"))
        assert events == []

    def test_run_completed(self):
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("RunStarted", run_id="r1", thread_id="t1"))
        events = adapter.to_digitalkin_events(_make_event("RunCompleted", run_id="r1", content="done"))
        assert AgentRunEvent.RUN_COMPLETED in _event_types(events)

    def test_run_completed_duplicate_skipped(self):
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("RunStarted", run_id="r1", thread_id="t1"))
        adapter.to_digitalkin_events(_make_event("RunCompleted", run_id="r1", content="done"))
        events = adapter.to_digitalkin_events(_make_event("RunCompleted", run_id="r1", content="done"))
        assert events == []

    def test_run_error(self):
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(
            _make_event("RunError", error_type="CRASH", content="something broke")
        )
        assert _event_types(events) == [AgentRunEvent.RUN_ERROR]
        assert events[0].content == "something broke"

    def test_unknown_event_ignored(self):
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(_make_event("SomeNewEvent"))
        assert events == []


# ═══════════════════════════════════════════════════════════════════════════
# 2. NATIVE REASONING (reasoning_started / content_delta / completed)
# ═══════════════════════════════════════════════════════════════════════════


class TestNativeReasoning:
    def test_full_reasoning_lifecycle(self):
        adapter = _create_adapter()
        all_events = []
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStarted")))
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("ReasoningContentDelta", reasoning_content="thinking..."))
        )
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningCompleted")))

        types = _event_types(all_events)
        assert types == [
            AgentRunEvent.REASONING_STARTED,
            AgentRunEvent.REASONING_CONTENT_DELTA,
            AgentRunEvent.REASONING_COMPLETED,
        ]

    def test_reasoning_started_closes_active_content(self):
        """If text is streaming and reasoning starts, text must close first."""
        adapter = _create_adapter()
        all_events = []
        # Start text
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("RunContent", content="hello", reasoning_content=None))
        )
        # Now reasoning starts → text should close
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStarted")))

        types = _event_types(all_events)
        assert AgentRunEvent.TEXT_MESSAGE_STARTED in types
        assert AgentRunEvent.TEXT_MESSAGE_COMPLETED in types
        # TEXT_MESSAGE_COMPLETED must come BEFORE REASONING_STARTED
        assert types.index(AgentRunEvent.TEXT_MESSAGE_COMPLETED) < types.index(AgentRunEvent.REASONING_STARTED)

    def test_reasoning_completed_when_not_active_returns_empty(self):
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(_make_event("ReasoningCompleted"))
        assert events == []


# ═══════════════════════════════════════════════════════════════════════════
# 3. REASONING VIA ReasoningTools (reasoning_step auto-wrap)
# ═══════════════════════════════════════════════════════════════════════════


class TestReasoningStep:
    def test_reasoning_step_auto_opens_lifecycle(self):
        """A reasoning_step without prior reasoning_started must auto-wrap."""
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="step content"))

        types = _event_types(events)
        assert types == [AgentRunEvent.REASONING_STARTED, AgentRunEvent.REASONING_STEP]
        assert adapter._reasoning_active is True

    def test_reasoning_step_reuses_active_reasoning(self):
        """If reasoning is already active, step doesn't re-open."""
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("ReasoningStarted"))
        events = adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="more thinking"))

        types = _event_types(events)
        assert types == [AgentRunEvent.REASONING_STEP]
        # No extra REASONING_STARTED

    def test_reasoning_step_closes_active_content(self):
        """If text is active when reasoning_step arrives, text closes first."""
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("RunContent", content="hello", reasoning_content=None))
        assert adapter._content_active is True

        events = adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="step"))
        types = _event_types(events)
        assert types[0] == AgentRunEvent.TEXT_MESSAGE_COMPLETED
        assert AgentRunEvent.REASONING_STARTED in types
        assert AgentRunEvent.REASONING_STEP in types

    def test_reasoning_step_empty_content_ignored(self):
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content=""))
        assert events == []
        assert adapter._reasoning_active is False

    def test_reasoning_step_then_text_auto_closes_reasoning(self):
        """After auto-opened reasoning_step, text content auto-closes reasoning."""
        adapter = _create_adapter()
        all_events = []
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="step")))
        assert adapter._reasoning_active is True

        all_events.extend(
            adapter.to_digitalkin_events(_make_event("RunContent", content="answer", reasoning_content=None))
        )

        types = _event_types(all_events)
        # reasoning_step auto-opens, then text closes reasoning and opens text
        assert AgentRunEvent.REASONING_STARTED in types
        assert AgentRunEvent.REASONING_STEP in types
        assert AgentRunEvent.REASONING_COMPLETED in types
        assert AgentRunEvent.TEXT_MESSAGE_STARTED in types
        # Order: REASONING_COMPLETED before TEXT_MESSAGE_STARTED
        assert types.index(AgentRunEvent.REASONING_COMPLETED) < types.index(AgentRunEvent.TEXT_MESSAGE_STARTED)

    def test_multiple_reasoning_steps_single_lifecycle(self):
        """Multiple consecutive reasoning_steps share one lifecycle."""
        adapter = _create_adapter()
        all_events = []
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="step 1")))
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="step 2")))

        types = _event_types(all_events)
        # Only one REASONING_STARTED at the beginning
        assert types.count(AgentRunEvent.REASONING_STARTED) == 1
        assert types.count(AgentRunEvent.REASONING_STEP) == 2


# ═══════════════════════════════════════════════════════════════════════════
# 4. TEXT CONTENT (run_content)
# ═══════════════════════════════════════════════════════════════════════════


class TestTextContent:
    def test_text_content_auto_opens_message(self):
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(_make_event("RunContent", content="hello", reasoning_content=None))
        types = _event_types(events)
        assert types == [AgentRunEvent.TEXT_MESSAGE_STARTED, AgentRunEvent.RUN_CONTENT]

    def test_text_content_subsequent_chunks_no_reopen(self):
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("RunContent", content="hello", reasoning_content=None))
        events = adapter.to_digitalkin_events(_make_event("RunContent", content=" world", reasoning_content=None))
        types = _event_types(events)
        assert types == [AgentRunEvent.RUN_CONTENT]

    def test_empty_text_closes_message(self):
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("RunContent", content="hello", reasoning_content=None))
        events = adapter.to_digitalkin_events(_make_event("RunContent", content="", reasoning_content=None))
        types = _event_types(events)
        assert types == [AgentRunEvent.TEXT_MESSAGE_COMPLETED]

    def test_text_content_closes_active_reasoning(self):
        """Text after reasoning auto-closes reasoning."""
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("ReasoningStarted"))
        adapter.to_digitalkin_events(_make_event("ReasoningContentDelta", reasoning_content="think"))
        events = adapter.to_digitalkin_events(_make_event("RunContent", content="answer", reasoning_content=None))

        types = _event_types(events)
        assert types[0] == AgentRunEvent.REASONING_COMPLETED
        assert AgentRunEvent.TEXT_MESSAGE_STARTED in types


# ═══════════════════════════════════════════════════════════════════════════
# 5. TOOL CALLS
# ═══════════════════════════════════════════════════════════════════════════


def _make_tool(
    tool_call_id="tc1",
    tool_name="search",
    tool_args=None,
    result=None,
    external_execution_required=False,
):
    return SimpleNamespace(
        tool_call_id=tool_call_id,
        tool_name=tool_name,
        tool_args=tool_args or {"q": "test"},
        result=result,
        external_execution_required=external_execution_required,
    )


class TestToolCalls:
    def test_tool_call_started(self):
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(
            _make_event("ToolCallStarted", tool=_make_tool())
        )
        types = _event_types(events)
        assert types == [AgentRunEvent.TOOL_CALL_STARTED]
        assert events[0].tool.tool_name == "search"

    def test_tool_call_completed(self):
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(
            _make_event("ToolCallCompleted", tool=_make_tool(result="found"), content="found")
        )
        types = _event_types(events)
        assert types == [AgentRunEvent.TOOL_CALL_COMPLETED]
        assert events[0].tool.result == "found"

    def test_tool_call_started_closes_reasoning_and_content(self):
        """Tool call must close both reasoning and content if active."""
        adapter = _create_adapter()
        all_events = []
        # Open reasoning
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStarted")))
        # Tool call starts → reasoning closes
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("ToolCallStarted", tool=_make_tool()))
        )
        types = _event_types(all_events)
        assert AgentRunEvent.REASONING_COMPLETED in types
        assert types.index(AgentRunEvent.REASONING_COMPLETED) < types.index(AgentRunEvent.TOOL_CALL_STARTED)

    def test_tool_call_error(self):
        adapter = _create_adapter()
        events = adapter.to_digitalkin_events(
            _make_event("ToolCallError", tool=_make_tool(), content="timeout")
        )
        types = _event_types(events)
        assert types == [AgentRunEvent.TOOL_CALL_ERROR]

    def test_tool_call_error_dedup(self):
        """Duplicate tool_call_error for same ID is skipped."""
        adapter = _create_adapter()
        adapter.to_digitalkin_events(
            _make_event("ToolCallCompleted", tool=_make_tool(tool_call_id="tc1"), content="ok")
        )
        events = adapter.to_digitalkin_events(
            _make_event("ToolCallError", tool=_make_tool(tool_call_id="tc1"), content="err")
        )
        assert events == []


# ═══════════════════════════════════════════════════════════════════════════
# 6. HITL PAUSE (run_paused → synthesized tool calls)
# ═══════════════════════════════════════════════════════════════════════════


class TestRunPaused:
    def test_run_paused_synthesizes_tool_events(self):
        adapter = _create_adapter()
        tool1 = _make_tool(tool_call_id="ext1", tool_name="get_weather", tool_args={"city": "Lyon"}, external_execution_required=True)
        events = adapter.to_digitalkin_events(
            _make_event("RunPaused", tools=[tool1], requirements=[])
        )
        types = _event_types(events)
        assert types == [AgentRunEvent.TOOL_CALL_STARTED, AgentRunEvent.TOOL_CALL_COMPLETED]
        assert events[0].tool.tool_name == "get_weather"
        assert events[1].content is None  # no result (external tool)
        assert adapter.is_paused is True

    def test_run_paused_multiple_tools(self):
        adapter = _create_adapter()
        tool1 = _make_tool(tool_call_id="ext1", tool_name="get_weather", external_execution_required=True)
        tool2 = _make_tool(tool_call_id="ext2", tool_name="select_items", external_execution_required=True)
        events = adapter.to_digitalkin_events(
            _make_event("RunPaused", tools=[tool1, tool2], requirements=[])
        )
        types = _event_types(events)
        assert types == [
            AgentRunEvent.TOOL_CALL_STARTED,
            AgentRunEvent.TOOL_CALL_COMPLETED,
            AgentRunEvent.TOOL_CALL_STARTED,
            AgentRunEvent.TOOL_CALL_COMPLETED,
        ]
        assert adapter.paused_tool_executions[0].tool_name == "get_weather"
        assert adapter.paused_tool_executions[1].tool_name == "select_items"

    def test_run_paused_closes_active_reasoning_and_content(self):
        adapter = _create_adapter()
        all_events = []
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("RunContent", content="checking", reasoning_content=None))
        )
        assert adapter._content_active is True

        tool1 = _make_tool(tool_call_id="ext1", tool_name="get_weather", external_execution_required=True)
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("RunPaused", tools=[tool1], requirements=[]))
        )
        types = _event_types(all_events)
        # Content must close before synthesized tool events
        assert AgentRunEvent.TEXT_MESSAGE_COMPLETED in types
        idx_close = types.index(AgentRunEvent.TEXT_MESSAGE_COMPLETED)
        idx_tool = types.index(AgentRunEvent.TOOL_CALL_STARTED)
        assert idx_close < idx_tool

    def test_run_paused_properties(self):
        adapter = _create_adapter()
        assert adapter.is_paused is False
        assert adapter.paused_tool_executions == []
        assert adapter.paused_requirements == []

        tool = _make_tool(tool_call_id="ext1", tool_name="get_weather", external_execution_required=True)
        req = SimpleNamespace(tool_execution=tool)
        adapter.to_digitalkin_events(_make_event("RunPaused", tools=[tool], requirements=[req]))

        assert adapter.is_paused is True
        assert len(adapter.paused_tool_executions) == 1
        assert len(adapter.paused_requirements) == 1


# ═══════════════════════════════════════════════════════════════════════════
# 7. STATE TRANSITIONS & OVERLAPS
# ═══════════════════════════════════════════════════════════════════════════


class TestStateTransitions:
    def test_reasoning_to_content_to_tool_to_content(self):
        """Complex sequence: reasoning → text → tool → text → completed."""
        adapter = _create_adapter()
        all_events = []

        # 1. Reasoning
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStarted")))
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("ReasoningContentDelta", reasoning_content="think"))
        )
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningCompleted")))

        # 2. Text
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("RunContent", content="I'll search", reasoning_content=None))
        )

        # 3. Tool call
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("ToolCallStarted", tool=_make_tool()))
        )
        all_events.extend(
            adapter.to_digitalkin_events(
                _make_event("ToolCallCompleted", tool=_make_tool(result="found"), content="found")
            )
        )

        # 4. More text
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("RunContent", content="Here's what I found", reasoning_content=None))
        )

        # 5. Completed
        all_events.extend(adapter.to_digitalkin_events(_make_event("RunCompleted", run_id="r1", content="done")))

        types = _event_types(all_events)

        # Verify order of lifecycle boundaries
        assert types.index(AgentRunEvent.REASONING_STARTED) < types.index(AgentRunEvent.REASONING_COMPLETED)
        # First text block: opened then closed by tool_call_started
        first_text_start = types.index(AgentRunEvent.TEXT_MESSAGE_STARTED)
        first_text_end = types.index(AgentRunEvent.TEXT_MESSAGE_COMPLETED)
        assert first_text_start < first_text_end
        assert first_text_end < types.index(AgentRunEvent.TOOL_CALL_STARTED)
        # Second text block: opened, closed by run_completed
        second_text_start = types.index(AgentRunEvent.TEXT_MESSAGE_STARTED, first_text_start + 1)
        second_text_end = types.index(AgentRunEvent.TEXT_MESSAGE_COMPLETED, first_text_end + 1)
        assert second_text_start < second_text_end
        assert second_text_end < types.index(AgentRunEvent.RUN_COMPLETED)

    def test_reasoning_step_to_tool_to_text(self):
        """ReasoningTools step → tool call → text answer."""
        adapter = _create_adapter()
        all_events = []

        # reasoning_step (auto-opens)
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="analyzing")))
        assert adapter._reasoning_active is True

        # tool_call_started → auto-closes reasoning
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("ToolCallStarted", tool=_make_tool()))
        )
        assert adapter._reasoning_active is False

        # tool completed
        all_events.extend(
            adapter.to_digitalkin_events(
                _make_event("ToolCallCompleted", tool=_make_tool(result="ok"), content="ok")
            )
        )

        # text answer
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("RunContent", content="result", reasoning_content=None))
        )

        types = _event_types(all_events)
        # reasoning auto-opened, then auto-closed by tool
        assert types[0] == AgentRunEvent.REASONING_STARTED
        assert types[1] == AgentRunEvent.REASONING_STEP
        assert types[2] == AgentRunEvent.REASONING_COMPLETED
        assert types[3] == AgentRunEvent.TOOL_CALL_STARTED

    def test_content_to_reasoning_via_run_content(self):
        """run_content with reasoning_content after text → close text, open reasoning."""
        adapter = _create_adapter()
        all_events = []

        # Text first
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("RunContent", content="hello", reasoning_content=None))
        )
        assert adapter._content_active is True

        # Now reasoning via run_content
        all_events.extend(
            adapter.to_digitalkin_events(_make_event("RunContent", content=None, reasoning_content="deep thought"))
        )

        types = _event_types(all_events)
        assert AgentRunEvent.TEXT_MESSAGE_COMPLETED in types
        assert AgentRunEvent.REASONING_STARTED in types
        assert types.index(AgentRunEvent.TEXT_MESSAGE_COMPLETED) < types.index(AgentRunEvent.REASONING_STARTED)

    def test_reasoning_to_reasoning_step_shares_lifecycle(self):
        """If native reasoning is active, reasoning_step reuses the open lifecycle."""
        adapter = _create_adapter()
        all_events = []

        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStarted")))
        reasoning_id = adapter._current_reasoning_id
        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="more")))

        types = _event_types(all_events)
        # No second REASONING_STARTED
        assert types.count(AgentRunEvent.REASONING_STARTED) == 1
        # Step uses the same reasoning_id
        step_event = [e for e in all_events if hasattr(e, "delta") and getattr(e, "delta", None) == "more"][0]
        assert step_event.reasoning_id == reasoning_id

    def test_run_paused_after_reasoning_step(self):
        """reasoning_step then run_paused: reasoning closes before synthesized tools."""
        adapter = _create_adapter()
        all_events = []

        all_events.extend(adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="plan")))
        assert adapter._reasoning_active is True

        tool = _make_tool(tool_call_id="ext1", tool_name="get_weather", external_execution_required=True)
        all_events.extend(adapter.to_digitalkin_events(_make_event("RunPaused", tools=[tool], requirements=[])))

        types = _event_types(all_events)
        assert AgentRunEvent.REASONING_COMPLETED in types
        assert types.index(AgentRunEvent.REASONING_COMPLETED) < types.index(AgentRunEvent.TOOL_CALL_STARTED)
        assert adapter.is_paused is True
        assert adapter._reasoning_active is False


# ═══════════════════════════════════════════════════════════════════════════
# 8. FLUSH
# ═══════════════════════════════════════════════════════════════════════════


class TestFlush:
    def test_flush_closes_active_content(self):
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("RunContent", content="hello", reasoning_content=None))
        events = adapter.flush()
        types = _event_types(events)
        assert types == [AgentRunEvent.TEXT_MESSAGE_COMPLETED]
        assert adapter._content_active is False

    def test_flush_closes_active_reasoning(self):
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("ReasoningStarted"))
        events = adapter.flush()
        types = _event_types(events)
        assert types == [AgentRunEvent.REASONING_COMPLETED]
        assert adapter._reasoning_active is False

    def test_flush_closes_reasoning_step_auto_opened(self):
        """Flush after auto-opened reasoning_step closes the reasoning."""
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("ReasoningStep", reasoning_content="step"))
        assert adapter._reasoning_active is True
        events = adapter.flush()
        types = _event_types(events)
        assert types == [AgentRunEvent.REASONING_COMPLETED]

    def test_flush_closes_both_content_and_reasoning(self):
        """Edge case: if somehow both are active, flush closes both."""
        adapter = _create_adapter()
        # Force both active (shouldn't happen normally, but test robustness)
        adapter._content_active = True
        adapter._current_message_id = "m1"
        adapter._reasoning_active = True
        adapter._current_reasoning_id = "r1"

        events = adapter.flush()
        types = _event_types(events)
        assert AgentRunEvent.TEXT_MESSAGE_COMPLETED in types
        assert AgentRunEvent.REASONING_COMPLETED in types

    def test_flush_empty_when_nothing_active(self):
        adapter = _create_adapter()
        events = adapter.flush()
        assert events == []

    def test_double_flush_idempotent(self):
        adapter = _create_adapter()
        adapter.to_digitalkin_events(_make_event("RunContent", content="hello", reasoning_content=None))
        adapter.flush()
        events = adapter.flush()
        assert events == []


# ═══════════════════════════════════════════════════════════════════════════
# 9. FULL REALISTIC SEQUENCES
# ═══════════════════════════════════════════════════════════════════════════


class TestRealisticSequences:
    def test_ada_sequence_think_search_analyze_text_pause(self):
        """Realistic Ada sequence: think → search → analyze → text → pause on frontend tool."""
        adapter = _create_adapter()
        all_events = []

        # 1. think tool call
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("ToolCallStarted", tool=_make_tool("tc-think", "think"))
        ))
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("ToolCallCompleted", tool=_make_tool("tc-think", "think", result="planned"), content="planned")
        ))

        # 2. reasoning_step after think (auto-wraps)
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("ReasoningStep", reasoning_content="## Plan\nI'll search first")
        ))

        # 3. text message
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("RunContent", content="Searching...", reasoning_content=None)
        ))

        # 4. search tool
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("ToolCallStarted", tool=_make_tool("tc-search", "web_search"))
        ))
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("ToolCallCompleted", tool=_make_tool("tc-search", "web_search", result="results"), content="results")
        ))

        # 5. analyze tool
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("ToolCallStarted", tool=_make_tool("tc-analyze", "analyze"))
        ))
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("ToolCallCompleted", tool=_make_tool("tc-analyze", "analyze", result="analysis"), content="analysis")
        ))

        # 6. reasoning_step after analyze (auto-wraps again — new lifecycle)
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("ReasoningStep", reasoning_content="## Analysis\nResults look good")
        ))

        # 7. text answer
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("RunContent", content="Here are the results!", reasoning_content=None)
        ))

        # 8. frontend tool pause — RunPausedEvent.tools contains ALL tools
        # (backend ones already executed + external ones). The adapter must
        # only synthesize events for external ones.
        backend_tool = _make_tool("tc-think", "think")  # already executed, NOT external
        ext_tool = _make_tool("ext-show", "show_sources", external_execution_required=True)
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("RunPaused", tools=[backend_tool, ext_tool], requirements=[])
        ))

        types = _event_types(all_events)

        # Verify no orphan reasoning events
        reasoning_starts = [i for i, t in enumerate(types) if t == AgentRunEvent.REASONING_STARTED]
        reasoning_ends = [i for i, t in enumerate(types) if t == AgentRunEvent.REASONING_COMPLETED]
        assert len(reasoning_starts) == len(reasoning_ends), (
            f"Mismatched reasoning lifecycle: {len(reasoning_starts)} starts vs {len(reasoning_ends)} ends"
        )
        for start, end in zip(reasoning_starts, reasoning_ends):
            assert start < end, "REASONING_COMPLETED before REASONING_STARTED"

        # Verify no orphan text events
        text_starts = [i for i, t in enumerate(types) if t == AgentRunEvent.TEXT_MESSAGE_STARTED]
        text_ends = [i for i, t in enumerate(types) if t == AgentRunEvent.TEXT_MESSAGE_COMPLETED]
        assert len(text_starts) == len(text_ends), (
            f"Mismatched text lifecycle: {len(text_starts)} starts vs {len(text_ends)} ends"
        )

        # Verify pause at the end
        assert adapter.is_paused is True
        ext_tools = [t for t in adapter.paused_tool_executions if getattr(t, "external_execution_required", False)]
        assert len(ext_tools) == 1
        assert ext_tools[0].tool_name == "show_sources"

    def test_template_archetype_simple_pause_resume_style(self):
        """Simple template-archetype sequence: reasoning → text → external tool → pause."""
        adapter = _create_adapter()
        all_events = []

        # Native reasoning
        all_events.extend(adapter.to_digitalkin_events(_make_event("RunStarted", run_id="r1", thread_id="t1")))
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("RunContent", content=None, reasoning_content="Let me check the weather")
        ))
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("RunContent", content=None, reasoning_content="")  # close reasoning
        ))

        # Text
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("RunContent", content="Sure! Let me check.", reasoning_content=None)
        ))

        # External tool pause
        ext_tool = _make_tool("ext1", "get_weather", {"city": "Lyon"}, external_execution_required=True)
        all_events.extend(adapter.to_digitalkin_events(
            _make_event("RunPaused", tools=[ext_tool], requirements=[])
        ))

        # Flush
        all_events.extend(adapter.flush())

        types = _event_types(all_events)

        # All lifecycles properly closed
        assert types.count(AgentRunEvent.REASONING_STARTED) == types.count(AgentRunEvent.REASONING_COMPLETED)
        assert types.count(AgentRunEvent.TEXT_MESSAGE_STARTED) == types.count(AgentRunEvent.TEXT_MESSAGE_COMPLETED)
        assert adapter.is_paused is True

    def test_run_paused_mixed_backend_and_frontend_tools(self):
        """RunPausedEvent.tools has backend tools (think) + 2 frontend tools.

        The adapter must only synthesize events for the 2 external tools,
        skip the backend tool (already streamed), and deduplicate if the
        same tool_call_id appears multiple times (Agno accumulation bug).
        """
        adapter = _create_adapter()

        backend = _make_tool("tc-think", "think")  # NOT external
        ext1 = _make_tool("ext-ask", "ask_question", external_execution_required=True)
        ext2 = _make_tool("ext-map", "show_map", external_execution_required=True)
        # Simulate Agno's accumulation: ext1 appears twice (from two yield batches)
        ext1_dup = _make_tool("ext-ask", "ask_question", external_execution_required=True)

        events = adapter.to_digitalkin_events(
            _make_event("RunPaused", tools=[backend, ext1, ext1_dup, ext2], requirements=[])
        )
        types = _event_types(events)

        # Only 2 unique external tools → 2 pairs of start/completed
        assert types == [
            AgentRunEvent.TOOL_CALL_STARTED,
            AgentRunEvent.TOOL_CALL_COMPLETED,
            AgentRunEvent.TOOL_CALL_STARTED,
            AgentRunEvent.TOOL_CALL_COMPLETED,
        ]
        # First pair is ask_question, second is show_map
        assert events[0].tool.tool_name == "ask_question"
        assert events[2].tool.tool_name == "show_map"
        # Backend tool NOT synthesized
        tool_names = [e.tool.tool_name for e in events if hasattr(e, "tool") and e.tool]
        assert "think" not in tool_names

    def test_run_paused_no_external_tools_emits_nothing(self):
        """If RunPausedEvent.tools only has backend tools, no events synthesized."""
        adapter = _create_adapter()
        backend = _make_tool("tc-think", "think")  # NOT external
        events = adapter.to_digitalkin_events(
            _make_event("RunPaused", tools=[backend], requirements=[])
        )
        # No tool events (think is not external)
        tool_events = [e for e in events if AgentRunEvent.TOOL_CALL_STARTED == e.event or AgentRunEvent.TOOL_CALL_COMPLETED == e.event]
        assert tool_events == []
        # But adapter is still paused
        assert adapter.is_paused is True
