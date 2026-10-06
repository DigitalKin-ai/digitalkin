"""Tests for the SDK-owned AG-UI pipeline state (``AgUiRunState``) and its mixin wiring.

The front only renders this state: ``STATE_SNAPSHOT`` right after ``RUN_STARTED``, then one
``STATE_DELTA`` (JSON Patch) per tool / subagent / run transition.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.mixins.agui_mixin import AgUiMixin, AgUiRunState
from digitalkin.models.events import (
    AgentRunEvent,
    RunCompletedEvent,
    RunErrorEvent,
    RunStartedEvent,
    SubagentErrorEvent,
    SubagentStartedEvent,
    ToolCallCompletedEvent,
    ToolCallErrorEvent,
    ToolCallStartedEvent,
    ToolInfo,
)
from digitalkin.models.module.ag_ui import (
    AgUiRunErrorOutput,
    AgUiRunFinishedOutput,
    AgUiRunStartedOutput,
    AgUiStateDeltaOutput,
    AgUiStateSnapshotOutput,
    AgUiToolCallEndOutput,
    AgUiToolCallResultOutput,
)

pytestmark = pytest.mark.unit


def _ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.callbacks.send_message = AsyncMock()
    ctx.session.current_ids = MagicMock(return_value={})
    ctx.agui_run = None
    return ctx


def _roots(ctx: MagicMock) -> list[Any]:
    return [call.args[0].root for call in ctx.callbacks.send_message.await_args_list]


def _deltas(ctx: MagicMock) -> list[list[dict[str, Any]]]:
    return [root.event.delta for root in _roots(ctx) if isinstance(root, AgUiStateDeltaOutput)]


def _tool(tool_call_id: str = "call_1", name: str = "search") -> ToolInfo:
    return ToolInfo(tool_call_id=tool_call_id, tool_name=name, tool_args={"q": "x"}, result=None)


async def _started(mixin: AgUiMixin, ctx: MagicMock) -> None:
    await mixin._handle_run_started(
        ctx,
        RunStartedEvent(event=AgentRunEvent.RUN_STARTED, run_id="r1", thread_id="t1", timestamp=None, metadata=None),
    )


class TestRunStateUpdate:
    async def test_update_sets_value_and_emits_add_then_replace(self) -> None:
        ctx = _ctx()
        run = AgUiRunState()

        await run.update(ctx, (("tools", "a/b~c"), {"status": "running"}))
        await run.update(ctx, (("tools", "a/b~c", "status"), "failed"))

        assert run.state["tools"]["a/b~c"] == {"status": "failed"}
        assert _deltas(ctx) == [
            [{"op": "add", "path": "/tools/a~1b~0c", "value": {"status": "running"}}],
            [{"op": "replace", "path": "/tools/a~1b~0c/status", "value": "failed"}],
        ]

    async def test_fail_closes_once(self) -> None:
        ctx = _ctx()
        run = AgUiRunState()

        await run.fail(ctx, "cancelled", "user_stop", "cancelled")
        await run.fail(ctx, "failed", "late", "module_error")

        errors = [root.event for root in _roots(ctx) if isinstance(root, AgUiRunErrorOutput)]
        assert [(e.message, e.code) for e in errors] == [("user_stop", "cancelled")]
        assert run.state["run"]["status"] == "cancelled"
        assert run.open is False


class TestMixinPipelineState:
    async def test_snapshot_follows_run_started(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()

        await _started(mixin, ctx)

        roots = _roots(ctx)
        assert [type(r) for r in roots] == [AgUiRunStartedOutput, AgUiStateSnapshotOutput]
        assert roots[1].event.snapshot["run"]["status"] == "running"
        assert ctx.agui_run is not None

    async def test_tool_lifecycle_running_then_completed(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()
        await _started(mixin, ctx)

        await mixin._handle_tool_call_started(
            ctx,
            ToolCallStartedEvent(event=AgentRunEvent.TOOL_CALL_STARTED, tool=_tool(), timestamp=None, metadata=None),
        )
        await mixin._handle_tool_call_completed(
            ctx,
            ToolCallCompletedEvent(
                event=AgentRunEvent.TOOL_CALL_COMPLETED, tool=_tool(), content="ok", timestamp=None, metadata=None
            ),
        )

        added, completed = _deltas(ctx)
        assert added[0]["op"] == "add"
        assert added[0]["path"] == "/tools/call_1"
        assert added[0]["value"]["status"] == "running"
        assert added[0]["value"]["name"] == "search"
        assert completed == [{"op": "replace", "path": "/tools/call_1/status", "value": "completed"}]

    async def test_tool_error_emits_error_result_and_failed_status(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()
        await _started(mixin, ctx)
        await mixin._handle_tool_call_started(
            ctx,
            ToolCallStartedEvent(event=AgentRunEvent.TOOL_CALL_STARTED, tool=_tool(), timestamp=None, metadata=None),
        )

        await mixin._handle_tool_call_error(
            ctx,
            ToolCallErrorEvent(
                event=AgentRunEvent.TOOL_CALL_ERROR,
                tool=_tool(),
                error_message="[X] boom",
                timestamp=None,
                metadata=None,
            ),
        )

        roots = _roots(ctx)
        assert isinstance(roots[-3], AgUiToolCallEndOutput)
        assert isinstance(roots[-2], AgUiToolCallResultOutput)
        assert json.loads(roots[-2].event.content) == {"error": "[X] boom"}
        assert _deltas(ctx)[-1] == [
            {"op": "replace", "path": "/tools/call_1/status", "value": "failed"},
            {"op": "replace", "path": "/tools/call_1/error", "value": "[X] boom"},
        ]

    async def test_subagent_error_marks_it_failed_without_closing_run(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()
        await _started(mixin, ctx)

        await mixin._handle_subagent_started(
            ctx,
            SubagentStartedEvent(
                event=AgentRunEvent.SUBAGENT_STARTED, name="writer", subagent_run_id="s1", timestamp=None, metadata=None
            ),
        )
        await mixin._handle_subagent_error(
            ctx,
            SubagentErrorEvent(
                event=AgentRunEvent.SUBAGENT_ERROR, message="oops", subagent_run_id="s1", timestamp=None, metadata=None
            ),
        )

        assert ctx.agui_run.state["subagents"]["s1"]["status"] == "failed"
        assert ctx.agui_run.state["subagents"]["s1"]["error"] == "oops"
        assert ctx.agui_run.open is True

    async def test_run_error_marks_failed_then_run_error(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()
        await _started(mixin, ctx)

        await mixin._handle_run_error(
            ctx,
            RunErrorEvent(
                event=AgentRunEvent.RUN_ERROR,
                error_type="ModelError",
                content="rate limited",
                error_details=None,
                timestamp=None,
                metadata=None,
            ),
        )

        roots = _roots(ctx)
        assert isinstance(roots[-2], AgUiStateDeltaOutput)
        assert isinstance(roots[-1], AgUiRunErrorOutput)
        assert roots[-1].event.code == "ModelError"
        assert ctx.agui_run.state["run"] == {"status": "failed", "error": "rate limited", "heartbeatAt": None}

    async def test_run_completed_marks_completed(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()
        await _started(mixin, ctx)

        await mixin._handle_run_completed(
            ctx,
            RunCompletedEvent(
                event=AgentRunEvent.RUN_COMPLETED,
                run_id="r1",
                final_content=None,
                usage=None,
                message_id=None,
                timestamp=None,
                metadata=None,
            ),
        )

        roots = _roots(ctx)
        assert _deltas(ctx)[-1] == [{"op": "replace", "path": "/run/status", "value": "completed"}]
        assert isinstance(roots[-1], AgUiRunFinishedOutput)
        assert ctx.agui_run.open is False
