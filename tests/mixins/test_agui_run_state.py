"""Tests for the SDK-owned AG-UI pipeline state (``AgUiRunState``) and its mixin wiring.

The front only renders this state: ``STATE_SNAPSHOT`` right after ``RUN_STARTED``, then one
``STATE_DELTA`` (JSON Patch) per tool / subagent / run transition.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from ag_ui.core.events import TokenUsage

from digitalkin.mixins.agui_mixin import AgUiMixin, AgUiRunState
from digitalkin.models.events import (
    AgentRunEvent,
    ReasoningStartedEvent,
    RunCompletedEvent,
    RunErrorCode,
    RunErrorEvent,
    RunStartedEvent,
    SubagentErrorEvent,
    SubagentStartedEvent,
    TextMessageCompletedEvent,
    TextMessageStartedEvent,
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
    AgUiSubagentErrorOutput,
    AgUiToolCallEndOutput,
    AgUiToolCallResultOutput,
)

pytestmark = pytest.mark.unit


def _ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.callbacks.send_message = AsyncMock()
    ctx.session.current_ids = MagicMock(return_value={})
    ctx.storage.read = AsyncMock(return_value=None)
    ctx.agui_run = None
    return ctx


def _roots(ctx: MagicMock) -> list[Any]:
    return [call.args[0].root for call in ctx.callbacks.send_message.await_args_list]


def _deltas(ctx: MagicMock) -> list[list[dict[str, Any]]]:
    """Patch ops exactly as they reach the wire (``ModuleRunner._on_output`` dump)."""
    return [
        call.args[0].model_dump(mode="json")["root"]["event"]["delta"]
        for call in ctx.callbacks.send_message.await_args_list
        if isinstance(call.args[0].root, AgUiStateDeltaOutput)
    ]


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

    async def test_null_value_survives_serialisation(self) -> None:
        """AG-UI 1.0 types patch ops; ``exclude_none`` must not strip a required ``value: null``."""
        ctx = _ctx()
        run = AgUiRunState()

        await run.update(ctx, (("run", "error"), None))

        assert _deltas(ctx) == [[{"op": "replace", "path": "/run/error", "value": None}]]

    @pytest.mark.parametrize(
        ("code", "expected"), [(RunErrorCode.MODEL_RATE_LIMITED, True), (RunErrorCode.MODEL_CONTENT_FILTERED, False)]
    )
    async def test_fail_sends_retryable_hint_in_metadata(self, code: RunErrorCode, expected: bool) -> None:
        ctx = _ctx()

        await AgUiRunState().fail(ctx, "x", code, metadata={"digitalkin": {"k": 1}})

        error = next(root.event for root in _roots(ctx) if isinstance(root, AgUiRunErrorOutput))
        dumped = error.model_dump(mode="json", by_alias=True)
        assert (dumped["code"], dumped["metadata"]) == (code.value, {"digitalkin": {"k": 1}, "retryable": expected})
        assert "retryable" not in dumped

    async def test_run_error_matches_the_closed_ag_ui_schema(self) -> None:
        """AG-UI 1.0 ``RunErrorEvent`` is closed: no field outside its schema reaches the wire."""
        ctx = _ctx()

        await AgUiRunState().fail(ctx, "x", RunErrorCode.MODEL_TIMEOUT)

        error = next(root.event for root in _roots(ctx) if isinstance(root, AgUiRunErrorOutput))
        assert set(error.model_dump(mode="json", by_alias=True)) <= {
            "type",
            "timestamp",
            "rawEvent",
            "metadata",
            "message",
            "code",
            "usage",
        }

    async def test_fail_omits_retryable_when_unknown(self) -> None:
        ctx = _ctx()

        await AgUiRunState().fail(ctx, "x", RunErrorCode.MODULE_FAILED)

        error = next(root.event for root in _roots(ctx) if isinstance(root, AgUiRunErrorOutput))
        assert error.metadata is None

    async def test_fail_closes_once(self) -> None:
        ctx = _ctx()
        run = AgUiRunState()

        await run.fail(ctx, "first", RunErrorCode.MODEL_TIMEOUT)
        await run.fail(ctx, "late", RunErrorCode.MODULE_FAILED)
        await run.cancel(ctx, "signal_service_cancel")

        errors = [root.event for root in _roots(ctx) if isinstance(root, AgUiRunErrorOutput)]
        assert [(e.message, e.code) for e in errors] == [("first", "MODEL_TIMEOUT")]
        assert not any(isinstance(root, AgUiRunFinishedOutput) for root in _roots(ctx))
        assert run.state["run"]["status"] == "failed"
        assert run.open is False

    async def test_fail_after_close_logs_the_code_value(self, caplog: pytest.LogCaptureFixture) -> None:
        ctx = _ctx()
        run = AgUiRunState()
        await run.cancel(ctx, "signal_service_cancel")

        with caplog.at_level(logging.INFO):
            await run.fail(ctx, "late", RunErrorCode.MODULE_FAILED)

        assert "RUN_ERROR code=MODULE_FAILED message=late" in caplog.text


class TestRunCancel:
    async def test_cancel_finishes_the_run_with_the_cancelled_outcome(self) -> None:
        ctx = _ctx()
        run = AgUiRunState("t1", "r1")

        await run.cancel(ctx, "signal_service_stop")
        await run.cancel(ctx, "signal_service_stop")

        roots = _roots(ctx)
        assert [type(r) for r in roots] == [AgUiStateDeltaOutput, AgUiRunFinishedOutput]
        finished = roots[-1].event.model_dump(mode="json", by_alias=True)
        assert (finished["threadId"], finished["runId"], finished["outcome"]) == ("t1", "r1", {"type": "cancelled"})
        assert not any(isinstance(r, AgUiRunErrorOutput) for r in roots)
        assert run.state["run"]["status"] == "cancelled"
        assert run.open is False

    async def test_cancel_closes_what_the_run_left_open(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()
        await _started(mixin, ctx)
        await mixin._handle_subagent_started(
            ctx,
            SubagentStartedEvent(
                event=AgentRunEvent.SUBAGENT_STARTED, name="writer", subagent_run_id="s1", timestamp=None, metadata=None
            ),
        )
        await mixin._handle_text_message_started(
            ctx,
            TextMessageStartedEvent(
                event=AgentRunEvent.TEXT_MESSAGE_STARTED, message_id="m1", subagent_run_id="s1", timestamp=None
            ),
        )
        await mixin._handle_reasoning_started(
            ctx, ReasoningStartedEvent(event=AgentRunEvent.REASONING_STARTED, reasoning_id="rs1", timestamp=None)
        )
        await mixin._handle_text_message_started(
            ctx, TextMessageStartedEvent(event=AgentRunEvent.TEXT_MESSAGE_STARTED, message_id="m2", timestamp=None)
        )
        await mixin._handle_text_message_completed(
            ctx, TextMessageCompletedEvent(event=AgentRunEvent.TEXT_MESSAGE_COMPLETED, message_id="m2", timestamp=None)
        )
        await mixin._handle_tool_call_started(
            ctx,
            ToolCallStartedEvent(
                event=AgentRunEvent.TOOL_CALL_STARTED, tool=_tool(), subagent_run_id="s1", timestamp=None, metadata=None
            ),
        )
        ctx.callbacks.send_message.reset_mock()

        await ctx.agui_run.cancel(ctx, "signal_service_cancel")

        closing = [
            (type(r).__name__, r.event.model_dump(mode="json", by_alias=True).get("subagentRunId"))
            for r in _roots(ctx)
            if not isinstance(r, AgUiStateDeltaOutput)
        ]
        assert closing == [
            ("AgUiTextMessageEndOutput", "s1"),
            ("AgUiReasoningMessageEndOutput", None),
            ("AgUiReasoningEndOutput", None),
            ("AgUiToolCallEndOutput", "s1"),
            ("AgUiToolCallResultOutput", "s1"),
            ("AgUiSubagentErrorOutput", "s1"),
            ("AgUiRunFinishedOutput", None),
        ]
        subagent_error = next(r.event for r in _roots(ctx) if isinstance(r, AgUiSubagentErrorOutput))
        assert subagent_error.code == RunErrorCode.RUN_CANCELLED
        assert _deltas(ctx)[-1] == [
            {"op": "replace", "path": "/tools/call_1/status", "value": "failed"},
            {"op": "replace", "path": "/tools/call_1/error", "value": "run cancelled"},
            {"op": "replace", "path": "/subagents/s1/status", "value": "failed"},
            {"op": "replace", "path": "/subagents/s1/error", "value": "run cancelled"},
            {"op": "replace", "path": "/run/status", "value": "cancelled"},
        ]

    async def test_agno_run_cancelled_finishes_as_cancelled(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()
        await _started(mixin, ctx)

        await mixin._handle_run_error(
            ctx,
            RunErrorEvent(
                event=AgentRunEvent.RUN_ERROR,
                error_type=RunErrorCode.RUN_CANCELLED,
                content="cancelled",
                error_details=None,
                timestamp=None,
                metadata=None,
            ),
        )

        roots = _roots(ctx)
        assert isinstance(roots[-1], AgUiRunFinishedOutput)
        assert roots[-1].event.outcome.type == "cancelled"
        assert not any(isinstance(r, AgUiRunErrorOutput) for r in roots)

    async def test_agno_run_cancelled_without_run_state_finishes_as_cancelled(self) -> None:
        """Without a handled RUN_STARTED there is no run state; the cancel still ends on RUN_FINISHED."""
        ctx, mixin = _ctx(), AgUiMixin()
        mixin._thread_id, mixin._run_id = "t1", "r1"

        await mixin._handle_run_error(
            ctx,
            RunErrorEvent(
                event=AgentRunEvent.RUN_ERROR,
                error_type=RunErrorCode.RUN_CANCELLED,
                content="cancelled",
                error_details=None,
                timestamp=None,
                metadata=None,
            ),
        )

        roots = _roots(ctx)
        assert [type(r) for r in roots] == [AgUiRunFinishedOutput]
        finished = roots[0].event.model_dump(mode="json", by_alias=True)
        assert (finished["threadId"], finished["runId"], finished["outcome"]) == ("t1", "r1", {"type": "cancelled"})


class TestRunUsage:
    @staticmethod
    def _usage(model: str, input_tokens: int, output_tokens: int) -> TokenUsage:
        return TokenUsage(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=input_tokens + output_tokens,
        )

    async def test_usage_of_the_task_reaches_run_finished(self) -> None:
        ctx = _ctx()
        ctx.session.job_id = "task-u1"
        mixin = AgUiMixin()
        await _started(mixin, ctx)

        now = time.time()
        AgUiRunState.record_usage("task-u1", self._usage("claude", 100, 10), now)
        AgUiRunState.record_usage("task-u1", self._usage("claude", 50, 5), now)
        AgUiRunState.record_usage("task-u1", self._usage("gemini", 7, 1), now)
        AgUiRunState.record_usage("task-other", self._usage("claude", 999, 999), now)
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
        AgUiRunState.record_usage("task-u1", self._usage("claude", 1, 1), time.time())

        finished = _roots(ctx)[-1].event.model_dump(mode="json", by_alias=True)
        assert finished["usage"] == [
            {"model": "claude", "inputTokens": 150, "outputTokens": 15, "totalTokens": 165},
            {"model": "gemini", "inputTokens": 7, "outputTokens": 1, "totalTokens": 8},
        ]
        assert "task-u1" not in AgUiRunState._open

    @pytest.mark.parametrize("ending", ["fail", "cancel"])
    async def test_usage_reaches_failed_and_cancelled_runs(self, ending: str) -> None:
        ctx = _ctx()
        run = AgUiRunState("t1", "r1", "task-u2")
        AgUiRunState.record_usage("task-u2", self._usage("claude", 3, 2), time.time())

        if ending == "fail":
            await run.fail(ctx, "x", RunErrorCode.MODEL_TIMEOUT)
        else:
            await run.cancel(ctx, "signal_service_cancel")

        terminal = _roots(ctx)[-1].event
        assert [u.input_tokens for u in terminal.usage] == [3]
        assert "task-u2" not in AgUiRunState._open

    async def test_no_usage_is_absent(self) -> None:
        ctx = _ctx()

        await AgUiRunState("t1", "r1", "task-u3").cancel(ctx, "signal_service_cancel")

        assert "usage" not in _roots(ctx)[-1].event.model_dump(mode="json", by_alias=True)

    async def test_two_runs_of_one_mission_keep_their_own_usage(self) -> None:
        """A HITL resume and a new message can run at once on one mission: each task counts its own calls."""
        resume = AgUiRunState("missions:m", "r1", "task-resume")
        message = AgUiRunState("missions:m", "r2", "task-message")

        AgUiRunState.record_usage("task-resume", self._usage("claude", 1, 1), time.time())

        assert (len(resume.usage), len(message.usage)) == (1, 0)
        resume.close()
        message.close()

    def test_only_a_call_started_before_the_close_is_reported_late(self, caplog: pytest.LogCaptureFixture) -> None:
        run = AgUiRunState("t1", "r1", "task-late")
        started_before = time.time() - 1
        run.close()

        with caplog.at_level(logging.INFO):
            AgUiRunState.record_usage("task-late", self._usage("claude", 1, 1), started_before)
            AgUiRunState.record_usage("task-late", self._usage("summary", 2, 2), time.time() + 1)
            AgUiRunState.record_usage("task-without-run", self._usage("tool", 3, 3), started_before)

        late = [r.getMessage() for r in caplog.records if "AGUI-USAGE-LATE" in r.getMessage()]
        assert len(late) == 1
        assert "usage of claude (2 tokens)" in late[0]
        assert run.usage == []


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

    async def test_run_state_tool_entry_has_display_names(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()
        await _started(mixin, ctx)
        tool = ToolInfo(
            tool_call_id="c1",
            tool_name="services_manager",
            tool_display_name="Services",
            function_display_name="Create",
        )

        await mixin._handle_tool_call_started(
            ctx, ToolCallStartedEvent(event=AgentRunEvent.TOOL_CALL_STARTED, tool=tool, timestamp=None, metadata=None)
        )

        expected = {"name": "services_manager", "toolDisplayName": "Services", "functionDisplayName": "Create"}
        assert expected.items() <= ctx.agui_run.state["tools"]["c1"].items()
        assert expected.items() <= _deltas(ctx)[0][0]["value"].items()

    async def test_run_state_tool_entry_without_labels_is_null(self) -> None:
        ctx, mixin = _ctx(), AgUiMixin()
        await _started(mixin, ctx)

        await mixin._handle_tool_call_started(
            ctx,
            ToolCallStartedEvent(event=AgentRunEvent.TOOL_CALL_STARTED, tool=_tool(), timestamp=None, metadata=None),
        )

        entry = ctx.agui_run.state["tools"]["call_1"]
        assert (entry["toolDisplayName"], entry["functionDisplayName"]) == (None, None)

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
                error_type=RunErrorCode.MODEL_RATE_LIMITED,
                content="rate limited",
                error_details=None,
                timestamp=None,
                metadata=None,
            ),
        )

        roots = _roots(ctx)
        assert isinstance(roots[-2], AgUiStateDeltaOutput)
        assert isinstance(roots[-1], AgUiRunErrorOutput)
        assert roots[-1].event.code == "MODEL_RATE_LIMITED"
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
