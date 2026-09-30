"""A HITL pause ends the run with the standard AG-UI interrupt outcome plus the legacy result."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("agno", reason="optional agno dependency not installed")

from digitalkin.community.agno.hitl import HitlEvents
from digitalkin.mixins.agui_mixin import AgUiRunState
from digitalkin.models.module.ag_ui import AgUiRunFinishedOutput, AgUiStateDeltaOutput

pytestmark = pytest.mark.unit


def _ctx(run: AgUiRunState | None) -> MagicMock:
    ctx = MagicMock()
    ctx.callbacks.send_message = AsyncMock()
    ctx.agui_run = run
    return ctx


async def test_pause_emits_interrupt_outcome_and_legacy_result() -> None:
    run = AgUiRunState()
    run.state["tools"]["call-1"] = {"name": "ask_user", "status": "completed"}
    ctx = _ctx(run)

    await HitlEvents.emit_awaiting_tool_result(ctx, thread_id="t1", run_id="r1", pending_tool_call_ids=["call-1"])

    delta, finished = (call.args[0].root for call in ctx.callbacks.send_message.await_args_list)
    assert isinstance(delta, AgUiStateDeltaOutput)
    assert run.state["tools"]["call-1"]["status"] == "awaiting_input"
    assert run.state["run"]["status"] == "interrupted"
    assert run.state["interrupts"] == [
        {"id": "call-1", "reason": "tool_call", "toolCallId": "call-1", "message": "Waiting for the result of ask_user"}
    ]
    assert run.open is False
    assert isinstance(finished, AgUiRunFinishedOutput)
    assert finished.event.outcome.type == "interrupt"
    assert [i.tool_call_id for i in finished.event.outcome.interrupts] == ["call-1"]
    assert finished.event.result == {"status": "awaiting_tool_result", "pending_tool_call_ids": ["call-1"]}


async def test_pause_without_open_run_still_carries_outcome() -> None:
    ctx = _ctx(None)

    await HitlEvents.emit_awaiting_tool_result(ctx, thread_id="t1", run_id="r1", pending_tool_call_ids=["call-9"])

    (finished,) = (call.args[0].root for call in ctx.callbacks.send_message.await_args_list)
    assert finished.event.outcome.interrupts[0].message == "Waiting for the result of a frontend tool"


async def test_pause_with_no_pending_ids_omits_outcome() -> None:
    ctx = _ctx(None)

    await HitlEvents.emit_awaiting_tool_result(ctx, thread_id="t1", run_id="r1", pending_tool_call_ids=[])

    (finished,) = (call.args[0].root for call in ctx.callbacks.send_message.await_args_list)
    assert finished.event.outcome is None
