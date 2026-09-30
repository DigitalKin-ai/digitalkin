"""PausedRunStore persists a trimmed payload that still resumes."""

from unittest.mock import AsyncMock

import pytest

pytest.importorskip("agno", reason="optional agno dependency not installed")

from agno.models.message import Message
from agno.models.response import ToolExecution
from agno.run.agent import RunOutput, RunStartedEvent

from digitalkin.community.agno.hitl import PausedRunStore


def _paused_run() -> RunOutput:
    return RunOutput(
        run_id="run-1",
        session_id="thread-1",
        messages=[
            Message(role="user", content="old question", from_history=True),
            Message(role="assistant", content="old answer", from_history=True),
            Message(role="user", content="new question"),
            Message(
                role="assistant",
                tool_calls=[{"id": "call-1", "type": "function", "function": {"name": "ask", "arguments": "{}"}}],
            ),
        ],
        events=[RunStartedEvent(run_id="run-1")],
        tools=[ToolExecution(tool_call_id="call-1", tool_name="ask", external_execution_required=True)],
    )


async def test_save_drops_history_messages_and_events() -> None:
    storage = AsyncMock()
    run = _paused_run()

    info = await PausedRunStore(storage).save(run, "thread-1")

    payload = storage.upsert.await_args.kwargs["data"]["payload"]
    assert [m.get("content") for m in payload["messages"]] == ["new question", None]
    assert "events" not in payload
    assert info.pending_tool_call_ids == ["call-1"]
    # The live run is untouched: the front still gets every message.
    assert len(run.messages or []) == 4


async def test_trimmed_payload_round_trips_for_resume() -> None:
    storage = AsyncMock()
    await PausedRunStore(storage).save(_paused_run(), "thread-1")

    restored = RunOutput.from_dict(storage.upsert.await_args.kwargs["data"]["payload"])

    assert restored.run_id == "run-1"
    assert not any(m.from_history for m in restored.messages or [])
    assert [t.tool_call_id for t in restored.tools or []] == ["call-1"]
    assert not restored.events
