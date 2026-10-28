"""AG-UI 1.0 stream invariants over real adapter → mixin → Struct streams (what the gateway relays).

Each scenario drives fake agno events through ``AgnoStreamAdapter`` and ``AgUiMixin``, round-trips
every output through ``google.protobuf.Struct`` exactly like ``ModuleRunner``/gateway, and checks the
resulting event list against the 1.0 lifecycle, pairing and attribution rules. When
``DIGITALKIN_AGUI_FIXTURES_OUT`` is set, each stream is also written there as JSON for the
reference-client replay (``@ag-ui/client``).
"""

from __future__ import annotations

import json
import os
import pathlib
import types
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from google.protobuf import json_format, struct_pb2

from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter
from digitalkin.community.agno.hitl import HitlEvents
from digitalkin.mixins.agui_mixin import AgUiMixin
from tests.community.agno.test_agno_adapter import (
    _FakeRunEvent,
    _FakeTeamRunEvent,
    _make_event,
    _make_tool,
    _make_tool_execution,
    fake_agno_modules,
)

pytestmark = pytest.mark.unit


class _Host(AgUiMixin):
    """Concrete mixin host."""


class _Wire:
    """Context stand-in that records every output as the client receives it (after the Struct hop)."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.callbacks = types.SimpleNamespace(send_message=self.send_message, logger=MagicMock())
        self.session = types.SimpleNamespace(current_ids=dict)
        self.storage = types.SimpleNamespace(read=AsyncMock(return_value=None))
        self.agui_run: Any = None

    async def send_message(self, output: Any) -> None:
        s = struct_pb2.Struct()
        s.update(output.model_dump(mode="json"))
        self.events.append(json_format.MessageToDict(s)["root"]["event"])


class StreamValidator:
    """Checks one AG-UI event stream against the 1.0 rules the SDK produces under."""

    @staticmethod
    def violations(events: list[dict[str, Any]]) -> list[str]:
        """Every rule broken by ``events``, as readable strings (empty when the stream conforms)."""
        out: list[str] = []
        texts: dict[str, Any] = {}
        tools: dict[str, Any] = {}
        spans: set[str] = set()
        reasonings: dict[str, Any] = {}
        active: set[str] = set()
        finished: set[str] = set()
        started_tools: set[str] = set()
        message_ids: set[str] = set()
        activities: dict[str, Any] = {}
        terminal: str | None = None

        def attr_ok(i: int, ev: dict[str, Any], opened: Any) -> None:
            if "subagentRunId" in ev and ev["subagentRunId"] != opened:
                out.append(f"#{i} {ev['type']}: attribution {ev['subagentRunId']} != opener {opened}")

        for i, ev in enumerate(events):
            kind = ev["type"]
            if kind.startswith("THINKING"):
                out.append(f"#{i} retired type {kind}")
            if i == 0 and kind not in {"RUN_STARTED", "RUN_ERROR"}:
                out.append(f"#0 stream starts with {kind}")
            if terminal is not None and not (
                kind == "RUN_STARTED" or (kind == "RUN_ERROR" and terminal == "RUN_FINISHED")
            ):
                out.append(f"#{i} {kind} after {terminal}")
            if any(value is None for value in ev.values()):
                out.append(f"#{i} {kind}: null field {[k for k, v in ev.items() if v is None]}")
            if "timestamp" in ev and not float(ev["timestamp"]).is_integer():
                out.append(f"#{i} {kind}: non-integer timestamp")
            sub = ev.get("subagentRunId")
            if sub is not None and not kind.startswith("SUBAGENT_") and sub not in active:
                out.append(f"#{i} {kind}: attributed to unannounced/closed subagent {sub}")

            if kind == "RUN_STARTED":
                if ev.get("protocolVersion") != "1.0":
                    out.append(f"#{i} RUN_STARTED protocolVersion={ev.get('protocolVersion')}")
                terminal = None
                texts, tools, reasonings, spans, active, finished = {}, {}, {}, set(), set(), set()
            elif kind == "SUBAGENT_STARTED":
                if sub in active or sub in finished:
                    out.append(f"#{i} subagent id {sub} reused")
                parent = ev.get("parentSubagentRunId")
                if parent is not None and parent not in active | finished:
                    out.append(f"#{i} parent {parent} cited before announced")
                active.add(sub)
            elif kind in {"SUBAGENT_FINISHED", "SUBAGENT_ERROR"}:
                if sub not in active:
                    out.append(f"#{i} {kind} for inactive {sub}")
                active.discard(sub)
                finished.add(sub)
                outcome = ev.get("outcome")
                if outcome is not None and outcome.get("type") not in {"success", "suspended"}:
                    out.append(f"#{i} unknown subagent outcome {outcome}")
            elif kind == "TEXT_MESSAGE_START":
                if ev["messageId"] in message_ids:
                    out.append(f"#{i} message id {ev['messageId']} reused")
                message_ids.add(ev["messageId"])
                texts[ev["messageId"]] = sub
            elif kind in {"TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END"}:
                if ev["messageId"] not in texts:
                    out.append(f"#{i} {kind} on unopened message {ev['messageId']}")
                else:
                    attr_ok(i, ev, texts[ev["messageId"]])
                    if kind == "TEXT_MESSAGE_END":
                        del texts[ev["messageId"]]
            elif kind == "TOOL_CALL_START":
                if ev["toolCallId"] in started_tools:
                    out.append(f"#{i} tool call id {ev['toolCallId']} reused")
                started_tools.add(ev["toolCallId"])
                tools[ev["toolCallId"]] = sub
            elif kind in {"TOOL_CALL_ARGS", "TOOL_CALL_END"}:
                if ev["toolCallId"] not in tools:
                    out.append(f"#{i} {kind} on unopened tool call {ev['toolCallId']}")
                else:
                    attr_ok(i, ev, tools[ev["toolCallId"]])
                    if kind == "TOOL_CALL_END":
                        del tools[ev["toolCallId"]]
            elif kind == "TOOL_CALL_RESULT":
                if ev["toolCallId"] not in started_tools or ev["toolCallId"] in tools:
                    out.append(f"#{i} result for a tool call not started and ended: {ev['toolCallId']}")
            elif kind == "REASONING_START":
                spans.add(ev["messageId"])
            elif kind == "REASONING_END":
                if ev["messageId"] not in spans:
                    out.append(f"#{i} REASONING_END for unopened span")
                spans.discard(ev["messageId"])
            elif kind == "REASONING_MESSAGE_START":
                if ev["messageId"] in message_ids:
                    out.append(f"#{i} reasoning message id reused")
                message_ids.add(ev["messageId"])
                reasonings[ev["messageId"]] = sub
            elif kind in {"REASONING_MESSAGE_CONTENT", "REASONING_MESSAGE_END"}:
                if ev["messageId"] not in reasonings:
                    out.append(f"#{i} {kind} on unopened reasoning message")
                else:
                    attr_ok(i, ev, reasonings[ev["messageId"]])
                    if kind == "REASONING_MESSAGE_END":
                        del reasonings[ev["messageId"]]
            elif kind == "ACTIVITY_SNAPSHOT":
                if not isinstance(ev.get("content"), dict):
                    out.append(f"#{i} activity content is not an object")
                if ev.get("replace") is not True:
                    out.append(f"#{i} activity replace not explicit")
                if ev["messageId"] in message_ids:
                    out.append(f"#{i} activity id collides with a text/reasoning message")
                activities[ev["messageId"]] = sub
            elif kind == "RUN_FINISHED":
                for name, still_open in (
                    ("text", texts),
                    ("tool", tools),
                    ("reasoning message", reasonings),
                    ("reasoning span", spans),
                    ("subagent", active),
                ):
                    if still_open:
                        out.append(f"#{i} RUN_FINISHED with open {name}: {sorted(still_open)}")
                terminal = kind
            elif kind == "RUN_ERROR":
                terminal = kind
        if terminal is None:
            out.append("stream has no terminal event")
        return out


async def _stream(name: str, agno_events: list[Any], *, pause_ids: list[str] | None = None) -> list[dict[str, Any]]:
    adapter, host, wire = AgnoStreamAdapter(), _Host(), _Wire()
    for agno_event in agno_events:
        for dk_event in adapter.to_digitalkin_events(agno_event):
            await host.send_message(wire, dk_event)  # type: ignore[arg-type]
    for dk_event in adapter.flush():
        await host.send_message(wire, dk_event)  # type: ignore[arg-type]
    if pause_ids is not None:
        await HitlEvents.emit_awaiting_tool_result(wire, thread_id="s1", run_id="r1", pending_tool_call_ids=pause_ids)  # type: ignore[arg-type]
    _dump(name, wire.events)
    return wire.events


def _dump(name: str, events: list[dict[str, Any]]) -> None:
    """Write a recorded stream to ``DIGITALKIN_AGUI_FIXTURES_OUT`` for the reference-client replay."""
    out = os.environ.get("DIGITALKIN_AGUI_FIXTURES_OUT")
    if out:
        pathlib.Path(out).mkdir(parents=True, exist_ok=True)
        (pathlib.Path(out) / f"{name}.json").write_text(json.dumps(events, indent=1), encoding="utf-8")


def _tool(call_id: str, run_id: str = "r1") -> list[Any]:
    tool = _make_tool(tool_call_id=call_id, tool_name="search", tool_args={"q": "x"}, result="ok")
    return [
        _make_event(_FakeRunEvent.tool_call_started, run_id=run_id, tool=tool),
        _make_event(_FakeRunEvent.tool_call_completed, run_id=run_id, tool=tool),
    ]


def _source(url: str, run_id: str = "r1") -> Any:
    return _make_event(
        _FakeRunEvent.custom_event, run_id=run_id, name="source_citation", value={"url": url, "title": "T"}
    )


def _start(run_id: str = "r1") -> Any:
    return _make_event(_FakeTeamRunEvent.run_started, run_id=run_id, session_id="s1")


def _member(run_id: str, name: str = "Alice", parent: str = "r1") -> Any:
    return _make_event(_FakeRunEvent.run_started, run_id=run_id, parent_run_id=parent, agent_name=name)


def _content(text: str, run_id: str = "r1", **kw: Any) -> Any:
    return _make_event(_FakeRunEvent.run_content, run_id=run_id, content=text, **kw)


class TestStreamInvariants:
    async def test_nominal_run(self) -> None:
        events = await _stream(
            "nominal",
            [
                _start(),
                _content(None, reasoning_content="thinking"),
                _content("Hello"),
                *_tool("c1"),
                *_tool("c2"),
                *_tool("c3"),
                _source("https://a.io/x"),
                _member("m1"),
                _content("member says", run_id="m1"),
                _source("https://b.io/", run_id="m1"),
                _make_event(_FakeRunEvent.run_completed, run_id="m1", parent_run_id="r1", content="done"),
                _source("https://a.io/x#again"),
                _content(" world"),
                _make_event(_FakeTeamRunEvent.run_completed, run_id="r1", content="final"),
            ],
        )
        assert StreamValidator.violations(events) == []
        kinds = [e["type"] for e in events]
        assert kinds[0] == "RUN_STARTED"
        assert kinds[-1] == "RUN_FINISHED"
        # A1: the client merges metadata onto messages and sends it back in RunAgentInput, so only
        # identity keys and tool-call labels may ever be written there (no prompt, token or secret).
        written = {key for e in events for key in e.get("metadata", {}).get("digitalkin", {})}
        assert written <= {"source", "name", "id", "parent_run_id"}
        labels = {"toolDisplayName", "functionDisplayName"}
        assert all(
            set(e.get("metadata", {}))
            <= ({"digitalkin"} | labels if e["type"] == "TOOL_CALL_START" else {"digitalkin"})
            for e in events
        )
        activities = [e for e in events if e["type"] == "ACTIVITY_SNAPSHOT"]
        assert [(a.get("subagentRunId"), len(a["content"]["sources"])) for a in activities] == [(None, 1), ("m1", 1)]

    async def test_hitl_pause_suspends_open_subagent_and_closes_open_tool_call(self) -> None:
        external = _make_tool_execution(
            tool_call_id="ext1", tool_name="ask_user", tool_args={}, external_execution_required=True
        )
        events = await _stream(
            "hitl_pause",
            [
                _start(),
                _member("m1"),
                _content("checking", run_id="m1"),
                _make_event(
                    _FakeRunEvent.tool_call_started, run_id="m1", tool=_make_tool(tool_call_id="c9", tool_name="slow")
                ),
                _make_event(_FakeTeamRunEvent.run_paused, run_id="r1", tools=[external], requirements=[]),
            ],
            pause_ids=["ext1"],
        )
        assert StreamValidator.violations(events) == []
        finished = next(e for e in events if e["type"] == "SUBAGENT_FINISHED")
        assert finished["outcome"] == {"type": "suspended"}
        closed = [e for e in events if e["type"] == "TOOL_CALL_END" and e["toolCallId"] == "c9"]
        assert closed
        assert closed[0]["subagentRunId"] == "m1"
        assert events[-1]["type"] == "RUN_FINISHED"
        assert events[-1]["outcome"]["type"] == "interrupt"

    async def test_subagent_error_then_parent_completes(self) -> None:
        events = await _stream(
            "subagent_error",
            [
                _start(),
                _member("m1"),
                _content("partial", run_id="m1"),
                _make_event(
                    _FakeRunEvent.tool_call_started, run_id="m1", tool=_make_tool(tool_call_id="c1", tool_name="t")
                ),
                _make_event(_FakeRunEvent.run_error, run_id="m1", content="model refused", error_type="ModelError"),
                _content("recovered"),
                _make_event(_FakeTeamRunEvent.run_completed, run_id="r1"),
            ],
        )
        assert StreamValidator.violations(events) == []
        assert [e["type"] for e in events if e["type"].startswith("SUBAGENT_")] == [
            "SUBAGENT_STARTED",
            "SUBAGENT_ERROR",
        ]

    async def test_parallel_members_interleave_without_cross_closing(self) -> None:
        events = await _stream(
            "parallel",
            [
                _start(),
                _member("m1", "Alice"),
                _member("m2", "Bob"),
                _content("a1", run_id="m1"),
                _content("b1", run_id="m2"),
                _content("a2", run_id="m1"),
                _make_event(_FakeRunEvent.run_completed, run_id="m1", parent_run_id="r1"),
                _content("b2", run_id="m2"),
                _make_event(_FakeRunEvent.run_completed, run_id="m2", parent_run_id="r1"),
                _make_event(_FakeTeamRunEvent.run_completed, run_id="r1"),
            ],
        )
        assert StreamValidator.violations(events) == []
        b_contents = [
            e["delta"] for e in events if e["type"] == "TEXT_MESSAGE_CONTENT" and e.get("subagentRunId") == "m2"
        ]
        assert b_contents == ["b1", "b2"]

    async def test_top_level_error_abandons_open_invocations(self) -> None:
        events = await _stream(
            "run_error",
            [
                _start(),
                _member("m1"),
                _make_event(
                    _FakeRunEvent.tool_call_started, run_id="m1", tool=_make_tool(tool_call_id="c1", tool_name="t")
                ),
                _make_event(_FakeTeamRunEvent.run_error, run_id="r1", content="provider down"),
            ],
        )
        assert StreamValidator.violations(events) == []
        assert events[-1]["type"] == "RUN_ERROR"
        assert not [e for e in events if e["type"] in {"SUBAGENT_FINISHED", "TOOL_CALL_END"}]

    async def test_open_tool_call_and_orphan_reasoning_are_closed_before_run_finished(self) -> None:
        events = await _stream(
            "closures",
            [
                _start(),
                _make_event(_FakeRunEvent.reasoning_content_delta, run_id="r1", reasoning_content="orphan"),
                _make_event(
                    _FakeRunEvent.tool_call_started, run_id="r1", tool=_make_tool(tool_call_id="c1", tool_name="t")
                ),
                _make_event(_FakeTeamRunEvent.run_completed, run_id="r1"),
            ],
        )
        assert StreamValidator.violations(events) == []
        result = next(e for e in events if e["type"] == "TOOL_CALL_RESULT")
        assert json.loads(result["content"]) == {"error": "tool call did not complete before its run ended"}


class TestToolCallTracking:
    async def test_external_tool_started_before_pause_is_closed_once_not_reopened(self) -> None:
        """If agno streams an external tool's start before pausing, the pause closes it, never re-opens it."""
        external = _make_tool_execution(
            tool_call_id="ext1", tool_name="ask_user", tool_args={}, external_execution_required=True
        )
        events = await _stream(
            "hitl_external_started",
            [
                _start(),
                _make_event(
                    _FakeRunEvent.tool_call_started,
                    run_id="r1",
                    tool=_make_tool(tool_call_id="ext1", tool_name="ask_user"),
                ),
                _make_event(_FakeTeamRunEvent.run_paused, run_id="r1", tools=[external], requirements=[]),
            ],
            pause_ids=["ext1"],
        )
        assert StreamValidator.violations(events) == []
        ext = [e["type"] for e in events if e.get("toolCallId") == "ext1"]
        assert ext == ["TOOL_CALL_START", "TOOL_CALL_END"]

    def test_completed_call_leaves_tracking_and_member_close_spares_the_parent(self) -> None:
        adapter = AgnoStreamAdapter()
        adapter.to_digitalkin_events(_start())
        done = _make_tool(tool_call_id="c-done", tool_name="t", result="ok")
        adapter.to_digitalkin_events(_make_event(_FakeRunEvent.tool_call_started, run_id="r1", tool=done))
        adapter.to_digitalkin_events(_make_event(_FakeRunEvent.tool_call_completed, run_id="r1", tool=done))
        adapter.to_digitalkin_events(
            _make_event(
                _FakeRunEvent.tool_call_started, run_id="r1", tool=_make_tool(tool_call_id="c-parent", tool_name="t")
            )
        )
        adapter.to_digitalkin_events(_member("m1"))
        adapter.to_digitalkin_events(
            _make_event(
                _FakeRunEvent.tool_call_started, run_id="m1", tool=_make_tool(tool_call_id="c-member", tool_name="t")
            )
        )

        closed = adapter.to_digitalkin_events(_make_event(_FakeRunEvent.run_completed, run_id="m1", parent_run_id="r1"))

        closed_ids = [e.tool.tool_call_id for e in closed if e.event == "tool_call_error"]
        assert closed_ids == ["c-member"]
        assert list(adapter._open_tool_calls) == ["c-parent"]


class TestValidatorIsNotVacuous:
    """The validator must flag the defects the SDK used to produce."""

    def test_flags_the_pre_migration_defects(self) -> None:
        events = [
            {"type": "RUN_STARTED", "threadId": "t", "runId": "r"},
            {"type": "SUBAGENT_STARTED", "subagentRunId": "m1", "name": "a"},
            {"type": "TOOL_CALL_START", "toolCallId": "c1", "toolCallName": "t", "subagentRunId": "m1"},
            {"type": "REASONING_MESSAGE_CONTENT", "messageId": "", "delta": "x"},
            {"type": "TOOL_CALL_END", "toolCallId": "other-uuid"},
            {"type": "SUBAGENT_FINISHED", "subagentRunId": "m1"},
            {"type": "RUN_FINISHED", "threadId": "t", "runId": "r"},
        ]
        found = StreamValidator.violations(events)
        assert any("protocolVersion" in v for v in found)
        assert any("unopened reasoning message" in v for v in found)
        assert any("unopened tool call other-uuid" in v for v in found)
        assert any("RUN_FINISHED with open tool" in v for v in found)
