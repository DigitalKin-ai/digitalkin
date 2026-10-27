"""AG-UI producer hot-path latency and per-run memory (A2/A3 of the AG-UI 1.0 migration).

Each case drives the real ``AgnoStreamAdapter`` → ``AgUiMixin`` → ``AgUiOutput.model_dump`` →
``Struct.update`` chain, which is exactly what ``ModuleRunner._on_output`` does before XADD.
Numbers are written to ``DIGITALKIN_AGUI_BENCH_OUT`` (JSON) when set, so a run before and after
a change can be diffed; assertions only cover correctness and memory growth, never timing.
"""

from __future__ import annotations

import gc
import json
import logging
import os
import pathlib
import statistics
import time
import tracemalloc
import types
from typing import Any

import pytest
from google.protobuf import struct_pb2
from tests.community.agno.test_agno_adapter import (
    _FakeRunEvent,
    _make_event,
    _make_tool,
    _make_tool_execution,
    fake_agno_modules,
)

from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter
from digitalkin.mixins.agui_mixin import AgUiMixin
from digitalkin.models.events import (
    AgentRunEvent,
    ReasoningContentDeltaEvent,
    RunContentEvent,
    RunStartedEvent,
    ToolCallStartedEvent,
    ToolInfo,
)

pytestmark = [pytest.mark.stress, pytest.mark.timeout(300)]


class _Sink:
    """Context stand-in whose ``send_message`` serialises like ``ModuleRunner._on_output``."""

    def __init__(self) -> None:
        self.count = 0
        self.callbacks = types.SimpleNamespace(send_message=self.send_message, logger=logging.getLogger("bench"))
        self.session = types.SimpleNamespace(current_ids=dict)
        self.agui_run: Any = None

    async def send_message(self, output: Any) -> None:
        struct_pb2.Struct().update(output.model_dump(mode="json"))
        self.count += 1


class _Trigger(AgUiMixin):
    """Concrete mixin host."""


class TestAgUiHotPath:
    """Latency (A3) and memory (A2) baselines for the AG-UI producer."""

    @staticmethod
    def _record(name: str, samples_ns: list[int]) -> dict[str, float]:
        samples_ns.sort()
        n = len(samples_ns)
        stats = {
            "n": n,
            "p50_us": samples_ns[n // 2] / 1000,
            "p95_us": samples_ns[int(n * 0.95)] / 1000,
            "p99_us": samples_ns[int(n * 0.99)] / 1000,
            "mean_us": statistics.fmean(samples_ns) / 1000,
        }
        TestAgUiHotPath._save(name, stats)
        return stats

    @staticmethod
    def _save(name: str, result: dict[str, Any]) -> None:
        """Merge ``result`` under ``name`` into ``DIGITALKIN_AGUI_BENCH_OUT`` when set."""
        out = os.environ.get("DIGITALKIN_AGUI_BENCH_OUT")
        if not out:
            return
        path = pathlib.Path(out)
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        data[name] = result
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    @staticmethod
    def _run_events(i: int) -> list[Any]:
        """One representative run: reasoning, text, 3 tools, 1 subagent, 3 sources."""
        run_id, sub_id = f"r{i}", f"sub{i}"
        events = [_make_event(_FakeRunEvent.run_started, run_id=run_id, session_id="s1")]
        events += [_make_event(_FakeRunEvent.run_content, run_id=run_id, reasoning_content="think ") for _ in range(10)]
        events += [_make_event(_FakeRunEvent.run_content, run_id=run_id, content="tok ") for _ in range(30)]
        for t in range(3):
            tool = _make_tool(tool_call_id=f"{run_id}-t{t}", tool_name="search", tool_args={"q": "x"}, result="ok")
            events.append(_make_event(_FakeRunEvent.tool_call_started, run_id=run_id, tool=tool))
            events.append(_make_event(_FakeRunEvent.tool_call_completed, run_id=run_id, tool=tool))
        events.append(_make_event(_FakeRunEvent.run_started, run_id=sub_id, parent_run_id=run_id, agent_name="member"))
        events += [_make_event(_FakeRunEvent.run_content, run_id=sub_id, content="sub ") for _ in range(5)]
        events.append(_make_event(_FakeRunEvent.run_completed, run_id=sub_id, parent_run_id=run_id, content="done"))
        events += [
            _make_event(
                _FakeRunEvent.custom_event,
                run_id=run_id,
                name="source_citation",
                value={"url": f"https://example.com/{i}/{s}", "title": "T", "description": "D"},
            )
            for s in range(3)
        ]
        events += [_make_event(_FakeRunEvent.run_content, run_id=run_id, content="tail ") for _ in range(10)]
        events.append(_make_event(_FakeRunEvent.run_completed, run_id=run_id, content="final"))
        return events

    @staticmethod
    def _paused_run_events(i: int) -> list[Any]:
        """A run that ends on an external-tool HITL pause."""
        run_id = f"p{i}"
        tool = _make_tool_execution(
            tool_call_id=f"{run_id}-ext", tool_name="front_tool", tool_args={}, external_execution_required=True
        )
        return [
            _make_event(_FakeRunEvent.run_started, run_id=run_id, session_id="s1"),
            _make_event(_FakeRunEvent.run_content, run_id=run_id, content="before pause"),
            _make_event(_FakeRunEvent.run_paused, run_id=run_id, tools=[tool], requirements=[]),
        ]

    @staticmethod
    async def _drive(adapter: AgnoStreamAdapter, trigger: _Trigger, ctx: _Sink, events: list[Any]) -> None:
        for agno_event in events:
            for dk_event in adapter.to_digitalkin_events(agno_event):
                await trigger.send_message(ctx, dk_event)  # type: ignore[arg-type]
        for dk_event in adapter.flush():
            await trigger.send_message(ctx, dk_event)  # type: ignore[arg-type]

    async def _micro(self, name: str, event: Any, iterations: int = 10_000) -> None:
        trigger, ctx = _Trigger(), _Sink()
        await trigger.send_message(ctx, RunStartedEvent(event=AgentRunEvent.RUN_STARTED, run_id="r", thread_id="t"))  # type: ignore[arg-type]
        for _ in range(500):
            await trigger.send_message(ctx, event)  # type: ignore[arg-type]
        samples = []
        clock = time.perf_counter_ns
        for _ in range(iterations):
            start = clock()
            await trigger.send_message(ctx, event)  # type: ignore[arg-type]
            samples.append(clock() - start)
        assert ctx.count > iterations
        self._record(name, samples)

    async def test_text_message_content(self) -> None:
        """DK RunContent → TEXT_MESSAGE_CONTENT → Struct."""
        await self._micro(
            "TEXT_MESSAGE_CONTENT",
            RunContentEvent(event=AgentRunEvent.RUN_CONTENT, content="hello world ", message_id="m1"),
        )

    async def test_tool_call_args(self) -> None:
        """DK ToolCallStarted → TOOL_CALL_START + TOOL_CALL_ARGS (+ state delta) → Struct."""
        await self._micro(
            "TOOL_CALL_START_ARGS",
            ToolCallStartedEvent(
                event=AgentRunEvent.TOOL_CALL_STARTED,
                tool=ToolInfo(
                    tool_call_id="tc1",
                    tool_name="search",
                    tool_args={"q": "weather in Lyon", "n": 3},
                    tool_display_name="Web search",
                    function_display_name="Search",
                ),
            ),
        )

    async def test_reasoning_message_content(self) -> None:
        """DK ReasoningContentDelta → REASONING_MESSAGE_CONTENT → Struct."""
        await self._micro(
            "REASONING_MESSAGE_CONTENT",
            ReasoningContentDeltaEvent(event=AgentRunEvent.REASONING_CONTENT_DELTA, delta="step ", reasoning_id="r1"),
        )

    async def test_full_run_per_event_latency(self) -> None:
        """Agno event → adapter → mixin → Struct, per agno event, over a representative run."""
        samples: list[int] = []
        clock = time.perf_counter_ns
        for i in range(200):
            adapter, trigger, ctx = AgnoStreamAdapter(), _Trigger(), _Sink()
            for agno_event in self._run_events(i):
                start = clock()
                for dk_event in adapter.to_digitalkin_events(agno_event):
                    await trigger.send_message(ctx, dk_event)  # type: ignore[arg-type]
                samples.append(clock() - start)
            assert ctx.count > 0
        self._record("FULL_RUN_PER_AGNO_EVENT", samples[len(samples) // 10 :])

    async def test_memory_flat_over_runs(self) -> None:
        """Same adapter and mixin across 600 runs (sources, subagent, pause): no net growth."""
        adapter, trigger, ctx = AgnoStreamAdapter(), _Trigger(), _Sink()

        async def batch(start: int, count: int) -> None:
            for i in range(start, start + count):
                await self._drive(adapter, trigger, ctx, self._run_events(i))
                if i % 50 == 0:
                    await self._drive(adapter, trigger, ctx, self._paused_run_events(i))

        await batch(0, 100)
        gc.collect()
        tracemalloc.start()
        await batch(100, 100)
        gc.collect()
        first, _ = tracemalloc.get_traced_memory()
        await batch(200, 400)
        gc.collect()
        second, _ = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        growth = second - first
        self._save("MEMORY_600_RUNS", {"after_200_b": first, "after_600_b": second, "growth_b": growth})
        assert growth < 64 * 1024, f"per-run state leaks: +{growth} B over 400 runs"
