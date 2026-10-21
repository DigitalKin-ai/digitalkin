"""Tests for ``AgUiMixin`` AG-UI run_id / thread_id propagation.

The mixin is the single point where two distinct identifiers meet:

* the **AG-UI client** ULID, primed by the trigger from ``RunAgentInput``;
* the **agno** UUID4, attached by the adapter to every wrapped event.

The AG-UI protocol requires that ``RUN_FINISHED`` echoes the same ``run_id`` as
``RUN_STARTED``. These tests pin the resolution policy:

#. If the trigger primed ``self._run_id`` (or ``self._thread_id``), keep it —
   never let an incoming event overwrite it.
#. Otherwise fall back to ``event.run_id`` / ``event.thread_id``.
#. If both are empty, mint a fresh ``uuid4``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.mixins.agui_mixin import AgUiMixin
from digitalkin.models.events import (
    AgentRunEvent,
    CustomEvent,
    ReasoningCompletedEvent,
    ReasoningContentDeltaEvent,
    ReasoningStartedEvent,
    RunCompletedEvent,
    RunContentEvent,
    RunStartedEvent,
    SubagentErrorEvent,
    SubagentFinishedEvent,
    SubagentStartedEvent,
    TextMessageCompletedEvent,
    TextMessageStartedEvent,
    ToolCallCompletedEvent,
    ToolCallErrorEvent,
    ToolCallStartedEvent,
    ToolInfo,
)
from digitalkin.models.module.ag_ui import (
    AgUiRunFinishedOutput,
    AgUiRunStartedOutput,
    AgUiSubagentErrorOutput,
    AgUiSubagentFinishedOutput,
    AgUiSubagentStartedOutput,
    AgUiTextMessageStartOutput,
)

CLIENT_THREAD_ID = "missions:01kpwyz1xm5t0xc847mwkr1tm0"
CLIENT_RUN_ID = "01kpwyz3xpncrkccnsa5a9g5fh"
AGNO_RUN_ID = "97882532-f1df-4ada-bde3-c760f8be8e13"
AGNO_THREAD_ID = "agno-session-id"


def _make_context() -> MagicMock:
    """Mock ``ModuleContext`` with the bits the mixin reads."""
    ctx = MagicMock()
    ctx.callbacks = MagicMock()
    ctx.callbacks.send_message = AsyncMock()
    ctx.callbacks.logger = MagicMock()
    ctx.session = MagicMock()
    ctx.session.current_ids = MagicMock(return_value={})
    ctx.agui_run = None
    return ctx


def _emitted_event(ctx: MagicMock, expected_cls: type) -> Any:
    """Pull the last AG-UI event of ``expected_cls`` sent through ``send_message``."""
    roots = [call.args[0].root for call in ctx.callbacks.send_message.await_args_list]
    matching = [root for root in roots if isinstance(root, expected_cls)]
    assert matching, f"no {expected_cls.__name__} in {[type(r).__name__ for r in roots]}"
    return matching[-1].event


def _started(*, run_id: str | None, thread_id: str | None) -> RunStartedEvent:
    """Build a ``RunStartedEvent`` with explicit Nones for pyright friendliness."""
    return RunStartedEvent(
        event=AgentRunEvent.RUN_STARTED,
        run_id=run_id,
        thread_id=thread_id,
        timestamp=None,
        metadata=None,
    )


def _completed(*, run_id: str | None) -> RunCompletedEvent:
    """Build a ``RunCompletedEvent`` with explicit Nones."""
    return RunCompletedEvent(
        event=AgentRunEvent.RUN_COMPLETED,
        run_id=run_id,
        timestamp=None,
        metadata=None,
        final_content=None,
        usage=None,
        message_id=None,
    )


class TestRunStartedResolution:
    """``_handle_run_started`` must honour a primed ``_run_id`` / ``_thread_id``."""

    @pytest.mark.asyncio
    async def test_primed_run_id_survives_agno_event(self) -> None:
        """Trigger primes ULID; event carries agno UUID4 → emit ULID."""
        mixin = AgUiMixin()
        mixin._thread_id = CLIENT_THREAD_ID
        mixin._run_id = CLIENT_RUN_ID
        ctx = _make_context()

        await mixin._handle_run_started(ctx, _started(run_id=AGNO_RUN_ID, thread_id=AGNO_THREAD_ID))

        emitted = _emitted_event(ctx, AgUiRunStartedOutput)
        assert emitted.run_id == CLIENT_RUN_ID
        assert emitted.thread_id == CLIENT_THREAD_ID
        assert mixin._run_id == CLIENT_RUN_ID
        assert mixin._thread_id == CLIENT_THREAD_ID

    @pytest.mark.asyncio
    async def test_unprimed_falls_back_to_event_ids(self) -> None:
        """No prime → event ids fill in (Ada-style flow)."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_run_started(ctx, _started(run_id=AGNO_RUN_ID, thread_id=AGNO_THREAD_ID))

        emitted = _emitted_event(ctx, AgUiRunStartedOutput)
        assert emitted.run_id == AGNO_RUN_ID
        assert emitted.thread_id == AGNO_THREAD_ID
        assert mixin._run_id == AGNO_RUN_ID
        assert mixin._thread_id == AGNO_THREAD_ID

    @pytest.mark.asyncio
    async def test_no_ids_anywhere_mints_uuid4(self) -> None:
        """Empty prime AND empty event ids → fresh uuid4 fallback."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_run_started(ctx, _started(run_id=None, thread_id=None))

        emitted = _emitted_event(ctx, AgUiRunStartedOutput)
        assert emitted.run_id, "run_id must not be empty"
        assert emitted.thread_id, "thread_id must not be empty"
        assert mixin._run_id == emitted.run_id
        assert mixin._thread_id == emitted.thread_id


class TestRunCompletedResolution:
    """``_handle_run_completed`` must echo whatever ``RUN_STARTED`` emitted."""

    @pytest.mark.asyncio
    async def test_primed_run_id_used_in_run_finished(self) -> None:
        """Regression: agno UUID4 must NOT win over the primed ULID.

        This is the bug that caused ``RUN_STARTED`` to carry the AG-UI ULID
        while ``RUN_FINISHED`` carried agno's UUID4 — the client could not
        correlate the closure and the run looked orphaned.
        """
        mixin = AgUiMixin()
        mixin._thread_id = CLIENT_THREAD_ID
        mixin._run_id = CLIENT_RUN_ID
        ctx = _make_context()

        await mixin._handle_run_completed(ctx, _completed(run_id=AGNO_RUN_ID))

        emitted = _emitted_event(ctx, AgUiRunFinishedOutput)
        assert emitted.run_id == CLIENT_RUN_ID
        assert emitted.thread_id == CLIENT_THREAD_ID

    @pytest.mark.asyncio
    async def test_unprimed_falls_back_to_event_run_id(self) -> None:
        """Without a prime, the agno run_id is the only id available."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_run_completed(ctx, _completed(run_id=AGNO_RUN_ID))

        emitted = _emitted_event(ctx, AgUiRunFinishedOutput)
        assert emitted.run_id == AGNO_RUN_ID

    @pytest.mark.asyncio
    async def test_no_ids_anywhere_mints_uuid4(self) -> None:
        """Defensive fallback — never emit an empty run_id."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_run_completed(ctx, _completed(run_id=None))

        emitted = _emitted_event(ctx, AgUiRunFinishedOutput)
        assert emitted.run_id, "run_id must not be empty"


class TestEndToEndConsistency:
    """``RUN_STARTED`` and ``RUN_FINISHED`` must always carry the same ids."""

    @pytest.mark.asyncio
    async def test_primed_ids_match_through_full_lifecycle(self) -> None:
        """Trigger primes → start emits ULID → finish emits same ULID."""
        mixin = AgUiMixin()
        mixin._thread_id = CLIENT_THREAD_ID
        mixin._run_id = CLIENT_RUN_ID
        ctx = _make_context()

        await mixin._handle_run_started(ctx, _started(run_id=AGNO_RUN_ID, thread_id=AGNO_THREAD_ID))
        await mixin._handle_run_completed(ctx, _completed(run_id=AGNO_RUN_ID))

        started = _emitted_event(ctx, AgUiRunStartedOutput)
        finished = _emitted_event(ctx, AgUiRunFinishedOutput)
        assert started.run_id == finished.run_id == CLIENT_RUN_ID
        assert started.thread_id == finished.thread_id == CLIENT_THREAD_ID

    @pytest.mark.asyncio
    async def test_unprimed_ids_match_through_full_lifecycle(self) -> None:
        """No prime → start adopts agno ids → finish echoes them."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_run_started(ctx, _started(run_id=AGNO_RUN_ID, thread_id=AGNO_THREAD_ID))
        await mixin._handle_run_completed(ctx, _completed(run_id=AGNO_RUN_ID))

        started = _emitted_event(ctx, AgUiRunStartedOutput)
        finished = _emitted_event(ctx, AgUiRunFinishedOutput)
        assert started.run_id == finished.run_id == AGNO_RUN_ID
        assert started.thread_id == finished.thread_id == AGNO_THREAD_ID

    @pytest.mark.asyncio
    async def test_run_started_does_not_overwrite_existing_run_id(self) -> None:
        """A second RUN_STARTED in the same stream must not clobber state."""
        mixin = AgUiMixin()
        mixin._run_id = CLIENT_RUN_ID
        mixin._thread_id = CLIENT_THREAD_ID
        ctx = _make_context()

        await mixin._handle_run_started(ctx, _started(run_id=AGNO_RUN_ID, thread_id=AGNO_THREAD_ID))
        await mixin._handle_run_started(ctx, _started(run_id="another-spurious-id", thread_id="another-thread"))

        assert mixin._run_id == CLIENT_RUN_ID
        assert mixin._thread_id == CLIENT_THREAD_ID


class TestSubAgentLabelling:
    """The author label and step lifecycle must reach the wire as structured fields."""

    @pytest.mark.asyncio
    async def test_author_name_is_forwarded_to_text_message_start(self) -> None:
        """A labelled bubble carries ``name`` so the client can attribute it."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_text_message_started(
            ctx,
            TextMessageStartedEvent(
                event=AgentRunEvent.TEXT_MESSAGE_STARTED,
                message_id="m1",
                name="Alice",
                timestamp=None,
                metadata=None,
            ),
        )

        event = _emitted_event(ctx, AgUiTextMessageStartOutput)
        assert event.message_id == "m1"
        # `name` is an extra field until ag-ui-protocol types it, so assert on the payload.
        assert event.model_dump(by_alias=True, exclude_none=True)["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_unlabelled_bubble_omits_name(self) -> None:
        """A top-level bubble must not carry a ``name`` key at all."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_text_message_started(
            ctx,
            TextMessageStartedEvent(
                event=AgentRunEvent.TEXT_MESSAGE_STARTED,
                message_id="m1",
                name=None,
                timestamp=None,
                metadata=None,
            ),
        )

        event = _emitted_event(ctx, AgUiTextMessageStartOutput)
        assert "name" not in event.model_dump(by_alias=True, exclude_none=True)

    @pytest.mark.asyncio
    async def test_subagent_lifecycle_is_emitted(self) -> None:
        """Delegation boundaries surface as AG-UI SUBAGENT_STARTED / SUBAGENT_FINISHED."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_subagent_started(
            ctx,
            SubagentStartedEvent(
                event=AgentRunEvent.SUBAGENT_STARTED,
                subagent_run_id="m1",
                name="Alice",
                timestamp=None,
                metadata=None,
            ),
        )
        started = _emitted_event(ctx, AgUiSubagentStartedOutput)
        assert (started.subagent_run_id, started.name) == ("m1", "Alice")

        await mixin._handle_subagent_finished(
            ctx,
            SubagentFinishedEvent(
                event=AgentRunEvent.SUBAGENT_FINISHED,
                subagent_run_id="m1",
                result="done",
                timestamp=None,
                metadata=None,
            ),
        )
        finished = _emitted_event(ctx, AgUiSubagentFinishedOutput)
        assert (finished.subagent_run_id, finished.result) == ("m1", "done")

    @pytest.mark.asyncio
    async def test_subagent_error_does_not_end_the_run(self) -> None:
        """A failing child emits SUBAGENT_ERROR; RUN_ERROR would kill the whole AG-UI stream."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_subagent_error(
            ctx,
            SubagentErrorEvent(
                event=AgentRunEvent.SUBAGENT_ERROR,
                subagent_run_id="m1",
                message="boom",
                code="ValueError",
                timestamp=None,
                metadata=None,
            ),
        )
        errored = _emitted_event(ctx, AgUiSubagentErrorOutput)
        assert (errored.subagent_run_id, errored.message, errored.code) == ("m1", "boom", "ValueError")

    @pytest.mark.asyncio
    async def test_attribution_is_forwarded_onto_agui_events(self) -> None:
        """``subagent_run_id`` and namespaced ``metadata`` must survive the conversion.

        They are the whole attribution channel: without them a client cannot tell which agent
        produced a bubble when several stream at once.
        """
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_text_message_started(
            ctx,
            TextMessageStartedEvent(
                event=AgentRunEvent.TEXT_MESSAGE_STARTED,
                message_id="msg-1",
                name="Alice",
                subagent_run_id="m1",
                timestamp=None,
                metadata={"source": "agent", "parent_run_id": "team-r1"},
            ),
        )

        event = _emitted_event(ctx, AgUiTextMessageStartOutput)
        assert event.subagent_run_id == "m1"
        # Namespaced: AG-UI reserves the "ag-ui" key and leaves the rest to the application.
        assert event.metadata == {"digitalkin": {"source": "agent", "parent_run_id": "team-r1"}}


class _Host(AgUiMixin):
    """Concrete host: the dispatch table is built per subclass."""


class TestAgUi10ProducerConformance:
    """AG-UI 1.0 producer rules the mixin owns."""

    async def test_run_started_declares_sdk_protocol_version(self) -> None:
        """RUN_STARTED carries the SDK constant, serialised as ``protocolVersion``."""
        from ag_ui.core import PROTOCOL_VERSION

        ctx = _make_context()
        await AgUiMixin()._handle_run_started(ctx, _started(run_id="r1", thread_id="t1"))

        wire = ctx.callbacks.send_message.await_args_list[0].args[0].model_dump(mode="json")["root"]["event"]
        assert wire["protocolVersion"] == PROTOCOL_VERSION == "1.0"

    @pytest.mark.parametrize(
        "event",
        [
            ToolCallStartedEvent(event=AgentRunEvent.TOOL_CALL_STARTED, tool=ToolInfo(tool_name="search")),
            ToolCallCompletedEvent(event=AgentRunEvent.TOOL_CALL_COMPLETED, tool=ToolInfo(tool_name="search")),
            ToolCallErrorEvent(event=AgentRunEvent.TOOL_CALL_ERROR, tool=ToolInfo(tool_name="search")),
            RunContentEvent(event=AgentRunEvent.RUN_CONTENT, content="hi"),
            TextMessageCompletedEvent(event=AgentRunEvent.TEXT_MESSAGE_COMPLETED, message_id=""),
            ReasoningStartedEvent(event=AgentRunEvent.REASONING_STARTED),
            ReasoningContentDeltaEvent(event=AgentRunEvent.REASONING_CONTENT_DELTA, delta="x"),
            ReasoningCompletedEvent(event=AgentRunEvent.REASONING_COMPLETED),
            SubagentStartedEvent(event=AgentRunEvent.SUBAGENT_STARTED, name="a"),
            SubagentFinishedEvent(event=AgentRunEvent.SUBAGENT_FINISHED),
            SubagentErrorEvent(event=AgentRunEvent.SUBAGENT_ERROR, message="boom"),
        ],
        ids=lambda e: e.event,
    )
    async def test_event_without_its_id_is_dropped_and_logged(
        self, event: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No AG-UI event leaves with an empty or invented id (1.x fails the run on it)."""
        log = MagicMock()
        monkeypatch.setattr("digitalkin.mixins.agui_mixin.logger", log)
        ctx = _make_context()
        await _Host().send_message(ctx, event)

        ctx.callbacks.send_message.assert_not_awaited()
        (message, kind, dumped), _ = log.warning.call_args
        assert message.startswith("[VALIDATE AGUI-EMPTY-ID]")
        assert kind == event.event
        assert dumped == event.model_dump_json(exclude_none=True)

    async def test_custom_null_value_is_kept_on_the_wire(self) -> None:
        """``CUSTOM.value`` is required; a null value must not be stripped by ``exclude_none``."""
        ctx = _make_context()
        await _Host().send_message(ctx, CustomEvent(event=AgentRunEvent.CUSTOM, name="ping", value=None))

        wire = ctx.callbacks.send_message.await_args_list[0].args[0].model_dump(mode="json")["root"]["event"]
        assert wire == {"type": "CUSTOM", "name": "ping", "value": None}


class TestSubagentOutcome:
    @pytest.mark.parametrize(("outcome", "wire", "status"), [("suspended", {"type": "suspended"}, "suspended"), (None, None, "completed")])
    async def test_outcome_reaches_wire_and_state(self, outcome: Any, wire: Any, status: str) -> None:
        from digitalkin.mixins.agui_mixin import AgUiRunState

        ctx = _make_context()
        ctx.agui_run = AgUiRunState()
        host = _Host()
        await host.send_message(ctx, SubagentStartedEvent(event=AgentRunEvent.SUBAGENT_STARTED, name="a", subagent_run_id="m1"))
        await host.send_message(
            ctx, SubagentFinishedEvent(event=AgentRunEvent.SUBAGENT_FINISHED, subagent_run_id="m1", outcome=outcome)
        )

        finished = next(
            c.args[0].model_dump(mode="json")["root"]["event"]
            for c in ctx.callbacks.send_message.await_args_list
            if isinstance(c.args[0].root, AgUiSubagentFinishedOutput)
        )
        assert finished.get("outcome") == wire
        assert ctx.agui_run.state["subagents"]["m1"]["status"] == status


class TestSourceCitationRouting:
    async def test_subagent_citation_lands_in_its_own_list(self) -> None:
        from digitalkin.mixins.agui_mixin import AgUiRunState
        from digitalkin.models.events import SourceCitation, SourceCitationEvent

        ctx = _make_context()
        ctx.agui_run = AgUiRunState()
        await _Host().send_message(ctx, SourceCitationEvent(value=SourceCitation(url="https://a.io/"), subagent_run_id="m1"))

        assert ctx.agui_run.sources == {"m1": {"https://a.io/": {"url": "https://a.io/"}}}

    async def test_invalid_citation_value_is_dropped(self) -> None:
        from digitalkin.mixins.agui_mixin import AgUiRunState

        ctx = _make_context()
        ctx.agui_run = AgUiRunState()
        await _Host().send_message(ctx, CustomEvent(name="source_citation", value={"url": "javascript:alert(1)"}))

        ctx.callbacks.send_message.assert_not_awaited()
        assert ctx.agui_run.sources == {}
