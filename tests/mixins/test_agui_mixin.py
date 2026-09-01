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
    RunCompletedEvent,
    RunStartedEvent,
    SubagentErrorEvent,
    SubagentFinishedEvent,
    SubagentStartedEvent,
    TextMessageStartedEvent,
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
    return ctx


def _emitted_event(ctx: MagicMock, expected_cls: type) -> Any:
    """Pull the last AG-UI event sent through ``send_message``."""
    assert ctx.callbacks.send_message.await_count >= 1, "send_message was never awaited"
    output = ctx.callbacks.send_message.await_args_list[-1].args[0]
    assert isinstance(output.root, expected_cls), f"expected {expected_cls.__name__}, got {type(output.root).__name__}"
    return output.root.event


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

        started = ctx.callbacks.send_message.await_args_list[0].args[0].root.event
        finished = ctx.callbacks.send_message.await_args_list[1].args[0].root.event
        assert started.run_id == finished.run_id == CLIENT_RUN_ID
        assert started.thread_id == finished.thread_id == CLIENT_THREAD_ID

    @pytest.mark.asyncio
    async def test_unprimed_ids_match_through_full_lifecycle(self) -> None:
        """No prime → start adopts agno ids → finish echoes them."""
        mixin = AgUiMixin()
        ctx = _make_context()

        await mixin._handle_run_started(ctx, _started(run_id=AGNO_RUN_ID, thread_id=AGNO_THREAD_ID))
        await mixin._handle_run_completed(ctx, _completed(run_id=AGNO_RUN_ID))

        started = ctx.callbacks.send_message.await_args_list[0].args[0].root.event
        finished = ctx.callbacks.send_message.await_args_list[1].args[0].root.event
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
