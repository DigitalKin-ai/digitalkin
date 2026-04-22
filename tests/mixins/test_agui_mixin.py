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
)
from digitalkin.models.module.ag_ui import AgUiRunFinishedOutput, AgUiRunStartedOutput

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
