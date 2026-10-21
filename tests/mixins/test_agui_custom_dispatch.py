"""M15 regression: ``AgentRunEvent.CUSTOM`` must dispatch to ``_handle_custom``.

The ``__init_subclass__`` dispatch table previously omitted ``CUSTOM`` (the default
event), so ``send_message(CustomEvent(...))`` was silently dropped.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.mixins.agui_mixin import AgUiMixin, AgUiRunState
from digitalkin.models.events import AgentRunEvent, CustomEvent, SourceCitation, SourceCitationEvent
from digitalkin.models.module.ag_ui import AgUiCustomEventOutput


class _Mixin(AgUiMixin):
    """Concrete subclass so ``__init_subclass__`` builds the dispatch table."""


def _ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.callbacks = MagicMock()
    ctx.callbacks.send_message = AsyncMock()
    ctx.callbacks.logger = MagicMock()
    ctx.session = MagicMock()
    ctx.session.current_ids = MagicMock(return_value={})
    return ctx


def test_custom_is_in_dispatch_table() -> None:
    assert _Mixin._agui_dispatch.get(AgentRunEvent.CUSTOM) is AgUiMixin._handle_custom  # noqa: SLF001


@pytest.mark.asyncio
async def test_send_message_dispatches_custom_event() -> None:
    mixin = _Mixin()
    ctx = _ctx()

    await mixin.send_message(ctx, CustomEvent(name="my_event", value={"k": "v"}))

    ctx.callbacks.send_message.assert_awaited_once()
    output = ctx.callbacks.send_message.await_args_list[-1].args[0]
    assert isinstance(output.root, AgUiCustomEventOutput)
    assert output.root.event.name == "my_event"


@pytest.mark.asyncio
async def test_source_citation_becomes_sources_activity() -> None:
    """``source_citation`` no longer travels as CUSTOM: it feeds the run's ``sources`` activity."""
    mixin = _Mixin()
    ctx = _ctx()
    ctx.agui_run = AgUiRunState()

    await mixin.send_message(
        ctx, SourceCitationEvent(value=SourceCitation(url="https://docs.digitalkin.ai/pricing", title="Pricing"))
    )

    frame = ctx.callbacks.send_message.await_args_list[-1].args[0].model_dump(mode="json")
    assert frame["root"]["protocol"] == "agui_activity_snapshot"
    assert frame["root"]["event"] == {
        "type": "ACTIVITY_SNAPSHOT",
        "messageId": f"sources-{ctx.agui_run.sources_id}",
        "activityType": "sources",
        "content": {"sources": [{"url": "https://docs.digitalkin.ai/pricing", "title": "Pricing"}]},
        "replace": True,
    }


@pytest.mark.asyncio
async def test_source_citation_without_open_run_is_dropped() -> None:
    mixin = _Mixin()
    ctx = _ctx()
    ctx.agui_run = None

    await mixin.send_message(ctx, SourceCitationEvent(value=SourceCitation(url="https://x.io/a")))

    ctx.callbacks.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_custom_event_carries_subagent_attribution() -> None:
    mixin = _Mixin()
    ctx = _ctx()

    await mixin.send_message(
        ctx,
        CustomEvent(
            name="live_view", value={"url": "https://x.io/a"}, subagent_run_id="member-r1", metadata={"name": "Alice"}
        ),
    )

    event = ctx.callbacks.send_message.await_args_list[-1].args[0].model_dump(mode="json")["root"]["event"]
    assert event["subagentRunId"] == "member-r1"
    assert event["metadata"] == {"digitalkin": {"name": "Alice"}}
