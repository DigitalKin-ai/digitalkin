"""M15 regression: ``AgentRunEvent.CUSTOM`` must dispatch to ``_handle_custom``.

The ``__init_subclass__`` dispatch table previously omitted ``CUSTOM`` (the default
event), so ``send_message(CustomEvent(...))`` was silently dropped.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.mixins.agui_mixin import AgUiMixin
from digitalkin.models.events import AgentRunEvent, CustomEvent
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
