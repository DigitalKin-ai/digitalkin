"""A crashed or cancelled module closes its AG-UI run and still ends its stream."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from tests.modules.test_base_module_lifecycle import (
    _instantiate,
    _LcInputModel,
    _LcInputTrigger,
    _LcSetupModel,
    _make_module_cls,
)

from digitalkin.mixins.agui_mixin import AgUiRunState
from digitalkin.models.module.ag_ui import AgUiRunErrorOutput, AgUiStateDeltaOutput
from digitalkin.models.module.module import ModuleCodeModel, ModuleStatus

pytestmark = pytest.mark.regression


def _protocols(send: AsyncMock) -> list[str]:
    return [
        type(sent).__name__ if isinstance(sent, ModuleCodeModel) else sent.root.protocol
        for sent in (call.args[0] for call in send.await_args_list)
    ]


async def test_crash_emits_run_error_then_stream_end_and_stays_failed() -> None:
    module = _instantiate(_make_module_cls())
    send = module.context.callbacks.send_message = AsyncMock()
    module.context.agui_run = AgUiRunState()

    with patch.object(module, "run", new_callable=AsyncMock, side_effect=RuntimeError("boom")):
        await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())
    await module.stop()
    await module.stop()

    assert _protocols(send) == ["ModuleCodeModel", "agui_state_delta", "agui_run_error", "stream.end"]
    run_error = send.await_args_list[2].args[0].root
    assert isinstance(run_error, AgUiRunErrorOutput)
    assert run_error.event.code == "module_error"
    assert run_error.event.message == "RuntimeError: boom"
    assert module.status == ModuleStatus.FAILED


async def test_crash_without_agui_run_still_ends_stream() -> None:
    module = _instantiate(_make_module_cls())
    send = module.context.callbacks.send_message = AsyncMock()

    with patch.object(module, "run", new_callable=AsyncMock, side_effect=RuntimeError("boom")):
        await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())
    await module.stop()

    assert _protocols(send) == ["ModuleCodeModel", "stream.end"]


async def test_cancel_emits_cancelled_run_error_before_stream_cancelled() -> None:
    module = _instantiate(_make_module_cls())
    send = module.context.callbacks.send_message = AsyncMock()
    module.context.agui_run = AgUiRunState()

    with (
        patch.object(module, "run", new_callable=AsyncMock, side_effect=asyncio.CancelledError),
        pytest.raises(asyncio.CancelledError),
    ):
        await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())
    await module.stop()

    assert _protocols(send) == ["agui_state_delta", "agui_run_error", "stream.cancelled", "stream.end"]
    assert isinstance(send.await_args_list[0].args[0].root, AgUiStateDeltaOutput)
    assert send.await_args_list[1].args[0].root.event.code == "cancelled"
    assert module.context.agui_run.state["run"]["status"] == "cancelled"
    assert module.status == ModuleStatus.STOPPED


async def test_finished_run_is_not_reopened_by_a_late_cancel() -> None:
    module = _instantiate(_make_module_cls())
    send = module.context.callbacks.send_message = AsyncMock()
    module.context.agui_run = AgUiRunState()
    module.context.agui_run.open = False

    await module.stop(cancel_reason="user_stop")

    assert _protocols(send) == ["stream.cancelled", "stream.end"]
