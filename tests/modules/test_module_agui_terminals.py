"""A crashed or cancelled module closes its AG-UI run and still ends its stream."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from tests.modules.test_base_module_lifecycle import (
    _instantiate,
    _LcInputModel,
    _LcInputTrigger,
    _LcSetupModel,
    _make_module_cls,
)

from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.mixins.agui_mixin import AgUiRunState
from digitalkin.models.core.task_monitor import CancellationReason
from digitalkin.models.events import RunErrorCode
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
    assert run_error.event.code == RunErrorCode.MODULE_FAILED
    assert run_error.event.model_dump(mode="json")["code"] == "MODULE_FAILED"
    assert run_error.event.message == "module execution failed"
    assert module.status == ModuleStatus.FAILED


async def test_crash_without_agui_run_still_ends_stream() -> None:
    module = _instantiate(_make_module_cls())
    send = module.context.callbacks.send_message = AsyncMock()

    with patch.object(module, "run", new_callable=AsyncMock, side_effect=RuntimeError("boom")):
        await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())
    await module.stop()

    assert _protocols(send) == ["ModuleCodeModel", "stream.end"]


async def _cancelled(module: object, cause: CancellationReason | None) -> AsyncMock:
    send = module.context.callbacks.send_message = AsyncMock()
    module.context.agui_run = AgUiRunState("t1", "r1")
    if cause is not None:
        session = SimpleNamespace(cancel_cause=lambda: cause)
        module.task_session = session
    with (
        patch.object(module, "run", new_callable=AsyncMock, side_effect=asyncio.CancelledError),
        pytest.raises(asyncio.CancelledError),
    ):
        await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())
    await module.stop()
    return send


@pytest.mark.parametrize("cause", [CancellationReason.SIGNAL_SERVICE_CANCEL, CancellationReason.SIGNAL_SERVICE_STOP])
async def test_user_cancel_finishes_the_run_cancelled_before_stream_cancelled(cause: CancellationReason) -> None:
    module = _instantiate(_make_module_cls())

    send = await _cancelled(module, cause)

    assert _protocols(send) == ["agui_state_delta", "agui_run_finished", "stream.cancelled", "stream.end"]
    assert isinstance(send.await_args_list[0].args[0].root, AgUiStateDeltaOutput)
    assert send.await_args_list[1].args[0].root.event.outcome.type == "cancelled"
    assert send.await_args_list[2].args[0].root.reason == cause.value
    assert module.context.agui_run.state["run"]["status"] == "cancelled"
    assert module.status == ModuleStatus.STOPPED


@pytest.mark.parametrize(
    "cause",
    [
        CancellationReason.SHUTDOWN,
        CancellationReason.TIMEOUT,
        CancellationReason.GRPC_SERVICE_ERROR,
        CancellationReason.UNKNOWN,
        None,
    ],
)
async def test_platform_cancel_ends_on_retryable_run_interrupted(cause: CancellationReason | None) -> None:
    module = _instantiate(_make_module_cls())

    send = await _cancelled(module, cause)

    reason = (cause or CancellationReason.UNKNOWN).value
    assert _protocols(send) == ["agui_state_delta", "agui_run_error", "stream.cancelled", "stream.end"]
    run_error = send.await_args_list[1].args[0].root
    assert isinstance(run_error, AgUiRunErrorOutput)
    assert run_error.event.code == RunErrorCode.RUN_INTERRUPTED
    assert run_error.event.message == f"run interrupted ({reason})"
    assert run_error.event.metadata == {"retryable": True}
    assert send.await_args_list[2].args[0].root.reason == reason
    assert module.status == ModuleStatus.STOPPED


async def test_task_session_gives_the_module_its_cancel_cause() -> None:
    module = _instantiate(_make_module_cls())
    session = TaskSession("job", "missions:m", module)
    session.pending_signal_action = "stop"

    send = await _cancelled(module, None)

    assert module.task_session is session
    assert send.await_args_list[1].args[0].root.event.outcome.type == "cancelled"
    assert send.await_args_list[2].args[0].root.reason == CancellationReason.SIGNAL_SERVICE_STOP.value


async def test_finished_run_is_not_reopened_by_a_late_cancel() -> None:
    module = _instantiate(_make_module_cls())
    send = module.context.callbacks.send_message = AsyncMock()
    module.context.agui_run = AgUiRunState()
    module.context.agui_run.open = False

    await module.stop(cancel_reason="user_stop")

    assert _protocols(send) == ["stream.cancelled", "stream.end"]


async def test_context_cleanup_releases_a_run_left_open() -> None:
    module = _instantiate(_make_module_cls())
    run = AgUiRunState("t1", "r1", "task-left-open")
    module.context.agui_run = run

    await module.context.cleanup()

    assert run.open is False
    assert "task-left-open" not in AgUiRunState._open
