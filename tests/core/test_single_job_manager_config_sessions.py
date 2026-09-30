"""Config-setup sessions hold no task slot, so they never release one (C12)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager


def _manager() -> SingleJobManager:
    mgr = object.__new__(SingleJobManager)
    mgr._task_manager = LocalTaskManager(default_timeout=5.0)
    mgr._config_sessions = {}
    mgr.module_class = MagicMock()
    return mgr


def _module(start_config_setup: Any) -> Mock:
    module = Mock()
    module.name = "cfg"
    module.start_config_setup = start_config_setup
    module.stop = AsyncMock()
    module.context.task_manager = None
    module.context.cleanup = AsyncMock()
    return module


def _slots(mgr: SingleJobManager) -> tuple[int, int, int]:
    tm = mgr._task_manager
    return tm._task_slot._value, tm._system_gate._value, tm._active_slots  # type: ignore[attr-defined]


async def test_config_session_round_trip_leaves_slots_untouched() -> None:
    mgr = _manager()
    before = _slots(mgr)

    async def _start(data: Any, callback: Any) -> None:
        await callback(MagicMock(model_dump=Mock(return_value={"updated": True})))

    module = _module(_start)
    with patch(
        "digitalkin.core.job_manager.single_job_manager.ModuleFactory.create_module_instance", return_value=module
    ):
        job_id = await mgr.create_config_setup_instance_job(MagicMock(), "missions:m", "setups:s", "sv:1")

    assert job_id in mgr._config_sessions
    assert job_id not in mgr.tasks_sessions

    # A stray cancel on the config job id must not release a slot the session never took.
    await mgr._task_manager.cancel_task(job_id, "missions:m")
    assert _slots(mgr) == before

    result = await mgr.generate_config_setup_module_response(job_id)

    assert result == {"updated": True}
    assert mgr._config_sessions == {}
    assert _slots(mgr) == before
    module.stop.assert_awaited_once()
    module.context.cleanup.assert_awaited_once()


async def test_cancel_during_start_config_setup_drops_the_session() -> None:
    mgr = _manager()
    module = _module(AsyncMock(side_effect=asyncio.CancelledError))
    with (
        patch(
            "digitalkin.core.job_manager.single_job_manager.ModuleFactory.create_module_instance", return_value=module
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await mgr.create_config_setup_instance_job(MagicMock(), "missions:m", "setups:s", "sv:1")

    assert mgr._config_sessions == {}
    module.stop.assert_awaited_once()
    module.context.cleanup.assert_awaited_once()
