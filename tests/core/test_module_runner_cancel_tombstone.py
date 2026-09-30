"""ModuleRunner honours the ``cancel:{task_id}`` tombstone around task registration.

A cancel published before the task registers with ``SharedRedisListener`` is lost
on pub/sub; the gateway also leaves a tombstone the runner checks before
``preload_instance``, after it, and right after ``run_instance``.
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from google.protobuf import struct_pb2

from digitalkin.core.task_manager.module_runner import ModuleRunner
from digitalkin.models.settings.gateway import get_gateway_settings
from tests.core.test_module_runner_m4 import _RecordingRedis


def _servicer() -> MagicMock:
    setup_version = MagicMock(content={}, setup_id="setups:s1", id="setup_versions:v1")
    servicer = MagicMock()
    servicer.resolve_setup = AsyncMock(return_value=setup_version)
    servicer.module_class.create_setup_model = AsyncMock(return_value=MagicMock())
    servicer.get_tool_cache = MagicMock(return_value=MagicMock())
    servicer.module_class.create_input_model = MagicMock(return_value=MagicMock())
    servicer.job_manager.tasks = {}
    servicer.job_manager.tasks_sessions = {}
    return servicer


async def _run(runner: ModuleRunner, task_id: str) -> list[tuple[str, str]]:
    fatals: list[tuple[str, str]] = []

    async def _on_fatal(code: str, message: str) -> None:  # noqa: RUF029
        fatals.append((code, message))

    with patch("digitalkin.core.task_manager.module_runner.TaskProfiler"):
        await runner.run(
            struct_pb2.Struct(),
            task_id=task_id,
            setup_id="setups:s1",
            mission_id="missions:m1",
            on_fatal=_on_fatal,
        )
    return fatals


def _protocols(redis: _RecordingRedis) -> list[str]:
    out: list[str] = []
    for _, fields, _ in redis.xadds:
        if fields.get("eos") == b"true":
            out.append("<eos>")
            continue
        s = struct_pb2.Struct()
        s.ParseFromString(fields["pb"])
        out.append(s.fields["root"].struct_value.fields["protocol"].string_value)
    return out


async def test_tombstone_before_preload_writes_cancelled_then_eos() -> None:
    get_gateway_settings.cache_clear()
    redis = _RecordingRedis(cancel_from_check=1)
    servicer = _servicer()
    servicer.job_manager.preload_instance = AsyncMock()
    servicer.job_manager.run_instance = AsyncMock()

    fatals = await _run(ModuleRunner(redis_client=redis, servicer=servicer), "t-pre")  # type: ignore[arg-type]

    assert fatals == []
    assert redis.exists_calls == [("cancel:t-pre",)]
    servicer.job_manager.preload_instance.assert_not_awaited()
    servicer.job_manager.run_instance.assert_not_awaited()
    assert _protocols(redis) == ["stream.cancelled", "<eos>"]
    s = struct_pb2.Struct()
    s.ParseFromString(redis.xadds[0][1]["pb"])
    assert s.fields["root"].struct_value.fields["reason"].string_value == "signal_service_cancel"


async def test_tombstone_after_preload_stops_then_cleans_instance() -> None:
    get_gateway_settings.cache_clear()
    redis = _RecordingRedis(cancel_from_check=2)
    servicer = _servicer()
    order: list[str] = []
    module = MagicMock()
    module.stop = AsyncMock(side_effect=lambda *_: order.append("stop"))
    module.context.cleanup = AsyncMock(side_effect=lambda: order.append("context"))

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: RUF029
        return module, kwargs["job_id"], kwargs["callback"]

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = AsyncMock()

    await _run(ModuleRunner(redis_client=redis, servicer=servicer), "t-mid")  # type: ignore[arg-type]

    servicer.job_manager.run_instance.assert_not_awaited()
    module.stop.assert_awaited_once_with("signal_service_cancel")
    assert order == ["stop", "context"]


async def test_tombstone_after_run_instance_cancels_registered_task() -> None:
    get_gateway_settings.cache_clear()
    redis = _RecordingRedis(cancel_from_check=3)
    servicer = _servicer()
    session = SimpleNamespace(pending_signal_action="")
    task = asyncio.create_task(asyncio.sleep(10))

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: RUF029
        return MagicMock(), kwargs["job_id"], kwargs["callback"]

    async def _run_instance(*, job_id: str, **_: Any) -> None:  # noqa: RUF029
        servicer.job_manager.tasks[job_id] = task
        servicer.job_manager.tasks_sessions[job_id] = session

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = _run_instance

    try:
        await _run(ModuleRunner(redis_client=redis, servicer=servicer), "t-post")  # type: ignore[arg-type]
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert task.cancelled()
        assert session.pending_signal_action == "cancel"
    finally:
        task.cancel()


async def test_tombstone_after_run_instance_skips_task_already_cancelling() -> None:
    """A task the listener already cancelled is not cancelled twice."""
    get_gateway_settings.cache_clear()
    redis = _RecordingRedis(cancel_from_check=3)
    servicer = _servicer()
    session = SimpleNamespace(pending_signal_action="cancel")
    task = asyncio.create_task(asyncio.sleep(10))

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: RUF029
        return MagicMock(), kwargs["job_id"], kwargs["callback"]

    async def _run_instance(*, job_id: str, **_: Any) -> None:  # noqa: RUF029
        servicer.job_manager.tasks[job_id] = task
        servicer.job_manager.tasks_sessions[job_id] = session

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = _run_instance

    try:
        await _run(ModuleRunner(redis_client=redis, servicer=servicer), "t-dup")  # type: ignore[arg-type]
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_no_tombstone_runs_normally() -> None:
    get_gateway_settings.cache_clear()
    redis = _RecordingRedis()
    servicer = _servicer()
    module = MagicMock()
    module.stop = AsyncMock()

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: RUF029
        return module, kwargs["job_id"], kwargs["callback"]

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = AsyncMock()

    await _run(ModuleRunner(redis_client=redis, servicer=servicer), "t-ok")  # type: ignore[arg-type]

    assert len(redis.exists_calls) == 3
    servicer.job_manager.run_instance.assert_awaited_once()
    module.stop.assert_not_awaited()
    assert redis.xadds == []
