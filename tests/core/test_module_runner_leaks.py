"""ModuleRunner leak and idem-TTL behavior.

A preloaded instance whose task never got created is released (M1), and EOS shortens
``idem:{task_id}`` to the post-EOS stream TTL (R3).
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.protobuf import struct_pb2

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.module_runner import ModuleRunner
from digitalkin.models.module.base_types import DataModel
from digitalkin.models.module.module import ModuleStatus
from digitalkin.models.module.utility import EndOfStreamOutput
from digitalkin.models.settings.gateway import get_gateway_settings
from tests.core.test_module_runner_cancel_tombstone import _servicer
from tests.core.test_module_runner_m4 import _RecordingRedis


def _module(order: list[str]) -> MagicMock:
    module = MagicMock()
    module.status = ModuleStatus.CREATED
    module.stop = AsyncMock(side_effect=lambda *_: order.append("stop"))
    module.context.cleanup = AsyncMock(side_effect=lambda: order.append("context"))
    return module


async def _run(runner: ModuleRunner, order: list[str]) -> None:
    async def _on_fatal(code: str, message: str) -> None:  # noqa: RUF029
        order.append(f"fatal:{code}:{message}")

    with patch("digitalkin.core.task_manager.module_runner.TaskProfiler"):
        await runner.run(
            struct_pb2.Struct(),
            task_id="t-leak",
            setup_id="setups:s1",
            mission_id="missions:m1",
            on_fatal=_on_fatal,
        )


@pytest.mark.parametrize(
    "exc",
    [RuntimeError("Maximum concurrent tasks (1) reached"), ValueError("Task t-leak already exists")],
)
async def test_run_instance_failure_reports_then_stops_and_cleans(exc: Exception) -> None:
    get_gateway_settings.cache_clear()
    servicer = _servicer()
    order: list[str] = []
    module = _module(order)

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: RUF029
        return module, kwargs["job_id"], kwargs["callback"]

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = AsyncMock(side_effect=exc)

    await _run(ModuleRunner(redis_client=_RecordingRedis(), servicer=servicer), order)  # type: ignore[arg-type]

    assert order[0].startswith("fatal:MODULE_RUNTIME_ERROR:")
    assert order[1:] == ["stop", "context"]
    module.stop.assert_awaited_once_with(None)


async def test_registered_then_failed_task_is_not_cleaned_twice() -> None:
    """create_task already ran session.cleanup (module STOPPED): the runner must not repeat it."""
    get_gateway_settings.cache_clear()
    servicer = _servicer()
    order: list[str] = []
    module = _module(order)
    module.status = ModuleStatus.STOPPED

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: RUF029
        return module, kwargs["job_id"], kwargs["callback"]

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = AsyncMock(side_effect=RuntimeError("boom"))

    await _run(ModuleRunner(redis_client=_RecordingRedis(), servicer=servicer), order)  # type: ignore[arg-type]

    module.stop.assert_not_awaited()
    module.context.cleanup.assert_not_awaited()


async def test_created_task_is_left_running() -> None:
    get_gateway_settings.cache_clear()
    servicer = _servicer()
    order: list[str] = []
    module = _module(order)

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: RUF029
        return module, kwargs["job_id"], kwargs["callback"]

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = AsyncMock()

    await _run(ModuleRunner(redis_client=_RecordingRedis(), servicer=servicer), order)  # type: ignore[arg-type]

    assert order == []


async def test_cancel_before_task_created_writes_fatal_cleans_and_reraises() -> None:
    get_gateway_settings.cache_clear()
    servicer = _servicer()
    order: list[str] = []
    module = _module(order)
    entered = asyncio.Event()

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: RUF029
        return module, kwargs["job_id"], kwargs["callback"]

    async def _run_instance(**_: Any) -> None:
        entered.set()
        await asyncio.sleep(10)

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = _run_instance

    task = asyncio.create_task(_run(ModuleRunner(redis_client=_RecordingRedis(), servicer=servicer), order))  # type: ignore[arg-type]
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert order[0] == "fatal:MODULE_RUNTIME_ERROR:module runner cancelled before the task started"
    assert order[1:] == ["stop", "context"]


async def test_eos_shortens_idem_ttl() -> None:
    get_gateway_settings.cache_clear()
    redis = _RecordingRedis()
    servicer = _servicer()

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:
        await kwargs["callback"](DataModel[EndOfStreamOutput](root=EndOfStreamOutput()))
        return MagicMock(status=ModuleStatus.RUNNING), kwargs["job_id"], kwargs["callback"]

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = AsyncMock()

    await _run(ModuleRunner(redis_client=redis, servicer=servicer), [])  # type: ignore[arg-type]

    ttl = get_gateway_settings().stream.redis_stream_ttl
    assert ("task:t-leak:stream", ttl) in redis.expires
    assert ("idem:t-leak", ttl) in redis.expires


async def test_preload_prepare_failure_releases_instance() -> None:
    mgr = object.__new__(SingleJobManager)
    mgr._redis_task_manager = MagicMock()
    mgr.module_class = MagicMock()
    order: list[str] = []
    module = MagicMock()
    module.prepare = AsyncMock(side_effect=RuntimeError("initialize failed"))
    module.cleanup = AsyncMock(side_effect=lambda: order.append("module"))
    module.context.cleanup = AsyncMock(side_effect=lambda: order.append("context"))
    module.stop = AsyncMock()

    with (
        patch(
            "digitalkin.core.job_manager.single_job_manager.ModuleFactory.create_module_instance", return_value=module
        ),
        pytest.raises(RuntimeError, match="initialize failed"),
    ):
        await mgr.preload_instance(MagicMock(), "missions:m1", "setups:s1", "setup_versions:v1", callback=AsyncMock())

    assert order == ["module", "context"]
    module.stop.assert_not_awaited()
