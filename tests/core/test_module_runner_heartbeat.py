"""A running-but-silent task writes liveness frames so the gateway idle guard does not kill it.

Paired with ``tests/integration/redis/test_heartbeat_real.py``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.protobuf import json_format, struct_pb2

from digitalkin.core.task_manager.module_runner import ModuleRunner
from digitalkin.mixins.agui_mixin import AgUiRunState
from digitalkin.models.settings.module import get_module_settings
from tests.core.test_module_runner_m4 import _Out, _RecordingRedis

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = [pytest.mark.unit, pytest.mark.timeout(10)]


@pytest.fixture(autouse=True)
def _fast_heartbeat(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("DIGITALKIN_MODULE_HEARTBEAT_INTERVAL_S", "0.05")
    get_module_settings.cache_clear()
    yield
    get_module_settings.cache_clear()


def _frames(redis: _RecordingRedis) -> list[dict[str, Any]]:
    return [
        json_format.MessageToDict(struct_pb2.Struct.FromString(fields["pb"]))
        for _, fields, _ in redis.xadds
        if "pb" in fields
    ]


async def _run(redis: _RecordingRedis, module: Any, body: Any) -> asyncio.Task:
    """Drive ``ModuleRunner.run`` with ``body(emit)`` as the module task; return that task."""
    servicer = MagicMock()
    servicer.resolve_setup = AsyncMock(return_value=MagicMock(content={}, setup_id="setups:s1", id="sv1"))
    servicer.module_class.create_setup_model = AsyncMock(return_value=MagicMock())
    servicer.get_tool_cache = MagicMock(return_value=MagicMock())
    servicer.module_class.create_input_model = MagicMock(return_value=MagicMock())
    servicer.job_manager.tasks = {}

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # ruff: ignore[unused-async]
        return module, kwargs["job_id"], kwargs["callback"]

    async def _run_instance(*, callback: Any, job_id: str, **_: Any) -> None:  # ruff: ignore[unused-async]
        module.context.callbacks.send_message = callback
        servicer.job_manager.tasks[job_id] = asyncio.create_task(body(callback))

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = _run_instance

    async def _on_fatal(code: str, message: str) -> None:  # ruff: ignore[unused-async]
        pytest.fail(f"unexpected on_fatal {code}: {message}")

    with patch("digitalkin.core.task_manager.module_runner.TaskProfiler"):
        await ModuleRunner(redis_client=redis, servicer=servicer).run(  # type: ignore[arg-type]
            struct_pb2.Struct(), task_id="t-hb", setup_id="setups:s1", mission_id="m1", on_fatal=_on_fatal
        )
    return servicer.job_manager.tasks["t-hb"]


def _module(agui_run: AgUiRunState | None = None) -> MagicMock:
    module = MagicMock()
    module.context.agui_run = agui_run
    return module


async def test_silent_task_beats_until_it_ends() -> None:
    redis = _RecordingRedis()

    async def _body(emit: Any) -> None:
        await asyncio.sleep(0.3)
        await emit(_Out({"root": {"protocol": "stream.end"}}))

    task = await _run(redis, _module(), _body)
    await task
    beats = sum(f["root"]["protocol"] == "stream.heartbeat" for f in _frames(redis))
    await asyncio.sleep(0.15)

    assert beats >= 2
    assert sum(f["root"]["protocol"] == "stream.heartbeat" for f in _frames(redis)) == beats


async def test_busy_task_never_beats() -> None:
    redis = _RecordingRedis()

    async def _body(emit: Any) -> None:
        for i in range(15):
            await emit(_Out({"root": {"protocol": "data", "value": i}}))
            await asyncio.sleep(0.02)
        await emit(_Out({"root": {"protocol": "stream.end"}}))

    await (await _run(redis, _module(), _body))

    assert all(f["root"]["protocol"] == "data" for f in _frames(redis))


async def test_open_agui_run_beats_as_state_delta() -> None:
    redis = _RecordingRedis()
    run = AgUiRunState()

    async def _body(emit: Any) -> None:
        await asyncio.sleep(0.2)
        await emit(_Out({"root": {"protocol": "stream.end"}}))

    await (await _run(redis, _module(run), _body))
    frames = _frames(redis)

    assert frames
    assert {f["root"]["protocol"] for f in frames} == {"agui_state_delta"}
    assert frames[-1]["root"]["event"]["delta"][0]["path"] == "/run/heartbeatAt"
    assert run.state["run"]["heartbeatAt"] is not None


async def test_crashed_task_stops_beating() -> None:
    redis = _RecordingRedis()

    async def _body(emit: Any) -> None:
        await asyncio.sleep(0.01)
        msg = "boom"
        raise RuntimeError(msg)

    task = await _run(redis, _module(), _body)
    with pytest.raises(RuntimeError):
        await task
    await asyncio.sleep(0.15)

    assert _frames(redis) == []
