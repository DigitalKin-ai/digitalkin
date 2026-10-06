"""L1 integration against REAL Redis: liveness heartbeat and crash terminals.

Pairs ``tests/core/test_module_runner_heartbeat.py`` and ``tests/modules/test_module_agui_terminals.py``.
A silent-but-alive task must outlive the gateway read idle guard; a crashed module must close its
AG-UI run and write EOS at once instead of idling out.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.protobuf import json_format, struct_pb2

from digitalkin.core.task_manager.module_runner import ModuleRunner
from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.mixins.agui_mixin import AgUiRunState
from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.models.settings.module import get_module_settings
from tests.modules.test_base_module_lifecycle import (
    _instantiate,
    _LcInputModel,
    _LcInputTrigger,
    _LcSetupModel,
    _make_module_cls,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator

    from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


class _Out:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def model_dump(self, mode: str = "json") -> dict[str, Any]:
        return self._payload


@pytest.fixture(autouse=True)
def _fast_timers(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("DIGITALKIN_MODULE_HEARTBEAT_INTERVAL_S", "0.1")
    monkeypatch.setenv("DIGITALKIN_GATEWAY_STREAM_READ_IDLE_TIMEOUT_S", "0.5")
    monkeypatch.setenv("DIGITALKIN_GATEWAY_STREAM_STREAM_READ_BLOCK_MS", "20")
    get_module_settings.cache_clear()
    get_gateway_settings.cache_clear()
    yield
    get_module_settings.cache_clear()
    get_gateway_settings.cache_clear()


async def _run(
    redis_client: RedisClient, task_id: str, module: Any, body: Callable[[Any], Awaitable[None]]
) -> asyncio.Task:
    servicer = MagicMock()
    servicer.resolve_setup = AsyncMock(return_value=MagicMock(content={}, setup_id="setups:s1", id="sv1"))
    servicer.module_class.create_setup_model = AsyncMock(return_value=MagicMock())
    servicer.get_tool_cache = MagicMock(return_value=MagicMock())
    servicer.module_class.create_input_model = MagicMock(return_value=MagicMock())
    servicer.job_manager.tasks = {}

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # ruff: ignore[unused-async]
        return module, kwargs["job_id"], kwargs["callback"]

    async def _run_instance(*, callback: Any, job_id: str, **_: Any) -> None:  # ruff: ignore[unused-async]
        servicer.job_manager.tasks[job_id] = asyncio.create_task(body(callback))

    async def _on_fatal(code: str, message: str) -> None:  # ruff: ignore[unused-async]
        pytest.fail(f"unexpected on_fatal {code}: {message}")

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = _run_instance
    with patch("digitalkin.core.task_manager.module_runner.TaskProfiler"):
        await ModuleRunner(redis_client=redis_client, servicer=servicer).run(  # type: ignore[arg-type]
            struct_pb2.Struct(), task_id=task_id, setup_id="setups:s1", mission_id="m1", on_fatal=_on_fatal
        )
    return servicer.job_manager.tasks[task_id]


def _protocol(msg: Any) -> str:
    return json_format.MessageToDict(msg.data)["root"]["protocol"]


async def test_silent_task_outlives_the_read_idle_guard(redis_client: RedisClient) -> None:
    module = MagicMock()
    module.context.agui_run = None

    async def _body(emit: Any) -> None:
        await emit(_Out({"root": {"protocol": "data", "value": 1}}))
        await asyncio.sleep(1.2)  # > 2x the 0.5s read idle timeout
        await emit(_Out({"root": {"protocol": "data", "value": 2}}))
        await emit(_Out({"root": {"protocol": "stream.end"}}))

    task = await _run(redis_client, "hb1", module, _body)
    protocols = [_protocol(msg) async for msg in GatewayServicer(redis_client=redis_client)._consume_guarded("hb1", 0)]
    await task

    assert "stream.error" not in protocols
    assert protocols.count("stream.heartbeat") >= 3
    assert [p for p in protocols if p != "stream.heartbeat"] == ["data", "data", "stream.end"]


async def test_crash_writes_run_error_then_eos(redis_client: RedisClient) -> None:
    module = _instantiate(_make_module_cls())
    module.context.agui_run = AgUiRunState()

    async def _body(emit: Any) -> None:
        module.context.callbacks.send_message = emit
        with patch.object(module, "run", new_callable=AsyncMock, side_effect=RuntimeError("boom")):
            await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())
        await module.stop()

    await (await _run(redis_client, "hb2", module, _body))
    entries = await redis_client._client.xrange("task:hb2:stream")

    data = [
        json_format.MessageToDict(struct_pb2.Struct.FromString(fields[b"pb"]))
        for _, fields in entries
        if b"pb" in fields
    ]
    assert [d.get("root", {}).get("protocol", d.get("code")) for d in data] == [
        "Error",
        "agui_state_delta",
        "agui_run_error",
    ]
    assert data[-1]["root"]["event"]["code"] == "module_error"
    assert entries[-1][1] == {b"eos": b"true"}
