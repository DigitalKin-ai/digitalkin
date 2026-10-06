"""L1 integration: cancel paths end-to-end against real Redis.

Pairs the ``_FakePubSub`` unit tests of the Phase-1 stop/cancel hardening:
cancel before register, cancel mid-run, SendSignal without a local session,
repeated cancels and the bounded ``_last_seen`` map.

Run with: ``docker compose up -d tests-redis`` then
``uv run pytest tests/integration/redis -m integration``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_dto_pb2, gateway_messages_pb2
from google.protobuf import struct_pb2

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.module_runner import ModuleRunner
from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.services.task_manager.redis_task_manager import RedisTaskManager
from tests.modules.test_base_module_lifecycle import _instantiate, _make_module_cls

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


@pytest.fixture(autouse=True)
def _clear_listeners() -> Any:
    SharedRedisListener._instances.clear()
    yield
    SharedRedisListener._instances.clear()


def _job_manager(redis_client: RedisClient) -> SingleJobManager:
    mgr = object.__new__(SingleJobManager)
    mgr._task_manager = LocalTaskManager(default_timeout=5.0)
    mgr._redis_client = redis_client
    mgr._redis_task_manager = RedisTaskManager(redis_client)
    return mgr


def _servicer(job_manager: SingleJobManager) -> MagicMock:
    setup_version = MagicMock(content={}, setup_id="setups:s1", id="setup_versions:v1")
    servicer = MagicMock()
    servicer.resolve_setup = AsyncMock(return_value=setup_version)
    servicer.module_class.create_setup_model = AsyncMock(return_value=MagicMock())
    servicer.get_tool_cache = MagicMock(return_value=MagicMock())
    servicer.module_class.create_input_model = MagicMock(return_value=MagicMock())
    servicer.job_manager = job_manager
    return servicer


async def _stream_protocols(redis_client: RedisClient, task_id: str) -> list[tuple[str, str]]:
    entries = await redis_client._client.xrange(f"task:{task_id}:stream")
    out: list[tuple[str, str]] = []
    for _, fields in entries:
        if fields.get(b"eos") == b"true":
            out.append(("<eos>", ""))
            continue
        s = struct_pb2.Struct()
        s.ParseFromString(fields[b"pb"])
        root = s.fields["root"].struct_value.fields
        out.append((root["protocol"].string_value, root["reason"].string_value if "reason" in root else ""))
    return out


async def _send_cancel(gateway: GatewayServicer, task_id: str) -> Any:
    request = gateway_dto_pb2.SendSignalRequest(cancel=gateway_messages_pb2.CancelSignal(task_id=task_id))
    return await gateway.SendSignal(request, MagicMock())


async def _run_runner(runner: ModuleRunner, task_id: str) -> None:
    async def _on_fatal(code: str, message: str) -> None:  # noqa: RUF029
        pytest.fail(f"unexpected on_fatal {code}: {message}")

    with patch("digitalkin.core.task_manager.module_runner.TaskProfiler"):
        await runner.run(
            struct_pb2.Struct(),
            task_id=task_id,
            setup_id="setups:s1",
            mission_id="missions:m1",
            on_fatal=_on_fatal,
        )


class TestCancelPathsReal:
    """SendSignal → Redis → listener/tombstone → stream.cancelled + stream.end."""

    async def test_cancel_before_register_emits_cancelled_then_end(self, redis_client: RedisClient) -> None:
        jm = _job_manager(redis_client)
        servicer = _servicer(jm)
        servicer.job_manager.preload_instance = AsyncMock()
        gateway = GatewayServicer(redis_client=redis_client)
        await redis_client.set("idem:rc_pre", "owner", ex=60)

        resp = await _send_cancel(gateway, "rc_pre")
        assert resp.success is True
        assert await redis_client._client.ttl("cancel:rc_pre") > 0

        await _run_runner(ModuleRunner(redis_client=redis_client, servicer=servicer), "rc_pre")

        servicer.job_manager.preload_instance.assert_not_awaited()
        assert await _stream_protocols(redis_client, "rc_pre") == [
            ("stream.cancelled", "signal_service_cancel"),
            ("<eos>", ""),
        ]
        await jm._redis_task_manager.close()

    async def test_cancel_mid_run_emits_cancelled_then_end(self, redis_client: RedisClient) -> None:
        jm = _job_manager(redis_client)
        listener = SharedRedisListener.singleton_or_none()
        assert listener is not None
        await listener.start()
        servicer = _servicer(jm)
        running = asyncio.Event()
        module = _instantiate(_make_module_cls())

        async def _run(*_: Any) -> None:
            running.set()
            await asyncio.sleep(30)

        async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: RUF029
            module.context.task_manager = jm._redis_task_manager
            module.context.callbacks.send_message = kwargs["callback"]
            module._prepared = True
            module.trigger_handlers = {}
            return module, kwargs["job_id"], kwargs["callback"]

        servicer.job_manager.preload_instance = _preload
        gateway = GatewayServicer(redis_client=redis_client)
        await redis_client.set("idem:rc_mid", "owner", ex=60)

        try:
            with patch.object(module, "run", side_effect=_run):
                await _run_runner(ModuleRunner(redis_client=redis_client, servicer=servicer), "rc_mid")
                await asyncio.wait_for(running.wait(), timeout=5)
                task = jm.tasks["rc_mid"]

                resp = await _send_cancel(gateway, "rc_mid")
                assert resp.success is True
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait_for(task, timeout=5)
                for _ in range(40):
                    if "rc_mid" not in jm.tasks_sessions:
                        break
                    await asyncio.sleep(0.05)

            assert await _stream_protocols(redis_client, "rc_mid") == [
                ("stream.cancelled", "cancelled"),
                ("<eos>", ""),
            ]
            assert "rc_mid" not in jm.tasks_sessions
            assert jm._task_manager._active_slots == 0
        finally:
            await jm._redis_task_manager.close()

    async def test_send_signal_without_local_session_publishes(self, redis_client: RedisClient) -> None:
        gateway = GatewayServicer(redis_client=redis_client)
        assert gateway._registry.get("rc_remote") is None
        await redis_client.set("idem:rc_remote", "other-replica", ex=60)
        pubsub = redis_client.pubsub()
        await pubsub.subscribe("signal_ch:rc_remote")
        await pubsub.get_message(timeout=0.5)
        try:
            resp = await _send_cancel(gateway, "rc_remote")
            assert resp.success is True
            msg = None
            for _ in range(20):
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if msg is not None:
                    break
            assert msg is not None
            assert json.loads(msg["data"])["action"] == "cancel"
            assert await redis_client.get("cancel:rc_remote") == b"1"

            unknown = await _send_cancel(gateway, "rc_nobody")
            assert unknown.success is False
        finally:
            await pubsub.aclose()


class TestListenerRepeatedCancelReal:
    """Real pub/sub: only the first cancel lands; foreign task ids are not remembered."""

    async def test_repeated_cancel_lands_once(self, redis_client: RedisClient) -> None:
        listener = SharedRedisListener(redis_client)
        session = MagicMock()
        session.pending_signal_action = ""
        session.last_signal_published_ns = 0
        session.cancelled = False
        task = MagicMock()
        task.done.return_value = False
        try:
            await listener.start()
            listener.register("rr1", session, task)
            for action in ("cancel", "stop", "cancel"):
                await redis_client.publish(
                    "signal_ch:rr1",
                    json.dumps({"action": action, "task_id": "rr1", "published_at_ns": time.time_ns()}),
                )
            for _ in range(40):
                await asyncio.sleep(0.05)
                if listener._counters["received"] >= 3:
                    break
            assert session.pending_signal_action == "cancel"
            task.cancel.assert_called_once()
        finally:
            await listener.close()

    async def test_last_seen_ignores_foreign_tasks(self, redis_client: RedisClient) -> None:
        listener = SharedRedisListener(redis_client)
        try:
            await listener.start()
            for i in range(20):
                await redis_client.publish(
                    f"signal_ch:foreign_{i}",
                    json.dumps({"action": "cancel", "task_id": f"foreign_{i}", "published_at_ns": time.time_ns()}),
                )
            for _ in range(40):
                await asyncio.sleep(0.05)
                if listener._counters["received"] >= 20:
                    break
            assert listener._counters["received"] >= 20
            assert listener._last_seen == {}
        finally:
            await listener.close()
