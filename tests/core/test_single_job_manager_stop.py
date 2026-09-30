"""SingleJobManager.stop(): running tasks are cancelled through Redis, slots freed, listener released."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.task_manager.redis_task_manager import RedisTaskManager

try:
    from tests.gateway.test_dial_consumer import _FakeRedisClient
except ImportError:  # pragma: no cover - fakeredis missing
    _FakeRedisClient = None  # type: ignore[assignment,misc]

pytestmark = [
    pytest.mark.timeout(30),
    pytest.mark.skipif(_FakeRedisClient is None, reason="fakeredis not installed"),
]


@pytest.fixture(autouse=True)
def _clear_listeners() -> Any:
    SharedRedisListener._instances.clear()
    yield
    SharedRedisListener._instances.clear()


def _manager(redis: Any) -> SingleJobManager:
    mgr = object.__new__(SingleJobManager)
    mgr._task_manager = LocalTaskManager(default_timeout=5.0)
    mgr._redis_client = redis
    mgr._redis_task_manager = RedisTaskManager(redis)
    return mgr


def _module(task_manager: RedisTaskManager) -> Mock:
    module = Mock(spec=BaseModule)
    module.stop = AsyncMock()
    module.context = Mock()
    module.context.session = Mock()
    module.context.session.setup_id = "setup:t"
    module.context.session.setup_version_id = "sv:t"
    module.context.session.current_ids = Mock(return_value={"task_id": "long", "mission_id": "m"})
    module.context.task_manager = task_manager
    module.context.cleanup = AsyncMock()
    return module


async def test_stop_cancels_running_task_and_releases_listener() -> None:
    redis = _FakeRedisClient()
    try:
        mgr = _manager(redis)
        listener = SharedRedisListener.singleton_or_none()
        assert listener is not None
        await listener.start()
        module = _module(mgr._redis_task_manager)
        started = asyncio.Event()

        async def long_job() -> None:
            started.set()
            await asyncio.sleep(30)

        await mgr.create_task("long", "m", module, long_job())
        await started.wait()
        assert mgr._task_manager._active_slots == 1

        await mgr.stop()

        assert "long" not in mgr.tasks_sessions
        assert mgr._task_manager._active_slots == 0
        module.stop.assert_awaited_once_with("shutdown")
        module.context.cleanup.assert_awaited_once()
        assert SharedRedisListener._instances == {}
        assert listener._listen_task is None
    finally:
        await redis.close()
