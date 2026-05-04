"""Tests for RedisTaskManager — Redis pub/sub signal delivery.

Unit tests using mocks for SharedRedisListener and RedisClient.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from digitalkin.services.task_manager.redis_task_manager import RedisTaskManager

pytestmark = pytest.mark.timeout(5)


class TestRedisTaskManagerSmoke:
    """Basic lifecycle: send, subscribe, unsubscribe, close."""

    def _make_tm(self) -> tuple[RedisTaskManager, MagicMock]:
        redis_client = MagicMock()
        redis_client.publish = AsyncMock(return_value=1)
        with patch("digitalkin.services.task_manager.redis_task_manager.SharedRedisListener") as mock_listener_cls:
            listener = MagicMock()
            listener.register = AsyncMock(return_value=asyncio.Queue())
            listener.unregister = MagicMock()
            mock_listener_cls.get_or_create.return_value = listener
            mock_listener_cls.release = AsyncMock()
            tm = RedisTaskManager(redis_client, redis_url="test")
        return tm, listener

    @pytest.mark.smoke
    async def test_send_signal_publishes_to_redis(self) -> None:
        """send_signal publishes JSON to signal_ch:{task_id}."""
        tm, _ = self._make_tm()
        data = {"action": "cancel", "task_id": "t1"}

        result = await tm.send_signal("t1", data)

        assert result == data
        tm._redis_client.publish.assert_awaited_once()
        call_args = tm._redis_client.publish.call_args
        assert call_args[0][0] == "signal_ch:t1"

    @pytest.mark.smoke
    async def test_subscribe_registers_with_listener(self) -> None:
        """subscribe_signals calls listener.register(task_id)."""
        tm, listener = self._make_tm()

        sub_id, gen = await tm.subscribe_signals("task-123")

        listener.register.assert_awaited_once_with("task-123")
        assert sub_id == "task-123"

    @pytest.mark.smoke
    async def test_unsubscribe_calls_listener_unregister(self) -> None:
        """unsubscribe_signals calls listener.unregister."""
        tm, listener = self._make_tm()

        await tm.unsubscribe_signals("task-123")

        listener.unregister.assert_called_once_with("task-123")

    @pytest.mark.smoke
    async def test_close_releases_listener(self) -> None:
        """close calls SharedRedisListener.release."""
        tm, _ = self._make_tm()

        with patch("digitalkin.services.task_manager.redis_task_manager.SharedRedisListener") as mock_cls:
            mock_cls.release = AsyncMock()
            await tm.close()
            mock_cls.release.assert_awaited_once_with("test")


class TestRedisTaskManagerEdgeCases:
    """Edge cases."""

    @pytest.mark.edge_case
    async def test_subscribe_generator_yields_queue_items(self) -> None:
        """The generator yields items from the registered queue."""
        redis_client = MagicMock()
        redis_client.publish = AsyncMock(return_value=1)

        queue: asyncio.Queue = asyncio.Queue()
        await queue.put({"action": "cancel", "task_id": "t1"})
        await queue.put(None)  # sentinel to stop

        with patch("digitalkin.services.task_manager.redis_task_manager.SharedRedisListener") as mock_cls:
            listener = MagicMock()
            listener.register = AsyncMock(return_value=queue)
            mock_cls.get_or_create.return_value = listener
            tm = RedisTaskManager(redis_client, redis_url="test")

        _, gen = await tm.subscribe_signals("t1")

        items = []
        async for item in gen:
            items.append(item)

        assert len(items) == 1
        assert items[0]["action"] == "cancel"
