"""Tests for RedisTaskManager — Redis pub/sub signal delivery.

Unit tests using mocks for SharedRedisListener and RedisClient.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from digitalkin.services.task_manager.redis_task_manager import RedisTaskManager

pytestmark = pytest.mark.timeout(5)


class TestRedisTaskManagerSmoke:
    """Basic lifecycle: send + close."""

    def _make_tm(self) -> tuple[RedisTaskManager, MagicMock]:
        redis_client = MagicMock()
        redis_client.publish = AsyncMock(return_value=1)
        with patch("digitalkin.services.task_manager.redis_task_manager.SharedRedisListener") as mock_listener_cls:
            listener = MagicMock()
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
    async def test_close_releases_listener(self) -> None:
        """close calls SharedRedisListener.release."""
        tm, _ = self._make_tm()

        with patch("digitalkin.services.task_manager.redis_task_manager.SharedRedisListener") as mock_cls:
            mock_cls.release = AsyncMock()
            await tm.close()
            mock_cls.release.assert_awaited_once_with("test")
