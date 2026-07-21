"""Tests for DefaultTaskManager — in-memory signal service.

Covers send + close. Receiving signals is now owned by
``SharedRedisListener.dispatch_signal`` — DefaultTaskManager is a
sender-only strategy.
"""

import pytest

from digitalkin.services.task_manager.default_task_manager import DefaultTaskManager

pytestmark = pytest.mark.timeout(5)


class TestDefaultTaskManagerSmoke:
    """Basic lifecycle: send + close."""

    @pytest.mark.smoke
    async def test_send_signal_stores_in_dict(self) -> None:
        """send_signal upserts into _signals dict."""
        tm = DefaultTaskManager()
        data = {"action": "cancel", "task_id": "t1"}
        result = await tm.send_signal("t1", data)

        assert result == data
        assert tm._signals["t1"] == data

    @pytest.mark.smoke
    async def test_close_clears_state(self) -> None:
        """close marks closed and drops the signals dict."""
        tm = DefaultTaskManager()
        await tm.send_signal("t1", {"action": "test"})

        await tm.close()

        assert tm._closed is True
        assert len(tm._signals) == 0


class TestDefaultTaskManagerEdgeCases:
    """Edge cases and boundary conditions."""

    @pytest.mark.edge_case
    async def test_send_after_close_does_not_raise(self) -> None:
        """Sending after close doesn't raise."""
        tm = DefaultTaskManager()
        await tm.close()

        result = await tm.send_signal("t1", {"action": "late"})
        assert result["action"] == "late"

    @pytest.mark.edge_case
    async def test_double_close_is_safe(self) -> None:
        """Calling close twice doesn't raise."""
        tm = DefaultTaskManager()
        await tm.close()
        await tm.close()
