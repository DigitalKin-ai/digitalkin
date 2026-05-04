"""Tests for DefaultTaskManager — in-memory signal service.

Covers send/subscribe/unsubscribe lifecycle, isolation between tasks,
and close behavior.
"""

import asyncio

import pytest

from digitalkin.services.task_manager.default_task_manager import DefaultTaskManager

pytestmark = pytest.mark.timeout(5)


class TestDefaultTaskManagerSmoke:
    """Basic lifecycle: send, subscribe, unsubscribe, close."""

    @pytest.mark.smoke
    async def test_send_signal_stores_in_dict(self) -> None:
        """send_signal upserts into _signals dict."""
        tm = DefaultTaskManager()
        data = {"action": "cancel", "task_id": "t1"}
        result = await tm.send_signal("t1", data)

        assert result == data
        assert tm._signals["t1"] == data

    @pytest.mark.smoke
    async def test_subscribe_yields_sent_signals(self) -> None:
        """Subscriber receives signals sent after subscribing."""
        tm = DefaultTaskManager()
        sub_id, gen = await tm.subscribe_signals("t1")

        await tm.send_signal("t1", {"action": "cancel", "task_id": "t1"})
        await tm.unsubscribe_signals(sub_id)

        received = []
        async for item in gen:
            received.append(item)

        assert len(received) == 1
        assert received[0]["action"] == "cancel"

    @pytest.mark.smoke
    async def test_unsubscribe_stops_generator(self) -> None:
        """unsubscribe sends poison pill, generator terminates."""
        tm = DefaultTaskManager()
        sub_id, gen = await tm.subscribe_signals("t1")

        await tm.unsubscribe_signals(sub_id)

        items = [item async for item in gen]
        assert items == []
        assert sub_id not in tm._subscribers

    @pytest.mark.smoke
    async def test_close_cleans_all_state(self) -> None:
        """close poisons all subscribers and clears signals."""
        tm = DefaultTaskManager()
        _, gen1 = await tm.subscribe_signals("t1")
        _, gen2 = await tm.subscribe_signals("t2")
        await tm.send_signal("t1", {"action": "test"})

        await tm.close()

        assert tm._closed is True
        assert len(tm._signals) == 0
        assert len(tm._subscribers) == 0


class TestDefaultTaskManagerConcurrency:
    """Concurrent access patterns."""

    @pytest.mark.concurrency
    async def test_subscribe_multiple_tasks_isolated(self) -> None:
        """Multiple subscribers each receive all broadcast signals."""
        tm = DefaultTaskManager()
        sub1_id, gen1 = await tm.subscribe_signals("t1")
        sub2_id, gen2 = await tm.subscribe_signals("t2")

        await tm.send_signal("t1", {"task_id": "t1", "action": "a"})
        await tm.send_signal("t2", {"task_id": "t2", "action": "b"})

        await tm.unsubscribe_signals(sub1_id)
        await tm.unsubscribe_signals(sub2_id)

        items1 = [item async for item in gen1]
        items2 = [item async for item in gen2]

        # Both subscribers receive both signals (broadcast)
        assert len(items1) == 2
        assert len(items2) == 2


class TestDefaultTaskManagerEdgeCases:
    """Edge cases and boundary conditions."""

    @pytest.mark.edge_case
    async def test_send_after_close_does_not_raise(self) -> None:
        """Sending after close doesn't raise (no subscribers to notify)."""
        tm = DefaultTaskManager()
        await tm.close()

        result = await tm.send_signal("t1", {"action": "late"})
        assert result["action"] == "late"

    @pytest.mark.edge_case
    async def test_unsubscribe_unknown_id_is_noop(self) -> None:
        """Unsubscribing with unknown sub_id does nothing."""
        tm = DefaultTaskManager()
        await tm.unsubscribe_signals("nonexistent")

    @pytest.mark.edge_case
    async def test_double_close_is_safe(self) -> None:
        """Calling close twice doesn't raise."""
        tm = DefaultTaskManager()
        await tm.close()
        await tm.close()
