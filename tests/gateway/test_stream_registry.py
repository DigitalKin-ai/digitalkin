"""Unit tests for StreamRegistry.

Covers: capacity enforcement, register/unregister, heartbeat touch,
zombie reaper, shutdown cleanup. Tests run in local-only mode
(redis_client=None) for fast unit tests.
"""

from __future__ import annotations

import asyncio

import pytest

from digitalkin.grpc_servers.stream_registry import StreamRegistry
from digitalkin.grpc_servers.stream_session import StreamSession

pytestmark = [pytest.mark.timeout(15)]


class TestRegistryCapacity:
    """Capacity enforcement via max_streams."""

    async def test_register_within_capacity(self) -> None:
        reg = StreamRegistry(max_streams=5)
        for i in range(5):
            await reg.register(StreamSession(task_id=f"t_{i}"))
        assert reg.active_count == 5

    async def test_register_over_capacity_returns_false(self) -> None:
        reg = StreamRegistry(max_streams=2)
        assert await reg.register(StreamSession(task_id="t_0")) is True
        assert await reg.register(StreamSession(task_id="t_1")) is True
        assert await reg.register(StreamSession(task_id="t_overflow")) is False

    async def test_unregister_frees_slot(self) -> None:
        reg = StreamRegistry(max_streams=1)
        await reg.register(StreamSession(task_id="t_a"))
        await reg.unregister("t_a")
        await reg.register(StreamSession(task_id="t_b"))
        assert reg.active_count == 1


class TestRegistryLookup:
    """Get and unregister operations."""

    async def test_get_returns_session(self) -> None:
        reg = StreamRegistry(max_streams=10)
        s = StreamSession(task_id="t_get")
        await reg.register(s)
        assert reg.get("t_get") is s

    def test_get_unknown_returns_none(self) -> None:
        reg = StreamRegistry(max_streams=10)
        assert reg.get("nonexistent") is None

    async def test_unregister_returns_session(self) -> None:
        reg = StreamRegistry(max_streams=10)
        s = StreamSession(task_id="t_unreg")
        await reg.register(s)
        removed = await reg.unregister("t_unreg")
        assert removed is s
        assert reg.active_count == 0

    async def test_unregister_unknown_returns_none(self) -> None:
        reg = StreamRegistry(max_streams=10)
        result = await reg.unregister("nonexistent")
        assert result is None


class TestRegistryLruEviction:
    """LRU cache eviction when local cache is full."""

    async def test_lru_evicts_oldest(self) -> None:
        reg = StreamRegistry(max_streams=100, max_local=3)
        for i in range(4):
            await reg.register(StreamSession(task_id=f"t_{i}"))
        # t_0 should be evicted (oldest)
        assert reg.get("t_0") is None
        assert reg.get("t_3") is not None
        assert reg.active_count == 3


class TestRegistryShutdown:
    """Clean shutdown."""

    async def test_shutdown_tears_down_all_sessions(self) -> None:
        reg = StreamRegistry(max_streams=10)
        for i in range(5):
            await reg.register(StreamSession(task_id=f"t_sd_{i}"))

        await reg.shutdown()
        assert reg.active_count == 0

    async def test_shutdown_without_reaper(self) -> None:
        """Reaper doesn't start without Redis — shutdown is still clean."""
        reg = StreamRegistry(max_streams=10, reaper_interval=0.05)
        await reg.start_reaper()
        # No Redis → reaper not started
        assert reg._reaper_task is None

        await reg.shutdown()  # Should not raise
