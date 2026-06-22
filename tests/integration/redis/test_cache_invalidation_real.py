"""L1 integration: cross-process cache invalidation via real Redis pub/sub.

Run with: ``docker compose --profile redis up -d`` then
``uv run pytest tests/integration/redis -m integration``.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


class TestCacheInvalidationFanOut:
    """`signal_ch:_global_` PSUBSCRIBE wildcard fan-outs invalidate_* to every peer listener."""

    async def test_invalidate_tools_broadcast_reaches_peer_listener(self, redis_client: RedisClient) -> None:
        """A peer listener subscribed to signal_ch:* receives invalidate_tools and fires its invalidator."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        peer = SharedRedisListener(redis_client)
        calls: list[tuple[str, str]] = []

        async def fake_invalidator(action: str, setup_id: str) -> None:
            calls.append((action, setup_id))

        peer.set_cache_invalidator(fake_invalidator)
        try:
            await peer.start()
            payload = json.dumps({
                "action": "invalidate_tools",
                "setup_id": "s1",
                "published_at_ns": time.time_ns(),
                # Different origin so the peer does NOT self-skip
                "origin": "other-process-uuid",
            })
            await redis_client.publish("signal_ch:_global_", payload)

            for _ in range(60):
                await asyncio.sleep(0.05)
                if calls:
                    break
            assert calls == [("INVALIDATE_TOOLS", "s1")]
        finally:
            await peer.close()

    async def test_self_broadcast_is_suppressed(self, redis_client: RedisClient) -> None:
        """A broadcast carrying our own ``SharedRedisListener.PROCESS_ID`` is skipped (no double-invalidation)."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        listener = SharedRedisListener(redis_client)
        calls: list[tuple[str, str]] = []

        async def fake_invalidator(action: str, setup_id: str) -> None:
            calls.append((action, setup_id))

        listener.set_cache_invalidator(fake_invalidator)
        try:
            await listener.start()
            payload = json.dumps({
                "action": "invalidate_tools",
                "setup_id": "s1",
                "published_at_ns": time.time_ns(),
                "origin": SharedRedisListener.PROCESS_ID,
            })
            await redis_client.publish("signal_ch:_global_", payload)
            await asyncio.sleep(0.4)
            assert calls == [], "self-broadcast should not invoke local invalidator"
        finally:
            await listener.close()

    async def test_scoped_invalidate_pops_only_target_setup_id_e2e(self, redis_client: RedisClient) -> None:
        """End-to-end: broadcast with setup_id=s1 wipes s1 in peer; siblings s2/s3 untouched."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        peer = SharedRedisListener(redis_client)
        tool_cache_state = {"s1": "tools_v1", "s2": "tools_v1", "s3": "tools_v1"}

        async def scoped_invalidator(action: str, setup_id: str) -> None:
            if action == "INVALIDATE_TOOLS" and setup_id:
                tool_cache_state.pop(setup_id, None)

        peer.set_cache_invalidator(scoped_invalidator)
        try:
            await peer.start()
            payload = json.dumps({
                "action": "invalidate_tools",
                "setup_id": "s1",
                "published_at_ns": time.time_ns(),
                "origin": "other-process-uuid",
            })
            await redis_client.publish("signal_ch:_global_", payload)

            for _ in range(60):
                await asyncio.sleep(0.05)
                if "s1" not in tool_cache_state:
                    break
            assert tool_cache_state == {"s2": "tools_v1", "s3": "tools_v1"}
        finally:
            await peer.close()
