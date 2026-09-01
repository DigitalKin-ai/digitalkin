"""L1 integration: SharedRedisListener against real Redis.

Run with: ``docker compose --profile redis up -d`` then
``uv run pytest tests/integration/redis -m integration``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


def _make_fake_session() -> MagicMock:
    """Return a Mock TaskSession with the side-channel attrs the listener writes."""
    s = MagicMock()
    s.pending_signal_action = ""
    s.last_signal_published_ns = 0
    return s


class TestSharedRedisListenerReal:
    """SharedRedisListener wired to a real Redis from the docker-compose ``redis`` profile."""

    async def test_start_psubscribes_and_dispatches_critical_signal(self, redis_client: RedisClient) -> None:
        """End-to-end: ``start()`` PSUBSCRIBEs; a published CANCEL reaches ``dispatch_signal``."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        listener = SharedRedisListener(redis_client)
        task: asyncio.Task[None] | None = None
        try:
            await listener.start()
            session = _make_fake_session()

            async def long_running() -> None:
                await asyncio.sleep(10)

            task = asyncio.create_task(long_running(), name="rt1_main")
            listener.register("rt1", session, task)

            payload = json.dumps({
                "action": "cancel",
                "task_id": "rt1",
                "published_at_ns": time.time_ns(),
            })
            await redis_client.publish("signal_ch:rt1", payload)

            for _ in range(40):
                await asyncio.sleep(0.05)
                if session.pending_signal_action:
                    break
            assert session.pending_signal_action == "cancel"
        finally:
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await listener.close()

    async def test_register_is_microseconds_after_start(self, redis_client: RedisClient) -> None:
        """Real Redis: ``start()`` pays the wire cost; ``register()`` stays sub-5ms."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        listener = SharedRedisListener(redis_client)
        task: asyncio.Task[None] | None = None
        try:
            await listener.start()
            session = _make_fake_session()

            async def long_running() -> None:
                await asyncio.sleep(10)

            task = asyncio.create_task(long_running(), name="rt2_main")
            t0 = time.perf_counter_ns()
            listener.register("rt2", session, task)
            elapsed_ms = (time.perf_counter_ns() - t0) / 1e6
            assert elapsed_ms < 5.0, f"register() took {elapsed_ms:.1f}ms against real Redis"
        finally:
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await listener.close()

    async def test_psubscribe_once_across_many_tasks(self, redis_client: RedisClient) -> None:
        """One PSUBSCRIBE for many tasks — verified via the pubsub's pattern set."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        listener = SharedRedisListener(redis_client)
        spawned: list[asyncio.Task[None]] = []
        try:
            await listener.start()
            for i in range(10):
                s = _make_fake_session()

                async def long_running() -> None:
                    await asyncio.sleep(10)

                t = asyncio.create_task(long_running(), name=f"rt_n_{i}_main")
                spawned.append(t)
                listener.register(f"rt_n_{i}", s, t)

            # redis-py exposes the live pattern set on the PubSub object — must be exactly 1
            assert listener._pubsub is not None  # noqa: SLF001
            patterns = listener._pubsub.patterns  # noqa: SLF001
            assert len(patterns) == 1, f"expected 1 PSUBSCRIBE pattern, got {len(patterns)}: {list(patterns)}"
            # And the redis-side `channels` (per-channel SUBSCRIBE) must be empty — proves no per-task subscribe.
            channels = listener._pubsub.channels  # noqa: SLF001
            assert len(channels) == 0, f"expected 0 per-channel SUBSCRIBEs, got {len(channels)}: {list(channels)}"
        finally:
            for t in spawned:
                if not t.done():
                    t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await t
            await listener.close()

    async def test_reconnect_re_psubscribes(self, redis_client: RedisClient) -> None:
        """After force-closing ``_pubsub``, the listen loop re-PSUBSCRIBEs and resumes dispatch."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        listener = SharedRedisListener(redis_client)
        task: asyncio.Task[None] | None = None
        try:
            await listener.start()
            session = _make_fake_session()

            async def long_running() -> None:
                await asyncio.sleep(10)

            task = asyncio.create_task(long_running(), name="rt3_main")
            listener.register("rt3", session, task)

            with contextlib.suppress(Exception):
                await listener._pubsub.aclose()
            listener._pubsub = None

            await asyncio.sleep(0.3)
            payload = json.dumps({
                "action": "cancel",
                "task_id": "rt3",
                "published_at_ns": time.time_ns(),
            })
            await redis_client.publish("signal_ch:rt3", payload)

            for _ in range(60):
                await asyncio.sleep(0.05)
                if session.pending_signal_action:
                    break
            assert session.pending_signal_action == "cancel"
        finally:
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await listener.close()

    async def test_psubscribe_survives_task_churn_real(self, redis_client: RedisClient) -> None:
        """Listener stays alive when tasks come and go; global broadcasts after idle still land."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        listener = SharedRedisListener(redis_client)
        calls: list[tuple[str, str]] = []

        async def fake_invalidator(action: str, setup_id: str) -> None:
            calls.append((action, setup_id))

        listener.set_cache_invalidator(fake_invalidator)
        try:
            await listener.start()
            listen_task_id = id(listener._listen_task)

            for i in range(3):
                session = _make_fake_session()

                async def quick() -> None:  # noqa: RUF029
                    return

                t = asyncio.create_task(quick(), name=f"churn_{i}_main")
                listener.register(f"churn_{i}", session, t)
                await t
                await asyncio.sleep(0.1)

            assert not listener._task_refs  # noqa: SLF001
            assert listener._listen_task is not None  # noqa: SLF001
            assert not listener._listen_task.done()  # noqa: SLF001
            assert id(listener._listen_task) == listen_task_id, "loop must NOT be respawned"  # noqa: SLF001

            payload = json.dumps({
                "action": "invalidate_tools",
                "setup_id": "s_churn",
                "published_at_ns": time.time_ns(),
                "origin": "other-process-uuid",
            })
            await redis_client.publish("signal_ch:_global_", payload)

            for _ in range(60):
                await asyncio.sleep(0.05)
                if calls:
                    break
            assert calls == [("INVALIDATE_TOOLS", "s_churn")]
        finally:
            await listener.close()

    async def test_listener_recovers_from_killed_pubsub_connection_real(self, redis_client: RedisClient) -> None:
        """``CLIENT KILL TYPE pubsub`` force-closes the listener's connection; redis-py auto-resubscribes via ``on_connect``."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        SharedRedisListener._instances.clear()
        listener = SharedRedisListener(redis_client)
        calls: list[tuple[str, str]] = []

        async def fake_invalidator(action: str, setup_id: str) -> None:
            calls.append((action, setup_id))

        listener.set_cache_invalidator(fake_invalidator)
        try:
            await listener.start()

            killed = await redis_client._client.execute_command("CLIENT", "KILL", "TYPE", "pubsub")  # noqa: SLF001
            assert int(killed) >= 1, "expected at least one pubsub client killed"

            await asyncio.sleep(1.0)

            payload = json.dumps({
                "action": "invalidate_setup",
                "setup_id": "s_kill",
                "published_at_ns": time.time_ns(),
                "origin": "other-process-uuid",
            })
            await redis_client.publish("signal_ch:_global_", payload)

            for _ in range(80):
                await asyncio.sleep(0.05)
                if calls:
                    break
            assert calls == [("INVALIDATE_SETUP", "s_kill")], "broadcast lost after CLIENT KILL"
        finally:
            await listener.close()
