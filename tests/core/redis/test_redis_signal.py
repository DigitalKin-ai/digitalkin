"""Tests for SharedRedisListener and RedisSendBuffer.

Covers dispatch, deduplication, race-safety on completed tasks, singleton
invariants, send-buffer batching, flush triggers, ref-counting.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

if TYPE_CHECKING:
    from collections.abc import Coroutine, Generator

pytestmark = pytest.mark.timeout(10)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakePubSub:
    """In-memory pub/sub for unit tests."""

    def __init__(self) -> None:
        self._subscribed: list[str] = []
        self._messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._closed = False

    async def subscribe(self, *channels: str) -> None:
        self._subscribed.extend(channels)

    async def psubscribe(self, *patterns: str) -> None:
        self._subscribed.extend(patterns)

    async def unsubscribe(self, *_channels: str) -> None:
        self._subscribed.clear()

    async def punsubscribe(self, *_patterns: str) -> None:
        self._subscribed.clear()

    async def aclose(self) -> None:
        self._closed = True

    async def get_message(self, ignore_subscribe_messages: bool = True, timeout: float = 0.5) -> dict[str, Any] | None:
        _ = ignore_subscribe_messages, timeout
        try:
            return self._messages.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.01)
            return None

    def inject(self, channel: str, data: str) -> None:
        self._messages.put_nowait({"type": "message", "channel": channel.encode(), "data": data.encode()})

    def inject_pmessage(self, channel: str, data: str, pattern: str = "signal_ch:*") -> None:
        self._messages.put_nowait({
            "type": "pmessage",
            "pattern": pattern.encode(),
            "channel": channel.encode(),
            "data": data.encode(),
        })


class _FakePipeline:
    """In-memory pipeline for unit tests."""

    def __init__(self) -> None:
        self._commands: list[tuple[str, ...]] = []

    def hset(self, name: str, mapping: dict[str, str]) -> Any:
        self._commands.append(("hset", name, str(mapping)))
        return self

    def expire(self, name: str, seconds: int) -> Any:
        self._commands.append(("expire", name, str(seconds)))
        return self

    def publish(self, channel: str, message: str) -> Any:
        self._commands.append(("publish", channel, message))
        return self

    async def execute(self) -> list[bool]:
        return [True] * len(self._commands)


def _make_mock_client() -> MagicMock:
    mock = MagicMock()
    mock.pubsub.return_value = _FakePubSub()
    mock.pipeline.return_value = _FakePipeline()
    mock.hgetall = AsyncMock(return_value={})
    return mock


def _make_fake_session() -> MagicMock:
    """Mock TaskSession exposing the side-channel attributes."""
    session = MagicMock()
    session.pending_signal_action = ""
    session.last_signal_published_ns = 0
    return session


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_instances() -> Generator[None]:
    from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

    SharedRedisListener._instances.clear()
    yield
    SharedRedisListener._instances.clear()


# ===========================================================================
# SharedRedisListener
# ===========================================================================


class TestSharedRedisListenerDispatch:
    """Signal dispatch routing."""

    async def test_critical_signal_writes_side_channel_and_cancels(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        session = _make_fake_session()

        async def long_running() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(long_running(), name="t1_main")
        try:
            await listener.start()
            listener.register("t1", session, task)

            data = {"action": "cancel", "task_id": "t1", "published_at_ns": 12345}
            assert listener.dispatch_signal("t1", data, json.dumps(data)) is True
            assert session.pending_signal_action == "cancel"
            assert session.last_signal_published_ns == 12345
            await asyncio.sleep(0)  # let cancellation propagate
            assert task.cancelled()
        finally:
            if not task.done():
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            await listener.close()

    async def test_non_critical_signal_is_observability_only(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        session = _make_fake_session()

        async def long_running() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(long_running(), name="t1_main")
        try:
            await listener.start()
            listener.register("t1", session, task)

            data = {"action": "ping", "task_id": "t1"}
            assert listener.dispatch_signal("t1", data, json.dumps(data)) is True
            # Side channel untouched, task still running.
            assert not session.pending_signal_action
            assert not task.done()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await listener.close()

    async def test_dispatch_unknown_critical_task_returns_false(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        # Critical action on unregistered task → dispatch_skipped, returns False.
        data = {"action": "cancel", "task_id": "unknown"}
        assert listener.dispatch_signal("unknown", data, json.dumps(data)) is False

    async def test_dispatch_unknown_non_critical_task_returns_true(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        # Non-critical action without a registered task is audit-only → True.
        data = {"action": "ping", "task_id": "unknown"}
        assert listener.dispatch_signal("unknown", data, json.dumps(data)) is True

    async def test_dispatch_skipped_on_task_done(self) -> None:
        """Race-safety: a finished task is not mutated by an incoming signal."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        session = _make_fake_session()

        async def quick() -> None:  # noqa: RUF029
            return

        task = asyncio.create_task(quick(), name="t1_main")
        await listener.start()
        try:
            listener.register("t1", session, task)
            await task  # task is now done

            data = {"action": "cancel", "task_id": "t1"}
            assert listener.dispatch_signal("t1", data, json.dumps(data)) is False
            # Side channel must NOT be written for a done task.
            assert not session.pending_signal_action
        finally:
            await listener.close()

    async def test_dedup_skips_identical_json(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        session = _make_fake_session()

        async def long_running() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(long_running(), name="t1_main")
        try:
            await listener.start()
            listener.register("t1", session, task)
            data = {"action": "ping", "task_id": "t1"}
            raw = json.dumps(data)
            assert listener.dispatch_signal("t1", data, raw) is True
            assert listener.dispatch_signal("t1", data, raw) is False
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await listener.close()


class TestSharedRedisListenerLifecycle:
    """Ref-counting and the singleton invariant."""

    async def test_get_or_create_reuses_instance(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        client = _make_mock_client()
        a = SharedRedisListener.get_or_create("url_1", client)
        b = SharedRedisListener.get_or_create("url_1", client)
        assert a is b
        assert a._refcount == 2

    async def test_release_closes_on_last_ref(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        client = _make_mock_client()
        SharedRedisListener.get_or_create("url_2", client)
        await SharedRedisListener.release("url_2")
        assert "url_2" not in SharedRedisListener._instances

    async def test_singleton_or_none_empty(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        assert SharedRedisListener.singleton_or_none() is None

    async def test_singleton_or_none_single(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        client = _make_mock_client()
        inst = SharedRedisListener.get_or_create("url_solo", client)
        assert SharedRedisListener.singleton_or_none() is inst

    async def test_singleton_or_none_multiple_raises(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        client_a = _make_mock_client()
        client_b = _make_mock_client()
        SharedRedisListener.get_or_create("url_a", client_a)
        SharedRedisListener.get_or_create("url_b", client_b)
        with pytest.raises(RuntimeError, match="singleton invariant violated"):
            SharedRedisListener.singleton_or_none()

    async def test_unregister_clears_state(self) -> None:
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        session = _make_fake_session()

        async def long_running() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(long_running(), name="t_u_main")
        try:
            await listener.start()
            listener.register("t_u", session, task)
            assert "t_u" in listener._task_refs
            listener.unregister("t_u")
            assert "t_u" not in listener._task_refs
            assert "t_u" not in listener._task_sessions
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await listener.close()

    async def test_unregister_does_not_kill_listen_loop(self) -> None:
        """Loop lifetime is process-wide; emptying ``_task_refs`` must not stop it."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        session = _make_fake_session()

        async def long_running() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(long_running(), name="loop_lifetime_test")
        try:
            await listener.start()
            assert listener._listen_task is not None
            listener.register("t_solo", session, task)
            listener.unregister("t_solo")
            assert not listener._task_refs
            assert listener._stop_event.is_set() is False
            assert not listener._listen_task.done()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await listener.close()

    async def test_register_before_start_raises(self) -> None:
        """The PSUBSCRIBE contract is explicit: ``start()`` must precede traffic."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        session = _make_fake_session()

        async def long_running() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(long_running(), name="before_start")
        try:
            with pytest.raises(RuntimeError, match="register called before start"):
                listener.register("t_pre", session, task)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_concurrent_start_calls_spawn_one_loop(self) -> None:
        """``asyncio.Lock`` ensures double-start is idempotent: one psubscribe, one listen task."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        client = _make_mock_client()
        pubsub = client.pubsub.return_value
        original_psub = pubsub.psubscribe
        call_count = 0

        async def counting_psub(*patterns: str) -> None:
            nonlocal call_count
            call_count += 1
            await original_psub(*patterns)

        pubsub.psubscribe = counting_psub
        listener = SharedRedisListener(client)
        try:
            await asyncio.gather(listener.start(), listener.start(), listener.start())
            assert call_count == 1
            assert listener._listen_task is not None
        finally:
            await listener.close()

    async def test_process_id_is_classvar_and_stable(self) -> None:
        """``PROCESS_ID`` is a 32-char hex on the class, identical across reads and instances."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        pid = SharedRedisListener.PROCESS_ID
        assert isinstance(pid, str)
        assert len(pid) == 32
        assert all(c in "0123456789abcdef" for c in pid)
        assert SharedRedisListener.PROCESS_ID == pid
        a = SharedRedisListener(_make_mock_client())
        b = SharedRedisListener(_make_mock_client())
        assert a.PROCESS_ID == b.PROCESS_ID == pid

    async def test_signal_psubscribe_audit_contains_origin(self) -> None:
        """Boot log carries ``origin=<PROCESS_ID>`` for cross-process correlation."""
        import logging

        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        records: list[logging.LogRecord] = []
        handler = logging.Handler()
        handler.setLevel(logging.DEBUG)
        handler.emit = records.append  # type: ignore[method-assign]
        digitalkin_logger = logging.getLogger("digitalkin")
        prev_level = digitalkin_logger.level
        digitalkin_logger.setLevel(logging.DEBUG)
        digitalkin_logger.addHandler(handler)

        listener = SharedRedisListener(_make_mock_client())
        try:
            await listener.start()
            audit = [r.getMessage() for r in records if "signal_psubscribe" in r.getMessage()]
            assert audit, "no signal_psubscribe audit emitted"
            assert f"origin={SharedRedisListener.PROCESS_ID}" in audit[0]
            assert "phase=boot" in audit[0]
        finally:
            digitalkin_logger.removeHandler(handler)
            digitalkin_logger.setLevel(prev_level)
            await listener.close()

    async def test_signal_counters_audit_contains_origin(self) -> None:
        """Periodic counters line carries ``origin=<PROCESS_ID>`` so processes are distinguishable on a shared Redis."""
        import logging
        import time as _time

        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        records: list[logging.LogRecord] = []
        handler = logging.Handler()
        handler.setLevel(logging.DEBUG)
        handler.emit = records.append  # type: ignore[method-assign]
        digitalkin_logger = logging.getLogger("digitalkin")
        prev_level = digitalkin_logger.level
        digitalkin_logger.setLevel(logging.DEBUG)
        digitalkin_logger.addHandler(handler)

        listener = SharedRedisListener(_make_mock_client())
        # Force the >=60s counters branch to fire on the first loop iteration.
        listener._last_counters_log = _time.monotonic() - 100
        try:
            await listener.start()
            for _ in range(40):
                await asyncio.sleep(0.02)
                if any("signal_counters" in r.getMessage() for r in records):
                    break
            counters = [r.getMessage() for r in records if "signal_counters" in r.getMessage()]
            assert counters, "no signal_counters audit emitted"
            assert f"origin={SharedRedisListener.PROCESS_ID}" in counters[0]
        finally:
            digitalkin_logger.removeHandler(handler)
            digitalkin_logger.setLevel(prev_level)
            await listener.close()


class TestSharedRedisListenerInvalidate:
    """``invalidate_*`` dispatch routes to the registered cache_invalidator (not task.cancel)."""

    async def test_invalidate_signal_invokes_cache_invalidator(self) -> None:
        """A ``pmessage`` with action=invalidate_tools triggers the registered invalidator."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        calls: list[tuple[str, str]] = []

        async def fake_invalidator(action: str, setup_id: str) -> None:
            calls.append((action, setup_id))

        listener.set_cache_invalidator(fake_invalidator)

        data = {"action": "invalidate_tools", "setup_id": "s1"}
        assert listener.dispatch_signal("_global_", data, json.dumps(data)) is True
        await asyncio.sleep(0)  # let create_task fire
        assert calls == [("INVALIDATE_TOOLS", "s1")]

    async def test_invalidate_signal_does_not_touch_task_refs(self) -> None:
        """``invalidate_*`` must not cancel any task; ``_task_refs`` unchanged."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        session = _make_fake_session()

        async def long_running() -> None:
            await asyncio.sleep(60)

        task = asyncio.create_task(long_running(), name="invalidate_isolation")
        try:
            await listener.start()
            listener.register("t1", session, task)
            data = {"action": "invalidate_setup", "setup_id": "s1"}
            listener.dispatch_signal("_global_", data, json.dumps(data))
            await asyncio.sleep(0)
            assert not task.done()
            assert "t1" in listener._task_refs  # noqa: SLF001
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await listener.close()

    async def test_invalidate_self_broadcast_is_skipped(self) -> None:
        """A broadcast with ``origin == SharedRedisListener.PROCESS_ID`` is suppressed (no double-invalidation)."""
        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener

        listener = SharedRedisListener(_make_mock_client())
        calls: list[tuple[str, str]] = []

        async def fake_invalidator(action: str, setup_id: str) -> None:
            calls.append((action, setup_id))

        listener.set_cache_invalidator(fake_invalidator)

        data = {"action": "invalidate_setup", "setup_id": "s1", "origin": SharedRedisListener.PROCESS_ID}
        assert listener.dispatch_signal("_global_", data, json.dumps(data)) is True
        await asyncio.sleep(0)
        assert calls == [], "self-broadcast should not invoke local invalidator"


class TestSharedRedisListenerRegisterIsFast:
    """register() must not be on a slow path — guards against re-introducing per-task subscribe."""

    async def test_register_unaffected_by_slow_psubscribe(self) -> None:
        """A 2s slow PSUBSCRIBE happens once in start(); register() never subscribes again."""
        import time as _time

        from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
        from digitalkin.models.settings.redis import get_redis_settings

        class _SlowPubSub(_FakePubSub):
            def __init__(self) -> None:
                super().__init__()
                self.psubscribe_calls = 0

            # Sync def returning the coroutine, so the counter ticks when psubscribe is
            # *called* — an unawaited fire-and-forget re-subscribe is caught too.
            def psubscribe(self, *patterns: str) -> Coroutine[Any, Any, None]:
                self.psubscribe_calls += 1
                return self._slow_psubscribe(*patterns)

            async def _slow_psubscribe(self, *patterns: str) -> None:
                await asyncio.sleep(2.0)
                await _FakePubSub.psubscribe(self, *patterns)

        client = MagicMock()
        pubsub = _SlowPubSub()
        client.pubsub.return_value = pubsub
        listener = SharedRedisListener(client)
        session = _make_fake_session()

        async def long_running() -> None:
            await asyncio.sleep(60)

        task = asyncio.create_task(long_running(), name="slow_subscribe_test")
        try:
            await listener.start()  # 2s slow PSUBSCRIBE happens here, once.
            # register() reads the lru_cached settings singleton, which conftest's autouse
            # fixture clears before every test — warm it so the timing below covers
            # register()'s own work, not a one-time pydantic-settings construction.
            get_redis_settings()
            t0 = _time.perf_counter_ns()
            listener.register("t1", session, task)
            elapsed_ms = (_time.perf_counter_ns() - t0) / 1e6

            # The actual invariant: a per-task subscribe must never come back.
            assert pubsub.psubscribe_calls == 1, "register() must not subscribe per task"
            # Wall-clock backstop for a *blocking* re-subscribe, which the injected pubsub
            # makes cost 2s. Sized to separate that from three dict writes without making
            # CI machine speed the thing under test.
            assert elapsed_ms < 100.0, f"register() blocked {elapsed_ms:.1f}ms — perf regression"
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await listener.close()


# ===========================================================================
# ===========================================================================
