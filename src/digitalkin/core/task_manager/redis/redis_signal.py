"""Redis signal transport: SharedRedisListener (pub/sub receive) + RedisSendBuffer (batched publish)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from typing import TYPE_CHECKING, Any, ClassVar

from digitalkin.core.resilience.task_supervisor import log_unhandled
from digitalkin.core.task_manager.redis.redis_client import RedisClient  # noqa: TC001
from digitalkin.logger import logger
from digitalkin.models.settings.redis import get_redis_settings

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from digitalkin.core.task_manager.task_session import TaskSession

    CacheInvalidator = Callable[[str, str], Coroutine[Any, Any, None]]


class SharedRedisListener:
    """One PubSub connection per Redis URL; direct-dispatches signals to tasks."""

    PROCESS_ID: ClassVar[str] = uuid.uuid4().hex
    """Per-process UUID generated at class definition; identifies this listener on
    ``signal_ch:_global_`` broadcasts. ``os.getpid()`` collides in Docker (always 1)."""

    _instances: ClassVar[dict[str, SharedRedisListener]] = {}

    @classmethod
    def get_or_create(cls, key: str, redis_client: RedisClient) -> SharedRedisListener:
        """Reuse the listener for this Redis URL or create one; bumps refcount.

        Returns:
            The listener for ``key``.
        """
        if key not in cls._instances:
            cls._instances[key] = cls(redis_client)
        inst = cls._instances[key]
        inst._refcount += 1  # noqa: SLF001
        return inst

    @classmethod
    async def release(cls, key: str) -> None:
        """Drop one refcount; close + drop the instance at zero."""
        inst = cls._instances.get(key)
        if inst is None:
            return
        inst._refcount -= 1  # noqa: SLF001
        if inst._refcount <= 0:  # noqa: SLF001
            cls._instances.pop(key, None)
            await inst.close()

    @classmethod
    def singleton_or_none(cls) -> SharedRedisListener | None:
        """Return the single active listener; ``None`` if absent.

        Returns:
            The lone instance, or ``None`` when ``_instances`` is empty.

        Raises:
            RuntimeError: If more than one instance exists.
        """
        if not cls._instances:
            return None
        if len(cls._instances) > 1:
            msg = f"Multiple SharedRedisListener instances ({len(cls._instances)}) — singleton invariant violated"
            raise RuntimeError(msg)
        return next(iter(cls._instances.values()))

    def __init__(self, redis_client: RedisClient) -> None:
        """Init with a shared Redis client."""
        self._redis_client = redis_client
        self._refcount: int = 0
        self._task_refs: dict[str, asyncio.Task[None]] = {}
        self._task_sessions: dict[str, TaskSession] = {}
        self._last_seen: dict[str, str] = {}
        self._pubsub: Any = None
        self._listen_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._start_lock = asyncio.Lock()
        self._counters: dict[str, int] = {
            "received": 0,
            "deduped": 0,
            "evicted": 0,
            "dropped": 0,
            "restarts": 0,
            "subscribed": 0,
            "invalidated": 0,
        }
        self._last_counters_log = time.monotonic()
        self._cache_invalidator: CacheInvalidator | None = None

    def set_cache_invalidator(self, handler: CacheInvalidator) -> None:
        """Register the ``(action_name, setup_id)`` handler invoked for ``invalidate_*`` signals."""
        self._cache_invalidator = handler

    async def start(self) -> None:
        """Open PubSub, PSUBSCRIBE ``signal_ch:*``, and start the listen loop. Idempotent under concurrent callers."""
        async with self._start_lock:
            if self._pubsub is not None and self._listen_task is not None and not self._listen_task.done():
                return
            psub_t0 = time.perf_counter_ns()
            self._pubsub = self._redis_client.pubsub()
            await self._pubsub.psubscribe("signal_ch:*")
            psub_ms = (time.perf_counter_ns() - psub_t0) / 1e6
            logger.debug(
                "[perf] signal_psubscribe: psubscribe_ms=%.2f pattern=signal_ch:* phase=boot origin=%s",
                psub_ms,
                SharedRedisListener.PROCESS_ID,
            )
            self._stop_event = asyncio.Event()
            self._listen_task = asyncio.create_task(self._listen_loop(), name="shared_redis_listener")
            self._listen_task.add_done_callback(log_unhandled)

    def register(
        self,
        task_id: str,
        session: TaskSession,
        task: asyncio.Task[None],
    ) -> None:
        """Store session + task refs; sub-millisecond, never awaits.

        Raises:
            RuntimeError: If max registered tasks is exceeded or ``start()`` was never called.
        """
        if self._listen_task is None or self._listen_task.done():
            msg = "SharedRedisListener.register called before start()"
            raise RuntimeError(msg)
        sig = get_redis_settings().signal
        if len(self._task_refs) >= sig.max_tasks:
            msg = f"SharedRedisListener: max tasks ({sig.max_tasks}) exceeded"
            raise RuntimeError(msg)

        reg_t0 = time.perf_counter_ns()
        self._task_sessions[task_id] = session
        self._task_refs[task_id] = task
        task.add_done_callback(lambda _: self.unregister(task_id))

        self._counters["subscribed"] += 1
        logger.debug(
            "[perf] signal_subscribe: register_ms=%.2f active_subs=%d task_id=%s origin=%s",
            (time.perf_counter_ns() - reg_t0) / 1e6,
            len(self._task_refs),
            task_id,
            SharedRedisListener.PROCESS_ID,
        )

    def unregister(self, task_id: str) -> None:
        """Drop the task_id. Loop lifetime is process-wide; ``close()`` is the only stop site."""
        self._task_refs.pop(task_id, None)
        self._task_sessions.pop(task_id, None)
        self._last_seen.pop(task_id, None)

    def dispatch_signal(self, task_id: str, data: dict[str, Any], raw_json: str) -> bool:
        """Route a signal: ``cancel``/``stop`` → side channel + ``task.cancel()``; other actions → audit-only.

        Returns:
            ``True`` if dispatched, ``False`` on dedup or already-done task.
        """
        dispatch_t0 = time.perf_counter_ns()
        if raw_json == self._last_seen.get(task_id):
            self._counters["deduped"] += 1
            return False
        self._last_seen[task_id] = raw_json

        action = data.get("action", "")
        pub_ns = data.get("published_at_ns") or 0
        e2e_ms = (time.time_ns() - pub_ns) / 1e6 if pub_ns else 0.0
        self._counters["received"] += 1

        if action.startswith("invalidate_"):
            origin = data.get("origin")
            if origin is not None and origin == SharedRedisListener.PROCESS_ID:
                return True
            setup_id = data.get("setup_id", "")
            self._counters["invalidated"] += 1
            logger.debug(
                "[perf] signal_invalidate: e2e_ms=%.2f action=%s setup_id=%s",
                e2e_ms,
                action,
                setup_id,
            )
            if self._cache_invalidator is not None:
                inv_task: asyncio.Task[None] = asyncio.create_task(
                    self._cache_invalidator(action.upper(), setup_id),
                    name=f"invalidate_{action}",
                )
                inv_task.add_done_callback(log_unhandled)
            return True

        logger.debug(
            "[perf] signal_dispatch: e2e_ms=%.2f dispatch_ms=%.2f action=%s task_id=%s",
            e2e_ms,
            (time.perf_counter_ns() - dispatch_t0) / 1e6,
            action,
            task_id,
        )

        if action not in {"cancel", "stop"}:
            return True

        task = self._task_refs.get(task_id)
        session = self._task_sessions.get(task_id)
        if task is None or session is None or task.done():
            logger.info(
                "[signal] dispatch_skipped: action=%s reason=task_already_done task_id=%s",
                action,
                task_id,
            )
            return False

        session.pending_signal_action = action
        session.last_signal_published_ns = pub_ns
        task.cancel()
        return True

    @staticmethod
    def _parse_message(msg: dict[str, Any]) -> tuple[str, dict[str, Any], str] | None:
        """Extract ``(task_id, data, raw_json)`` from a PubSub message.

        Returns:
            The triple, or ``None`` if the message is not a usable ``signal_ch:`` payload.
        """
        if msg["type"] not in {"message", "pmessage"}:
            return None
        channel = msg["channel"].decode() if isinstance(msg["channel"], bytes) else msg["channel"]
        if not channel.startswith("signal_ch:"):
            return None
        task_id = channel[len("signal_ch:") :]
        raw_json = msg["data"].decode() if isinstance(msg["data"], bytes) else msg["data"]
        try:
            data = json.loads(raw_json)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Invalid JSON in signal for task_id=%s", task_id)
            return None
        return task_id, data, raw_json

    async def _listen_loop(self) -> None:
        """Drain PubSub messages; exponential-backoff retry on transient Redis errors."""
        backoff = 0.1
        while not self._stop_event.is_set():
            try:  # noqa: PLW0717
                if self._pubsub is None:
                    self._pubsub = self._redis_client.pubsub()
                    psub_t0 = time.perf_counter_ns()
                    await self._pubsub.psubscribe("signal_ch:*")
                    psub_ms = (time.perf_counter_ns() - psub_t0) / 1e6
                    logger.debug(
                        "[perf] signal_psubscribe: psubscribe_ms=%.2f pattern=signal_ch:* phase=loop",
                        psub_ms,
                    )
                msg = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if msg is not None:
                    parsed = self._parse_message(msg)
                    if parsed is not None:
                        route_task_id, data, raw_json = parsed
                        pub_ns = data.get("published_at_ns") or 0
                        e2e_ms = (time.time_ns() - pub_ns) / 1e6 if pub_ns else 0.0
                        logger.debug(
                            "[perf] signal_route: e2e_ms=%.2f action=%s task_id=%s",
                            e2e_ms,
                            data.get("action", ""),
                            route_task_id,
                        )
                        self.dispatch_signal(route_task_id, data, raw_json)
                backoff = 0.1
            except asyncio.CancelledError:
                break
            except Exception:
                self._counters["restarts"] += 1
                if self._pubsub is not None:
                    with contextlib.suppress(Exception):
                        await self._pubsub.aclose()
                self._pubsub = None
                logger.exception("SharedRedisListener iteration error, retrying in %.1fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)

            now = time.monotonic()
            if now - self._last_counters_log >= 60.0:  # noqa: PLR2004
                c = self._counters
                logger.debug(
                    "[perf] signal_counters: origin=%s received=%d deduped=%d evicted=%d "
                    "dropped=%d listener_restarts=%d active_subs=%d subscribed_total=%d "
                    "invalidated=%d",
                    SharedRedisListener.PROCESS_ID,
                    c["received"],
                    c["deduped"],
                    c["evicted"],
                    c["dropped"],
                    c["restarts"],
                    len(self._task_refs),
                    c["subscribed"],
                    c["invalidated"],
                )
                self._last_counters_log = now
        self._listen_task = None

    async def close(self) -> None:
        """Stop the listener and close the PubSub connection."""
        self._stop_event.set()
        if self._listen_task is not None and not self._listen_task.done():
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
        self._task_refs.clear()
        self._task_sessions.clear()
        self._last_seen.clear()
        if self._pubsub is not None:
            with contextlib.suppress(Exception):
                await self._pubsub.punsubscribe("signal_ch:*")
            with contextlib.suppress(Exception):
                await self._pubsub.aclose()
            self._pubsub = None
