"""Redis signal transport infrastructure.

Provides the shared listener and batched send buffer used by the core
task manager for signal delivery. These are infrastructure components,
not swappable service strategies.

- ``SharedRedisListener``: one PubSub connection per Redis URL, dispatches
  to per-task bounded queues with deduplication and priority dispatch.
- ``RedisSendBuffer``: batches HSET+EXPIRE+PUBLISH into Redis pipelines,
  flushing on size or time threshold.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any, ClassVar

from digitalkin.core.task_manager.redis.redis_client import RedisClient  # noqa: TC001
from digitalkin.logger import logger

# ============================================================================
# SharedRedisListener — one PubSub for N tasks
# ============================================================================


class SharedRedisListener:
    """Aggregates Redis pub/sub for all task subscriptions sharing one RedisClient.

    Instead of N PubSub connections (one per task), a single listener
    subscribes to ``signal_ch:{task_id}`` channels and dispatches messages
    to per-task bounded queues.

    Thread-safety: all methods are called from the same event loop.
    The listen loop runs as a single asyncio.Task.
    """

    _instances: ClassVar[dict[str, SharedRedisListener]] = {}

    @classmethod
    def get_or_create(cls, key: str, redis_client: RedisClient) -> SharedRedisListener:
        """Get existing listener for this Redis URL or create a new one.

        Args:
            key: Redis URL identifying the shared resource.
            redis_client: Shared Redis connection.

        Returns:
            Shared listener for this URL.
        """
        if key not in cls._instances:
            cls._instances[key] = cls(redis_client)
        inst = cls._instances[key]
        inst._refcount += 1  # noqa: SLF001
        return inst

    @classmethod
    async def release(cls, key: str) -> None:
        """Decrement refcount and close when last holder releases.

        Args:
            key: Redis URL identifying the shared resource.
        """
        inst = cls._instances.get(key)
        if inst is None:
            return
        inst._refcount -= 1  # noqa: SLF001
        if inst._refcount <= 0:  # noqa: SLF001
            cls._instances.pop(key, None)
            await inst.close()

    def __init__(self, redis_client: RedisClient) -> None:
        """Initialize the shared listener.

        Args:
            redis_client: Shared Redis connection pool.
        """
        from digitalkin.models.settings.redis import RedisSignalSettings

        sig = RedisSignalSettings()
        self._redis_client = redis_client
        self._refcount: int = 0
        self._task_queues: dict[str, asyncio.Queue[dict[str, Any] | None]] = {}
        self._task_refs: dict[str, asyncio.Task[None]] = {}
        self._last_seen: dict[str, str] = {}
        self._pubsub: Any = None
        self._listen_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._queue_size = sig.queue_size
        self._max_tasks = sig.max_tasks

    async def register(self, task_id: str) -> asyncio.Queue[dict[str, Any] | None]:
        """Register a task_id and subscribe to its signal channel.

        Args:
            task_id: Unique task identifier.

        Returns:
            Bounded queue that receives signal dicts or None (sentinel).

        Raises:
            RuntimeError: If max registered tasks is exceeded.
        """
        if len(self._task_queues) >= self._max_tasks:
            msg = f"SharedRedisListener: max tasks ({self._max_tasks}) exceeded"
            raise RuntimeError(msg)
        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=self._queue_size)
        self._task_queues[task_id] = queue

        # Subscribe before starting the listen loop — prevents get_message on unsubscribed pubsub
        if self._pubsub is None:
            self._pubsub = self._redis_client.pubsub()
        await self._pubsub.subscribe(f"signal_ch:{task_id}")

        if self._listen_task is None or self._listen_task.done():
            self._stop_event = asyncio.Event()
            self._listen_task = asyncio.create_task(self._listen_loop(), name="shared_redis_listener")
        return queue

    def register_task(self, task_id: str, task: asyncio.Task[None]) -> None:
        """Associate an asyncio.Task with a task_id for direct cancellation.

        Auto-cleanup: a done callback unregisters the task when it finishes
        (success, cancel, or crash), preventing orphaned entries in
        _task_queues/_task_refs/_last_seen.

        Args:
            task_id: Unique task identifier.
            task: The asyncio.Task to cancel on stop/cancel signal.
        """
        self._task_refs[task_id] = task
        task.add_done_callback(lambda _: self.unregister(task_id))

    def unregister(self, task_id: str) -> None:
        """Remove a task_id from listening. Stops listener when empty.

        Args:
            task_id: Unique task identifier.
        """
        self._task_queues.pop(task_id, None)
        self._task_refs.pop(task_id, None)
        self._last_seen.pop(task_id, None)
        if not self._task_queues:
            self._stop_event.set()

    def wake(self, task_id: str) -> None:
        """Send a None sentinel to wake up a blocked consumer for task_id.

        Args:
            task_id: Unique task identifier.
        """
        if (queue := self._task_queues.get(task_id)) is not None:
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(None)

    def dispatch_signal(self, task_id: str, data: dict[str, Any], raw_json: str) -> bool:
        """Enqueue a signal if it has not already been seen.

        Deduplication: identical JSON payloads are skipped.
        Priority: stop/cancel signals evict oldest item on QueueFull,
        then send a None sentinel and unregister the task.

        Args:
            task_id: Task this signal is for.
            data: Parsed signal dict.
            raw_json: Original JSON string (used for dedup).

        Returns:
            True if the signal was queued (new), False if skipped or dropped.
        """
        queue = self._task_queues.get(task_id)
        if queue is None:
            return False

        if raw_json == self._last_seen.get(task_id):
            return False
        self._last_seen[task_id] = raw_json

        action = data.get("action", "")
        try:
            queue.put_nowait(data)
        except asyncio.QueueFull:
            if action in {"stop", "cancel"}:
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
                queue.put_nowait(data)
                logger.warning("Signal queue full for task_id=%s, dropped oldest for critical %s", task_id, action)
            else:
                logger.warning("Signal queue full for task_id=%s, dropping signal", task_id)
                return False

        if action in {"stop", "cancel"}:
            # Cancel the asyncio.Task directly — no per-task listener needed
            task = self._task_refs.get(task_id)
            if task is not None and not task.done():
                task.cancel()
            with contextlib.suppress(Exception):
                queue.put_nowait(None)
            self.unregister(task_id)
        return True

    @staticmethod
    def _parse_message(msg: dict[str, Any]) -> tuple[str, dict[str, Any], str] | None:
        """Extract task_id, data, and raw JSON from a PubSub message.

        Args:
            msg: Raw PubSub message dict.

        Returns:
            Tuple of (task_id, parsed_data, raw_json) or None if invalid.
        """
        if msg["type"] != "message":
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
        """Single loop reading PubSub messages for all registered tasks.

        Crash recovery: try/except inside the loop with exponential backoff
        (0.1s → 10s cap). Transient Redis errors retry instead of killing
        signal delivery for all tasks.
        """
        backoff = 0.1
        while not self._stop_event.is_set():
            try:
                if self._pubsub is None:
                    await asyncio.sleep(0.05)
                    continue
                msg = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if msg is not None:
                    parsed = self._parse_message(msg)
                    if parsed is not None:
                        self.dispatch_signal(*parsed)
                backoff = 0.1
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("SharedRedisListener iteration error, retrying in %.1fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
        self._listen_task = None

    async def close(self) -> None:
        """Stop the listener, drain all queues, close PubSub connection."""
        self._stop_event.set()
        if self._listen_task is not None and not self._listen_task.done():
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
        for queue in self._task_queues.values():
            with contextlib.suppress(Exception):
                queue.put_nowait(None)
        self._task_queues.clear()
        self._last_seen.clear()
        if self._pubsub is not None:
            with contextlib.suppress(Exception):
                await self._pubsub.unsubscribe()
            with contextlib.suppress(Exception):
                await self._pubsub.aclose()
            self._pubsub = None


# ============================================================================
# RedisSendBuffer — batched pipeline writes
# ============================================================================


class RedisSendBuffer:
    """Batches HSET+EXPIRE+PUBLISH into Redis pipelines.

    Instead of 3 round-trips per signal send, signals are accumulated
    and flushed together either when the batch hits ``max_batch_size``
    or after ``flush_interval`` seconds — whichever comes first.

    Relies on asyncio's single-threaded model: list operations between
    await points are atomic, so no locks are needed for the pending list.
    """

    _instances: ClassVar[dict[str, RedisSendBuffer]] = {}

    @classmethod
    def get_or_create(cls, key: str, redis_client: RedisClient, signal_ttl: int) -> RedisSendBuffer:
        """Get existing buffer for this Redis URL or create a new one.

        Args:
            key: Redis URL identifying the shared resource.
            redis_client: Shared Redis connection.
            signal_ttl: TTL in seconds for signal hash keys.

        Returns:
            Shared buffer for this URL.
        """
        if key not in cls._instances:
            cls._instances[key] = cls(redis_client, signal_ttl)
        inst = cls._instances[key]
        inst._refcount += 1  # noqa: SLF001
        return inst

    @classmethod
    async def release(cls, key: str) -> None:
        """Decrement refcount and close when last holder releases.

        Args:
            key: Redis URL identifying the shared resource.
        """
        inst = cls._instances.get(key)
        if inst is None:
            return
        inst._refcount -= 1  # noqa: SLF001
        if inst._refcount <= 0:  # noqa: SLF001
            cls._instances.pop(key, None)
            await inst.close()

    def __init__(self, redis_client: RedisClient, signal_ttl: int) -> None:
        """Initialize the send buffer.

        Args:
            redis_client: Shared Redis connection pool.
            signal_ttl: TTL in seconds for signal hash keys.
        """
        from digitalkin.models.settings.redis import RedisSignalSettings

        sig = RedisSignalSettings()
        self._redis_client = redis_client
        self._signal_ttl = signal_ttl
        self._refcount: int = 0
        self._flush_interval = sig.flush_interval
        self._max_batch_size = sig.max_batch_size
        self._max_pending = sig.max_pending
        self._pending: list[tuple[str, str, asyncio.Future[bool]]] = []
        self._flush_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def send(self, task_id: str, json_data: str) -> bool:
        """Enqueue a signal and wait for the batch flush.

        Args:
            task_id: Unique task identifier.
            json_data: Pre-serialized JSON signal payload.

        Returns:
            True when the pipeline completes successfully.

        Raises:
            RuntimeError: If the pending buffer exceeds max_pending.
        """
        if len(self._pending) >= self._max_pending:
            msg = f"RedisSendBuffer: pending buffer full ({self._max_pending} items)"
            raise RuntimeError(msg)
        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._pending.append((task_id, json_data, future))

        if len(self._pending) >= self._max_batch_size:
            await self._flush()
        elif self._flush_task is None or self._flush_task.done():
            self._stop_event = asyncio.Event()
            self._flush_task = asyncio.create_task(self._flush_after_interval(), name="redis_send_buffer_flush")

        return await future

    async def _flush_after_interval(self) -> None:
        """Sleep for flush_interval (with jitter) then flush."""
        try:
            # ±10% jitter to prevent thundering herd across buffers
            import random

            jittered = self._flush_interval * (0.9 + random.random() * 0.2)  # noqa: S311
            stop_wait = asyncio.create_task(self._stop_event.wait())
            done, _ = await asyncio.wait([stop_wait], timeout=jittered)
            if not done:
                stop_wait.cancel()
            await self._flush()
        except Exception:
            logger.warning("RedisSendBuffer flush timer crashed", exc_info=True)
        finally:
            self._flush_task = None

    async def _flush(self) -> None:
        """Execute all pending signals in one Redis pipeline and resolve futures.

        Atomic swap: new sends during flush land in a fresh list.
        Pipeline packs N x (HSET + EXPIRE + PUBLISH) into one round-trip.
        """
        batch, self._pending = self._pending, []
        if not batch:
            return

        futures = [f for _, _, f in batch]
        exc: Exception | None = None

        try:
            pipe = self._redis_client.pipeline()
            for task_id, json_data, _ in batch:
                key = f"signal:{task_id}"
                pipe.hset(key, mapping={"data": json_data})
                pipe.expire(key, self._signal_ttl)
                pipe.publish(f"signal_ch:{task_id}", json_data)
            await pipe.execute()
        except Exception as e:
            exc = e
            logger.warning("RedisSendBuffer pipeline failed: %s", e)

        for f in futures:
            if not f.done():
                if exc is not None:
                    f.set_exception(exc)
                else:
                    f.set_result(True)

    async def close(self) -> None:
        """Flush all pending signals and stop the timer task."""
        self._stop_event.set()
        if self._flush_task is not None and not self._flush_task.done():
            with contextlib.suppress(Exception):
                await self._flush_task
        await self._flush()
