"""Redis Streams for lossless token streaming.

Replaces ``asyncio.Queue`` in ``SingleJobManager`` with Redis Streams,
providing durable, cursor-based output that survives process crashes.

- ``RedisStreamWriter``: batches XADD calls via pipeline (mirrors _RedisSendBuffer).
- ``RedisStreamReader``: reads with XREAD + cursor persistence + gap detection.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from typing import TYPE_CHECKING, Any

from digitalkin.core.resilience.task_supervisor import log_unhandled
from digitalkin.core.task_manager.redis.redis_client import RedisClient  # noqa: TC001
from digitalkin.logger import logger

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class RedisStreamWriter:
    """Writes module output to a Redis Stream with monotonic sequence numbers.

    Each entry contains ``{data: <json>, seq: <uint64>}``.
    EOS (end-of-stream) is written as a special entry with ``eos=true``,
    followed by an EXPIRE on the stream key.
    """

    _task_id: str
    _redis_client: RedisClient
    _stream_key: str
    _seq: int
    _stream_ttl: int
    _maxlen: int

    def __init__(
        self,
        task_id: str,
        redis_client: RedisClient,
        stream_ttl: int = int(os.environ.get("DIGITALKIN_REDIS_STREAM_TTL", "300")),
        maxlen: int = int(os.environ.get("DIGITALKIN_REDIS_STREAM_MAXLEN", "10000")),
    ) -> None:
        """Initialize stream writer.

        Args:
            task_id: Unique task identifier.
            redis_client: Shared Redis connection.
            stream_ttl: TTL in seconds for the stream key after EOS.
            maxlen: Approximate max entries before trimming.
        """
        self._task_id = task_id
        self._redis_client = redis_client
        self._stream_key = f"task:{task_id}:stream"
        self._seq = 0
        self._stream_ttl = stream_ttl
        self._maxlen = maxlen

    async def write(self, data: dict[str, Any]) -> int:
        """Write an output chunk to the stream.

        Args:
            data: Output data to write.

        Returns:
            The sequence number assigned to this entry.
        """
        self._seq += 1
        await self._redis_client.xadd(
            self._stream_key,
            {"data": json.dumps(data, default=str), "seq": str(self._seq)},
            maxlen=self._maxlen,
        )
        return self._seq

    async def write_eos(self) -> None:
        """Write end-of-stream marker and set TTL on the stream key."""
        self._seq += 1
        await self._redis_client.xadd(
            self._stream_key,
            {"data": "", "seq": str(self._seq), "eos": "true"},
        )
        await self._redis_client.expire(self._stream_key, self._stream_ttl)
        logger.debug("RedisStreamWriter.write_eos: task_id=%s seq=%d", self._task_id, self._seq)

    @property
    def last_seq(self) -> int:
        """The last sequence number written."""
        return self._seq


class RedisStreamBatchWriter:
    """Batched Redis Stream writer — accumulates items and flushes via pipeline.

    Instead of 1 XADD per item, accumulates up to ``batch_size`` items or
    waits ``flush_interval_ms`` before flushing all in a single pipeline.
    Same pattern as ``RedisSendBuffer`` but for stream output.

    Use for high-throughput token streaming where per-item XADD latency
    is the bottleneck. For low-frequency output, use ``RedisStreamWriter``.
    """

    _task_id: str
    _redis_client: RedisClient
    _stream_key: str
    _seq: int
    _stream_ttl: int
    _maxlen: int
    _batch_size: int
    _flush_interval: float
    _pending: list[tuple[str, int]]  # (json_data, seq)
    _flush_task: asyncio.Task[None] | None
    _stop_event: asyncio.Event

    def __init__(
        self,
        task_id: str,
        redis_client: RedisClient,
        stream_ttl: int = int(os.environ.get("DIGITALKIN_REDIS_STREAM_TTL", "300")),
        maxlen: int = int(os.environ.get("DIGITALKIN_REDIS_STREAM_MAXLEN", "10000")),
        batch_size: int = int(os.environ.get("DIGITALKIN_REDIS_STREAM_BATCH_SIZE", "20")),
        flush_interval_ms: int = int(os.environ.get("DIGITALKIN_REDIS_STREAM_FLUSH_MS", "50")),
    ) -> None:
        """Initialize batched stream writer.

        Args:
            task_id: Unique task identifier.
            redis_client: Shared Redis connection.
            stream_ttl: TTL in seconds for the stream key after EOS.
            maxlen: Approximate max entries before trimming.
            batch_size: Max items per pipeline flush.
            flush_interval_ms: Max milliseconds before auto-flush.
        """
        self._task_id = task_id
        self._redis_client = redis_client
        self._stream_key = f"task:{task_id}:stream"
        self._seq = 0
        self._stream_ttl = stream_ttl
        self._maxlen = maxlen
        self._batch_size = batch_size
        self._flush_interval = flush_interval_ms / 1000.0
        self._pending = []
        self._flush_task = None
        self._stop_event = asyncio.Event()

    async def write(self, data: dict[str, Any]) -> int:
        """Buffer an output chunk for batched write.

        Flushes immediately when batch is full. Otherwise arms a timer
        for flush_interval_ms. Returns the assigned sequence number.

        Args:
            data: Output data to write.

        Returns:
            The sequence number assigned to this entry.
        """
        self._seq += 1
        self._pending.append((json.dumps(data, default=str), self._seq))

        if len(self._pending) >= self._batch_size:
            await self._flush()
        elif self._flush_task is None or self._flush_task.done():
            self._stop_event = asyncio.Event()
            self._flush_task = asyncio.create_task(
                self._flush_after_interval(),
                name=f"stream_batch_flush_{self._task_id}",
            )
            self._flush_task.add_done_callback(log_unhandled)
        return self._seq

    async def _flush_after_interval(self) -> None:
        """Sleep for flush_interval then flush."""
        try:
            import random

            jittered = self._flush_interval * (0.9 + random.random() * 0.2)  # noqa: S311
            stop_wait = asyncio.create_task(self._stop_event.wait())
            done, _ = await asyncio.wait([stop_wait], timeout=jittered)
            if not done:
                stop_wait.cancel()
            await self._flush()
        except Exception:
            logger.warning("RedisStreamBatchWriter flush timer crashed", exc_info=True)
        finally:
            self._flush_task = None

    async def _flush(self) -> None:
        """Flush all pending items in one Redis pipeline.

        Atomic swap: new writes during flush land in a fresh list.
        """
        batch, self._pending = self._pending, []
        if not batch:
            return

        pipe = self._redis_client.pipeline()
        for json_data, seq in batch:
            pipe.xadd(self._stream_key, {"data": json_data, "seq": str(seq)}, maxlen=self._maxlen, approximate=True)
        await pipe.execute()

    async def write_eos(self) -> None:
        """Flush remaining items, write EOS marker, set TTL."""
        await self._flush()
        self._seq += 1
        await self._redis_client.xadd(
            self._stream_key,
            {"data": "", "seq": str(self._seq), "eos": "true"},
        )
        await self._redis_client.expire(self._stream_key, self._stream_ttl)
        logger.debug("RedisStreamBatchWriter.write_eos: task_id=%s seq=%d", self._task_id, self._seq)

    async def close(self) -> None:
        """Flush and stop the timer task."""
        self._stop_event.set()
        if self._flush_task is not None and not self._flush_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task
        await self._flush()

    @property
    def last_seq(self) -> int:
        """The last sequence number assigned (may not be flushed yet)."""
        return self._seq


class RedisStreamReader:
    """Reads module output from a Redis Stream with cursor persistence and gap detection.

    Yields output dicts from ``XREAD``, tracking the last-read entry ID as a
    cursor in Redis for crash recovery.
    """

    _task_id: str
    _redis_client: RedisClient
    _stream_key: str
    _cursor_key: str
    _last_id: str
    _last_seq: int
    _cursor_ttl: int

    def __init__(
        self,
        task_id: str,
        redis_client: RedisClient,
        cursor_ttl: int = int(os.environ.get("DIGITALKIN_REDIS_CURSOR_TTL", "360")),
    ) -> None:
        """Initialize stream reader.

        Args:
            task_id: Unique task identifier.
            redis_client: Shared Redis connection.
            cursor_ttl: TTL in seconds for the cursor key (slightly > stream TTL).
        """
        self._task_id = task_id
        self._redis_client = redis_client
        self._stream_key = f"task:{task_id}:stream"
        self._cursor_key = f"task:{task_id}:cursor"
        self._last_id = "0-0"
        self._last_seq = 0
        self._cursor_ttl = cursor_ttl

    async def restore_cursor(self) -> None:
        """Restore the read cursor from Redis (for crash recovery)."""
        raw = await self._redis_client.get(self._cursor_key)
        if raw is not None:
            self._last_id = raw.decode()
            logger.debug("RedisStreamReader restored cursor: task_id=%s last_id=%s", self._task_id, self._last_id)
        else:
            logger.warning(
                "RedisStreamReader cursor expired or absent, starting from stream head: task_id=%s",
                self._task_id,
            )

    async def _save_cursor(self) -> None:
        """Persist the current cursor to Redis."""
        await self._redis_client.set(self._cursor_key, self._last_id, ex=self._cursor_ttl)

    async def read(self, count: int = 50, block_ms: int = 1000) -> AsyncGenerator[dict[str, Any], None]:
        """Read entries from the stream, yielding parsed output dicts.

        Performs gap detection on the ``seq`` field. Yields until EOS or
        the stream is empty and block times out.

        Args:
            count: Max entries per XREAD call.
            block_ms: Milliseconds to block waiting for new entries.

        Yields:
            Parsed output dicts from the stream.
        """
        while True:
            result = await self._redis_client.xread(
                {self._stream_key: self._last_id},
                count=count,
                block=block_ms,
            )
            if not result:
                # XREAD returned empty — stream may not exist yet or all entries
                # consumed. Retry; EOS entry will terminate the loop via return.
                continue

            for _stream_name, entries in result:
                for entry_id, fields in entries:
                    self._last_id = entry_id if isinstance(entry_id, str) else entry_id.decode()

                    eos = fields.get(b"eos", b"").decode()
                    if eos == "true":
                        await self._save_cursor()
                        return

                    seq_raw = fields.get(b"seq", b"0").decode()
                    seq = int(seq_raw)
                    if seq != self._last_seq + 1 and self._last_seq > 0:
                        logger.warning(
                            "Gap detected in stream: task_id=%s expected_seq=%d got_seq=%d",
                            self._task_id,
                            self._last_seq + 1,
                            seq,
                        )
                    self._last_seq = seq

                    data_raw = fields.get(b"data", b"{}").decode()
                    try:
                        yield json.loads(data_raw)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("Invalid JSON in stream entry: task_id=%s seq=%d", self._task_id, seq)

            await self._save_cursor()
