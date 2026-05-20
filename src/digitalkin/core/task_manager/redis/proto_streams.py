"""Zero-copy proto binary stream writer/reader for Redis.

Stores ``google.protobuf.Struct`` as serialized binary bytes in Redis
Streams instead of JSON strings. Eliminates 4 dict conversions per
message on the Gateway hot path:

Write: ``Struct.SerializeToString()`` → bytes → Redis XADD (~0.1-0.5ms)
Read:  Redis XREAD → bytes → ``Struct.ParseFromString()`` (~0.1-0.5ms)

vs JSON path:
Write: dict → ``json.dumps()`` → string → Redis XADD (~1-3ms)
Read:  Redis XREAD → ``json.loads()`` → dict → ``Struct.update()`` (~3-8ms)

Use for Gateway-mediated inter-module streams where both ends speak proto. For internal
module output (Pydantic models), use ``RedisStreamWriter`` (JSON) or
convert to proto Struct once at the module boundary.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from google.protobuf import struct_pb2

from digitalkin.core.exceptions import BackpressureTimeoutError
from digitalkin.logger import logger
from digitalkin.models.settings.gateway import GatewaySettings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from digitalkin.core.task_manager.redis.redis_client import RedisClient


class ProtoStreamWriter:
    """Writes proto Struct binary bytes to a Redis Stream.

    Each entry contains ``{pb: <serialized bytes>, seq: <uint64>}``.
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
        *,
        stream_ttl: int | None = None,
        maxlen: int | None = None,
        batch_size: int | None = None,
        flush_ms: int | None = None,
        backpressure_threshold: float | None = None,
        backpressure_delay_ms: int | None = None,
        backpressure_check_interval: int | None = None,
        backpressure_timeout_s: float | None = None,
    ) -> None:
        """Initialize proto stream writer.

        Adaptive flush: buffers entries and pipelines them when writes
        arrive faster than ``flush_ms``. Single slow writes go directly
        via XADD (no pipeline overhead). No mode flag — the writer
        adapts to traffic automatically.

        Args:
            task_id: Unique task identifier.
            redis_client: Shared Redis connection.
            stream_ttl: TTL in seconds for the stream key after EOS.
            maxlen: Approximate max entries before trimming.
            batch_size: Max entries per pipeline flush.
            flush_ms: Max ms between flushes. Writes spaced further
                apart go directly via XADD.
            backpressure_threshold: Fraction of maxlen at which to throttle.
            backpressure_delay_ms: Milliseconds to sleep when backpressure is active.
            backpressure_check_interval: Check XLEN every N writes.
            backpressure_timeout_s: Max seconds to wait on backpressure.
        """
        settings = GatewaySettings()
        effective_stream_ttl = stream_ttl if stream_ttl is not None else settings.stream.redis_stream_ttl
        effective_maxlen = maxlen if maxlen is not None else settings.stream.redis_stream_maxlen
        effective_batch_size = batch_size if batch_size is not None else settings.stream.stream_batch_size
        effective_flush_ms = flush_ms if flush_ms is not None else settings.stream.stream_flush_ms
        bp = settings.backpressure
        effective_bp_threshold = (
            backpressure_threshold if backpressure_threshold is not None else bp.backpressure_threshold
        )
        effective_bp_delay_ms = backpressure_delay_ms if backpressure_delay_ms is not None else bp.backpressure_delay_ms
        effective_bp_check_interval = (
            backpressure_check_interval if backpressure_check_interval is not None else bp.backpressure_check_interval
        )
        effective_bp_timeout_s = (
            backpressure_timeout_s if backpressure_timeout_s is not None else bp.backpressure_timeout_s
        )

        self._task_id = task_id
        self._redis_client = redis_client
        self._stream_key = f"task:{task_id}:stream"
        self._seq = 0
        self._stream_ttl = effective_stream_ttl
        self._maxlen = effective_maxlen
        self._bp_threshold = int(effective_maxlen * effective_bp_threshold)
        self._bp_delay = effective_bp_delay_ms / 1000
        self._bp_check_interval = effective_bp_check_interval
        self._bp_timeout = effective_bp_timeout_s
        self._writes_since_check = 0

        # Adaptive batching
        self._batch_size = effective_batch_size
        self._flush_interval = effective_flush_ms / 1000
        self._pending: list[dict[str, str | bytes]] = []
        self._last_write_time = 0.0  # Ensures first write always goes direct (gap is huge)
        self._last_mode = "single"  # tracks current flush mode for logging

    async def restore_seq(self) -> int:
        """Continue the sequence counter from the stream's last entry.

        Reads the most recent entry via ``XREVRANGE ... COUNT 1`` and sets
        the internal counter to its ``seq`` field. Call this after ``__init__``
        when another writer may have already appended entries to this stream
        (e.g. a previous producer crashed mid-stream and this writer is
        resuming, or the module already wrote entries before this writer
        was created).

        If the stream is empty or the last entry has no ``seq`` field, the
        counter stays at 0 and the next write will be seq=1.

        Returns:
            The restored sequence value (0 if stream is empty).
        """
        last = await self._redis_client.xrevrange(self._stream_key, count=1)
        if not last:
            logger.debug("ProtoStreamWriter.restore_seq: empty stream task_id=%s", self._task_id)
            return 0

        _, fields = last[0]
        seq_raw = fields.get(b"seq", b"0")
        if isinstance(seq_raw, bytes):
            seq_raw = seq_raw.decode()
        try:
            self._seq = int(seq_raw)
        except ValueError:
            logger.warning(
                "ProtoStreamWriter.restore_seq: malformed seq=%r task_id=%s",
                seq_raw,
                self._task_id,
            )
            return 0

        logger.debug(
            "ProtoStreamWriter.restore_seq: task_id=%s resumed at seq=%d",
            self._task_id,
            self._seq,
        )
        return self._seq

    async def _check_backpressure(self) -> None:
        """Check stream length and apply backpressure if needed.

        Uses exponential backoff when at maxlen to avoid tight XLEN polling.
        Base delay doubles each iteration (50ms → 100ms → 200ms → ...) up to 1s.
        """
        self._writes_since_check += 1
        if self._writes_since_check < self._bp_check_interval:
            return

        self._writes_since_check = 0
        stream_len = await self._redis_client.xlen(self._stream_key)
        if stream_len >= self._maxlen:
            logger.warning("Backpressure: stream at maxlen, blocking: task_id=%s len=%d", self._task_id, stream_len)

            waited = 0.0
            delay = self._bp_delay
            while stream_len >= self._maxlen:
                if waited >= self._bp_timeout:
                    msg = (
                        f"Backpressure timeout after {waited:.0f}s on stream "
                        f"task_id={self._task_id} (len={stream_len} >= maxlen={self._maxlen})"
                    )
                    logger.error(msg)
                    raise BackpressureTimeoutError(msg)

                await asyncio.sleep(delay)
                waited += delay
                delay = min(delay * 2, 1.0)  # exponential backoff, cap at 1s
                stream_len = await self._redis_client.xlen(self._stream_key)
        elif stream_len >= self._bp_threshold:
            await asyncio.sleep(self._bp_delay)

    async def _flush(self) -> None:
        """Flush pending batch entries to Redis via pipeline.

        Single entry: direct XADD (no pipeline overhead).
        Multiple entries: pipeline XADD (one RTT for N writes).
        """
        if not self._pending:
            return
        batch, self._pending = self._pending, []
        if len(batch) == 1:
            await self._redis_client.xadd(self._stream_key, batch[0], maxlen=self._maxlen)
        else:
            pipe = self._redis_client.pipeline()
            for entry in batch:
                pipe.xadd(self._stream_key, entry, maxlen=self._maxlen, approximate=True)  # type: ignore[arg-type]
            await pipe.execute()

    async def write_struct(self, data: struct_pb2.Struct) -> int:
        """Write a proto Struct as binary bytes to the stream.

        Adaptive flush — no mode flag, adapts to traffic:
        - Slow writes (gap >= flush_ms): XADD directly, no buffering
        - Fast writes (gap < flush_ms): buffer and pipeline flush when
          batch_size is reached or flush_ms elapses
        - ``write_eos()`` force-flushes remaining entries

        No background timer tasks — flushing is driven by the caller's
        write cadence.

        Args:
            data: Proto Struct to persist.

        Returns:
            The sequence number assigned to this entry.
        """
        await self._check_backpressure()

        self._seq += 1
        entry: dict[str, str | bytes] = {"pb": data.SerializeToString(), "seq": str(self._seq)}
        now = time.monotonic()
        gap = now - self._last_write_time
        self._last_write_time = now

        if gap >= self._flush_interval and not self._pending:
            # Slow traffic: write directly, skip buffering
            if self._last_mode != "single":
                logger.debug("Adaptive flush → single: task_id=%s", self._task_id)
                self._last_mode = "single"
            await self._redis_client.xadd(self._stream_key, entry, maxlen=self._maxlen)
        else:
            # Fast traffic: buffer and flush on size or time
            if self._last_mode != "batch":
                logger.debug("Adaptive flush → batch: task_id=%s", self._task_id)
                self._last_mode = "batch"
            if self._pending and gap >= self._flush_interval:
                await self._flush()
            self._pending.append(entry)
            if len(self._pending) >= self._batch_size:
                await self._flush()

        return self._seq

    async def write_dict(self, data: dict) -> int:
        """Write a dict by converting to proto Struct then serializing.

        One conversion (dict → Struct → bytes) instead of the JSON path's
        two (dict → JSON string → bytes).

        Args:
            data: Dict to persist (must be JSON-compatible types).

        Returns:
            The sequence number assigned to this entry.
        """
        s = struct_pb2.Struct()
        s.update(data)
        return await self.write_struct(s)

    async def write_eos(self) -> None:
        """Flush pending batch (if any), write end-of-stream marker, set TTL."""
        if self._pending:
            await self._flush()

        self._seq += 1
        await self._redis_client.xadd(
            self._stream_key,
            {"pb": b"", "seq": str(self._seq), "eos": b"true"},
        )
        await self._redis_client.expire(self._stream_key, self._stream_ttl)
        logger.debug("ProtoStreamWriter.write_eos: task_id=%s seq=%d", self._task_id, self._seq)

    @property
    def last_seq(self) -> int:
        """The last sequence number written."""
        return self._seq


class ProtoStreamReader:
    """Reads proto Struct binary bytes from a Redis Stream.

    Zero-copy read: bytes → ``ParseFromString()`` → proto Struct.
    No JSON parsing, no dict intermediate.
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
        cursor_ttl: int | None = None,
    ) -> None:
        """Initialize proto stream reader.

        Args:
            task_id: Unique task identifier.
            redis_client: Shared Redis connection.
            cursor_ttl: TTL in seconds for the cursor key. Defaults to
                ``GatewayStreamSettings.redis_cursor_ttl``.
        """
        self._task_id = task_id
        self._redis_client = redis_client
        self._stream_key = f"task:{task_id}:stream"
        self._cursor_key = f"task:{task_id}:cursor"
        self._last_id = "0-0"
        self._last_seq = 0
        self._cursor_ttl = cursor_ttl if cursor_ttl is not None else GatewaySettings().stream.redis_cursor_ttl

    async def restore_cursor(self) -> None:
        """Restore the read cursor from Redis."""
        raw = await self._redis_client.get(self._cursor_key)
        if raw is not None:
            self._last_id = raw.decode()
            logger.debug("ProtoStreamReader restored cursor: task_id=%s", self._task_id)
        else:
            logger.warning("ProtoStreamReader cursor absent, starting from head: task_id=%s", self._task_id)

    async def _save_cursor(self) -> None:
        """Persist the current cursor to Redis."""
        await self._redis_client.set(self._cursor_key, self._last_id, ex=self._cursor_ttl)

    async def read_structs(
        self,
        count: int = 50,
        block_ms: int | None = None,
        cursor_save_interval: int = 100,
    ) -> AsyncGenerator[struct_pb2.Struct, None]:
        """Read proto Structs from the stream until EOS.

        Blocks on ``XREAD`` for up to ``block_ms`` per iteration. Entries
        are deserialized via ``ParseFromString()`` (zero-copy from Redis
        bytes). Terminates when an entry with ``eos=true`` is read.

        Cursor is saved every ``cursor_save_interval`` entries (not every
        XREAD batch) to reduce Redis SET ops under high concurrency.
        Worst-case crash re-reads up to ``cursor_save_interval`` entries.

        Args:
            count: Max entries per XREAD call.
            block_ms: Milliseconds to block waiting for new entries.
            cursor_save_interval: Save cursor every N entries (default 100).

        Yields:
            Proto Struct objects from the stream.
        """
        effective_block_ms = block_ms if block_ms is not None else GatewaySettings().stream.stream_read_block_ms
        entries_since_save = 0
        while True:
            t_xread_start = time.perf_counter_ns()
            result = await self._redis_client.xread(
                {self._stream_key: self._last_id},
                count=count,
                block=effective_block_ms,
            )
            t_xread_end = time.perf_counter_ns()
            if not result:
                continue

            for _stream_name, entries in result:
                for entry_id, fields in entries:
                    self._last_id = entry_id if isinstance(entry_id, str) else entry_id.decode()

                    eos = fields.get(b"eos", b"")
                    if eos == b"true":
                        logger.info(
                            "[close-debug] reader_saw_eos: last_xread_block=%.2fms t_seen_ns=%d task_id=%s",
                            (t_xread_end - t_xread_start) / 1e6,
                            t_xread_end,
                            self._task_id,
                        )
                        await self._save_cursor()
                        return

                    seq_raw = fields.get(b"seq", b"0").decode()
                    try:
                        seq = int(seq_raw)
                    except ValueError:
                        logger.warning("Malformed seq=%r, skipping: task_id=%s", seq_raw, self._task_id)
                        continue
                    if seq != self._last_seq + 1 and self._last_seq > 0:
                        logger.warning(
                            "Gap in proto stream: task_id=%s expected=%d got=%d",
                            self._task_id,
                            self._last_seq + 1,
                            seq,
                        )
                    self._last_seq = seq

                    pb_bytes = fields.get(b"pb", b"")
                    if not pb_bytes:
                        continue

                    s = struct_pb2.Struct()
                    s.ParseFromString(pb_bytes)
                    yield s

                    entries_since_save += 1

            if entries_since_save >= cursor_save_interval:
                await self._save_cursor()
                entries_since_save = 0
