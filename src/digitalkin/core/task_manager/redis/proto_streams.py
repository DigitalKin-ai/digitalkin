"""Zero-copy proto binary stream reader for Redis.

Reads ``google.protobuf.Struct`` entries stored as serialized binary bytes in a
Redis Stream (``{pb, seq}`` entries + an ``eos`` marker), as written by
``module_runner._on_output`` on the Gateway hot path. Avoids the JSON round-trip:

Read:  Redis XREAD → bytes → ``Struct.ParseFromString()`` (~0.1-0.5ms)
vs JSON: Redis XREAD → ``json.loads()`` → dict → ``Struct.update()`` (~3-8ms)
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from google.protobuf import struct_pb2
from google.protobuf.message import DecodeError

from digitalkin.logger import logger
from digitalkin.models.settings.gateway import get_gateway_settings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from digitalkin.core.task_manager.redis.redis_client import RedisClient


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

    def __init__(self, task_id: str, redis_client: RedisClient) -> None:
        """Initialize proto stream reader.

        Cursor TTL comes from ``GatewayStreamSettings.redis_cursor_ttl`` (env
        ``DIGITALKIN_REDIS_CURSOR_TTL``).

        Args:
            task_id: Unique task identifier.
            redis_client: Shared Redis connection.
        """
        self._task_id = task_id
        self._redis_client = redis_client
        self._stream_key = f"task:{task_id}:stream"
        self._cursor_key = f"task:{task_id}:cursor"
        self._last_id = "0-0"
        self._last_seq = 0

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
        await self._redis_client.set(self._cursor_key, self._last_id, ex=get_gateway_settings().stream.redis_cursor_ttl)

    async def read_structs(  # noqa: C901
        self,
        count: int = 50,
        cursor_save_interval: int = 100,
        skip_to_seq: int | None = None,
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
            cursor_save_interval: Save cursor every N entries (default 100).
            skip_to_seq: If set, entries with stored ``seq <= skip_to_seq``
                are consumed (cursor and gap detection advance) but not
                yielded, so a resumed reader starts past the consumer's cursor.

        Yields:
            Proto Struct objects from the stream.
        """
        block_ms = get_gateway_settings().stream.stream_read_block_ms
        entries_since_save = 0
        while True:
            t_xread_start = time.perf_counter_ns()
            result = await self._redis_client.xread(
                {self._stream_key: self._last_id},
                count=count,
                block=block_ms,
            )
            t_xread_end = time.perf_counter_ns()
            if not result:
                continue

            for _stream_name, entries in result:
                for entry_id, fields in entries:
                    self._last_id = entry_id if isinstance(entry_id, str) else entry_id.decode()

                    eos = fields.get(b"eos", b"")
                    if eos == b"true":
                        logger.debug(
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

                    if skip_to_seq is not None and seq <= skip_to_seq:
                        continue

                    pb_bytes = fields.get(b"pb", b"")
                    if not pb_bytes:
                        continue

                    s = struct_pb2.Struct()
                    try:
                        s.ParseFromString(pb_bytes)
                    except DecodeError:
                        # M6: poison entry (truncated/corrupt pb) — drop it, keep the stream alive.
                        continue
                    yield s

                    entries_since_save += 1

            if entries_since_save >= cursor_save_interval:
                await self._save_cursor()
                entries_since_save = 0
