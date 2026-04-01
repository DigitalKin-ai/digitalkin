"""Tests for RedisStreamBatchWriter.

Covers batch accumulation, size-triggered flush, time-triggered flush,
jitter, EOS, close, concurrent writes.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock

import pytest

pytestmark = [pytest.mark.timeout(15)]


class _FakePipeline:
    """In-memory pipeline tracking commands."""

    def __init__(self) -> None:
        self._commands: list[tuple[str, ...]] = []

    def xadd(self, name: str, fields: dict[str, str | bytes], **kwargs: Any) -> _FakePipeline:
        self._commands.append(("xadd", name))
        return self

    async def execute(self) -> list[bool]:
        return [True] * len(self._commands)


def _mock_client() -> MagicMock:
    mock = MagicMock()
    mock.pipeline.return_value = _FakePipeline()
    mock.xadd = MagicMock()
    mock.expire = MagicMock()

    async def fake_xadd(*_a: Any, **_kw: Any) -> bytes:
        return b"1-0"

    async def fake_expire(*_a: Any, **_kw: Any) -> bool:
        return True

    mock.xadd = fake_xadd
    mock.expire = fake_expire
    return mock


@pytest.fixture(autouse=True)
def _fresh() -> Generator[None]:
    yield


class TestBatchAccumulation:
    """Items accumulate until batch_size or flush_interval."""

    async def test_flush_on_batch_size(self) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamBatchWriter

        client = _mock_client()
        writer = RedisStreamBatchWriter("task_b1", client, batch_size=3, flush_interval_ms=5000)

        s1 = await writer.write({"a": 1})
        s2 = await writer.write({"b": 2})
        s3 = await writer.write({"c": 3})  # Triggers flush

        assert s1 == 1
        assert s2 == 2
        assert s3 == 3
        assert len(writer._pending) == 0  # Flushed

    async def test_no_flush_below_batch_size(self) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamBatchWriter

        client = _mock_client()
        writer = RedisStreamBatchWriter("task_b2", client, batch_size=10, flush_interval_ms=5000)

        await writer.write({"a": 1})
        await writer.write({"b": 2})

        assert len(writer._pending) == 2  # Not flushed yet

        await writer.close()

    async def test_flush_on_timer(self) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamBatchWriter

        client = _mock_client()
        writer = RedisStreamBatchWriter("task_b3", client, batch_size=100, flush_interval_ms=50)

        await writer.write({"a": 1})
        assert len(writer._pending) == 1

        # Wait for timer flush
        await asyncio.sleep(0.1)
        assert len(writer._pending) == 0

        await writer.close()


class TestBatchEOS:
    """EOS flushes remaining items."""

    async def test_eos_flushes_pending(self) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamBatchWriter

        client = _mock_client()
        writer = RedisStreamBatchWriter("task_b4", client, batch_size=100, flush_interval_ms=5000)

        await writer.write({"a": 1})
        await writer.write({"b": 2})
        assert len(writer._pending) == 2

        await writer.write_eos()
        assert len(writer._pending) == 0
        assert writer.last_seq == 3  # 2 items + EOS


class TestBatchClose:
    """Close flushes and stops timer."""

    async def test_close_flushes_remaining(self) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamBatchWriter

        client = _mock_client()
        writer = RedisStreamBatchWriter("task_b5", client, batch_size=100, flush_interval_ms=5000)

        await writer.write({"a": 1})
        await writer.close()
        assert len(writer._pending) == 0

    async def test_close_idempotent(self) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamBatchWriter

        client = _mock_client()
        writer = RedisStreamBatchWriter("task_b6", client)
        await writer.close()
        await writer.close()  # Should not raise


class TestBatchConcurrency:
    """Concurrent writes resolve correctly."""

    @pytest.mark.concurrency
    async def test_concurrent_writes_all_flushed(self) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamBatchWriter

        client = _mock_client()
        writer = RedisStreamBatchWriter("task_bc", client, batch_size=5, flush_interval_ms=50)

        await asyncio.gather(*[writer.write({"i": i}) for i in range(20)])

        # All should be flushed (4 batches of 5)
        assert len(writer._pending) == 0
        assert writer.last_seq == 20

        await writer.close()
