"""Tests for FileHistoryMixin caching, batching, and storage optimization."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.mixins.file_history_mixin import FileHistoryMixin
from digitalkin.models.services.storage import FileHistory, FileModel
from digitalkin.services.storage.storage_strategy import StorageRecord


class _ConcreteMixin(FileHistoryMixin):
    """Concrete class to test FileHistoryMixin."""


def _make_context(mission_id: str = "test_mission") -> MagicMock:
    """Build a mock ModuleContext with storage and session."""
    ctx = MagicMock()
    ctx.session.mission_id = mission_id
    ctx.storage = AsyncMock()
    ctx.storage.read = AsyncMock(return_value=None)
    ctx.storage.upsert = AsyncMock(return_value=MagicMock(spec=StorageRecord))
    ctx.storage.update = AsyncMock(return_value=MagicMock(spec=StorageRecord))
    return ctx


def _make_files(count: int = 1, prefix: str = "file") -> list[FileModel]:
    """Create a list of FileModel instances."""
    return [FileModel(file_id=f"{prefix}_{i}", name=f"{prefix}_{i}.txt", metadata={}) for i in range(count)]


def _storage_record_with_history(files: list[dict[str, Any]]) -> MagicMock:
    """Create a mock StorageRecord whose .data looks like a FileHistory."""
    record = MagicMock(spec=StorageRecord)
    record.data = FileHistory(files=[FileModel(**{**{"metadata": {}}, **f}) for f in files])
    return record


class TestFileHistoryCache:
    """Tests for in-memory file history caching."""

    @pytest.mark.asyncio
    async def test_load_reads_storage_once_then_caches(self) -> None:
        """Second load_file_history call returns cached value without gRPC read."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        existing = _storage_record_with_history([{"file_id": "f1", "name": "a.txt"}])
        ctx.storage.read = AsyncMock(return_value=existing)

        first = await mixin.load_file_history(ctx)
        second = await mixin.load_file_history(ctx)

        if first is not second:
            pytest.fail("Expected cached FileHistory object on second call")
        ctx.storage.read.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_load_returns_empty_on_cache_miss(self) -> None:
        """When storage has no record, returns empty FileHistory and caches it."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        history = await mixin.load_file_history(ctx)

        if len(history.files) != 0:
            pytest.fail(f"Expected empty files, got {len(history.files)}")
        await mixin.load_file_history(ctx)
        ctx.storage.read.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_different_missions_cached_independently(self) -> None:
        """Different mission_ids get independent cache entries."""
        mixin = _ConcreteMixin()
        ctx_a = _make_context(mission_id="mission_a")
        ctx_b = _make_context(mission_id="mission_b")

        history_a = await mixin.load_file_history(ctx_a)
        history_b = await mixin.load_file_history(ctx_b)

        if history_a is history_b:
            pytest.fail("Different missions should have separate cache entries")


class TestAppendStorageOptimization:
    """Tests for upsert vs update optimization with batched flush."""

    @pytest.mark.asyncio
    async def test_first_append_uses_upsert(self) -> None:
        """First append to a new key uses upsert_storage after flush."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_files_history(ctx, _make_files(1))
        await mixin.flush_file_history(ctx)

        ctx.storage.upsert.assert_awaited_once()
        ctx.storage.update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_second_append_uses_update(self) -> None:
        """After first persist, subsequent appends use update_storage (1 call)."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_files_history(ctx, _make_files(1, "first"))
        await mixin.flush_file_history(ctx)
        await mixin.append_files_history(ctx, _make_files(1, "second"))
        await mixin.flush_file_history(ctx)

        if ctx.storage.upsert.await_count != 1:
            pytest.fail(f"Expected exactly 1 upsert call, got {ctx.storage.upsert.await_count}")
        if ctx.storage.update.await_count != 1:
            pytest.fail(f"Expected exactly 1 update call, got {ctx.storage.update.await_count}")

    @pytest.mark.asyncio
    async def test_preexisting_record_uses_update_from_start(self) -> None:
        """When storage already has the record, first append uses update (not upsert)."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        existing = _storage_record_with_history([{"file_id": "f1", "name": "old.txt"}])
        ctx.storage.read = AsyncMock(return_value=existing)

        await mixin.load_file_history(ctx)
        await mixin.append_files_history(ctx, _make_files(1))
        await mixin.flush_file_history(ctx)

        ctx.storage.upsert.assert_not_awaited()
        ctx.storage.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_append_accumulates_files_in_cache(self) -> None:
        """Multiple appends accumulate in the cached FileHistory object."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_files_history(ctx, _make_files(2, "batch1"))
        await mixin.append_files_history(ctx, _make_files(3, "batch2"))

        history = await mixin.load_file_history(ctx)
        if len(history.files) != 5:
            pytest.fail(f"Expected 5 files in cache, got {len(history.files)}")
        ctx.storage.read.assert_awaited_once()


class TestBatchingBehavior:
    """Tests for batched flush behavior."""

    @pytest.mark.asyncio
    async def test_append_does_not_write_below_threshold(self) -> None:
        """Files below threshold are buffered, not written to storage."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_files_history(ctx, _make_files(1))

        ctx.storage.upsert.assert_not_awaited()
        ctx.storage.update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_threshold_triggers_flush(self) -> None:
        """Reaching the threshold auto-flushes to storage."""
        mixin = _ConcreteMixin()
        mixin._fh_flush_threshold = 3
        ctx = _make_context()

        await mixin.append_files_history(ctx, _make_files(1, "a"))
        await mixin.append_files_history(ctx, _make_files(1, "b"))
        ctx.storage.upsert.assert_not_awaited()

        await mixin.append_files_history(ctx, _make_files(1, "c"))
        ctx.storage.upsert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_flush_clears_dirty_state(self) -> None:
        """After flush, dirty state is cleared — second flush is a no-op."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_files_history(ctx, _make_files(1))
        await mixin.flush_file_history(ctx)

        ctx.storage.upsert.reset_mock()
        await mixin.flush_file_history(ctx)
        ctx.storage.upsert.assert_not_awaited()
        ctx.storage.update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flush_failure_leaves_dirty_for_retry(self) -> None:
        """If flush fails, dirty state is preserved for retry on next flush."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        ctx.storage.upsert = AsyncMock(side_effect=RuntimeError("storage down"))

        await mixin.append_files_history(ctx, _make_files(1))
        await mixin.flush_file_history(ctx)

        if not mixin._fh_dirty:
            pytest.fail("Expected dirty state to be preserved after flush failure")

        # Fix storage and retry
        ctx.storage.upsert = AsyncMock(return_value=MagicMock(spec=StorageRecord))
        await mixin.flush_file_history(ctx)

        ctx.storage.upsert.assert_awaited_once()
        if mixin._fh_dirty:
            pytest.fail("Expected dirty state to be cleared after successful retry")

    @pytest.mark.asyncio
    async def test_multiple_missions_flush_independently(self) -> None:
        """Each mission's dirty state is tracked and flushed independently."""
        mixin = _ConcreteMixin()
        ctx_a = _make_context(mission_id="mission_a")
        ctx_b = _make_context(mission_id="mission_b")

        await mixin.append_files_history(ctx_a, _make_files(1, "a"))
        await mixin.append_files_history(ctx_b, _make_files(1, "b"))

        # Flush only mission_a's context
        await mixin.flush_file_history(ctx_a)

        # Both were flushed because flush iterates all dirty keys
        if mixin._fh_dirty:
            pytest.fail("Expected all dirty state to be cleared after flush")
