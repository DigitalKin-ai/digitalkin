"""Tests for ChatHistoryMixin caching and storage optimization."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.mixins.chat_history_mixin import ChatHistoryMixin
from digitalkin.models.services.storage import BaseMessage, BaseRole, ChatHistory
from digitalkin.services.storage.storage_strategy import StorageRecord


class _ConcreteMixin(ChatHistoryMixin):  # type: ignore[type-arg]
    """Concrete class to test ChatHistoryMixin (abstract mixin needs host class)."""


def _make_context(mission_id: str = "test_mission") -> MagicMock:
    """Build a mock ModuleContext with storage and session."""
    ctx = MagicMock()
    ctx.session.mission_id = mission_id
    ctx.storage = AsyncMock()
    ctx.storage.read = AsyncMock(return_value=None)
    ctx.storage.upsert = AsyncMock(return_value=MagicMock(spec=StorageRecord))
    ctx.storage.update = AsyncMock(return_value=MagicMock(spec=StorageRecord))
    return ctx


def _storage_record_with_history(messages: list[dict[str, Any]]) -> MagicMock:
    """Create a mock StorageRecord whose .data looks like a ChatHistory."""
    record = MagicMock(spec=StorageRecord)
    record.data = ChatHistory(messages=[BaseMessage(**m) for m in messages])
    return record


@pytest.fixture(autouse=True)
def _fresh_mixin():
    """Reset class-level caches between tests."""
    _ConcreteMixin._chat_history_cache = None
    _ConcreteMixin._persisted_keys = None
    yield
    _ConcreteMixin._chat_history_cache = None
    _ConcreteMixin._persisted_keys = None


class TestChatHistoryCache:
    """Tests for in-memory chat history caching."""

    @pytest.mark.asyncio
    async def test_load_reads_storage_once_then_caches(self) -> None:
        """Second load_chat_history call returns cached value without gRPC read."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        existing = _storage_record_with_history([{"role": "user", "content": "hello"}])
        ctx.storage.read = AsyncMock(return_value=existing)

        first = await mixin.load_chat_history(ctx)
        second = await mixin.load_chat_history(ctx)

        if first is not second:
            pytest.fail("Expected cached ChatHistory object on second call")
        ctx.storage.read.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_load_returns_empty_on_cache_miss(self) -> None:
        """When storage has no record, returns empty ChatHistory and caches it."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        ctx.storage.read = AsyncMock(return_value=None)

        history = await mixin.load_chat_history(ctx)

        if len(history.messages) != 0:
            pytest.fail(f"Expected empty messages, got {len(history.messages)}")
        # Second call should not read storage again
        await mixin.load_chat_history(ctx)
        ctx.storage.read.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_different_missions_cached_independently(self) -> None:
        """Different mission_ids get independent cache entries."""
        mixin = _ConcreteMixin()
        ctx_a = _make_context(mission_id="mission_a")
        ctx_b = _make_context(mission_id="mission_b")
        ctx_a.storage.read = AsyncMock(return_value=None)
        ctx_b.storage.read = AsyncMock(return_value=None)

        history_a = await mixin.load_chat_history(ctx_a)
        history_b = await mixin.load_chat_history(ctx_b)

        if history_a is history_b:
            pytest.fail("Different missions should have separate cache entries")


class TestAppendStorageOptimization:
    """Tests for upsert vs update optimization."""

    @pytest.mark.asyncio
    async def test_first_append_uses_upsert(self) -> None:
        """First append to a new key uses upsert_storage (record may not exist yet)."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        ctx.storage.read = AsyncMock(return_value=None)

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "hello")

        ctx.storage.upsert.assert_awaited_once()
        ctx.storage.update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_second_append_uses_update(self) -> None:
        """After first persist, subsequent appends use update_storage (1 call)."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        ctx.storage.read = AsyncMock(return_value=None)

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "first")
        await mixin.append_chat_history_message(ctx, BaseRole.ASSISTANT, "second")

        if ctx.storage.upsert.await_count != 1:
            pytest.fail(f"Expected exactly 1 upsert call, got {ctx.storage.upsert.await_count}")
        if ctx.storage.update.await_count != 1:
            pytest.fail(f"Expected exactly 1 update call, got {ctx.storage.update.await_count}")

    @pytest.mark.asyncio
    async def test_preexisting_record_uses_update_from_start(self) -> None:
        """When storage already has the record, first append uses update (not upsert)."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        existing = _storage_record_with_history([{"role": "user", "content": "old"}])
        ctx.storage.read = AsyncMock(return_value=existing)

        # load first to populate cache + persisted_keys
        await mixin.load_chat_history(ctx)
        await mixin.append_chat_history_message(ctx, BaseRole.ASSISTANT, "new")

        ctx.storage.upsert.assert_not_awaited()
        ctx.storage.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_append_accumulates_messages_in_cache(self) -> None:
        """Multiple appends accumulate in the cached ChatHistory object."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        ctx.storage.read = AsyncMock(return_value=None)

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "msg1")
        await mixin.append_chat_history_message(ctx, BaseRole.ASSISTANT, "msg2")
        await mixin.append_chat_history_message(ctx, BaseRole.USER, "msg3")

        history = await mixin.load_chat_history(ctx)
        if len(history.messages) != 3:
            pytest.fail(f"Expected 3 messages in cache, got {len(history.messages)}")
        # Storage read should only have been called once (first load)
        ctx.storage.read.assert_awaited_once()
