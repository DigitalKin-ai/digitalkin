"""Tests for ChatHistoryMixin caching and storage optimization."""

from typing import Any, ClassVar, Literal
from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.mixins.chat_history_mixin import ChatHistoryMixin
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.services.storage import BaseMessage, BaseRole, ChatHistory
from digitalkin.modules.trigger_handler import TriggerHandler
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
    """Tests for upsert vs update optimization with batched flush."""

    @pytest.mark.asyncio
    async def test_first_append_uses_upsert(self) -> None:
        """First append to a new key uses upsert_storage after flush."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "hello")
        await mixin.flush_chat_history(ctx)

        ctx.storage.upsert.assert_awaited_once()
        ctx.storage.update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_second_append_uses_update(self) -> None:
        """After first persist, subsequent appends use update_storage (1 call)."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "first")
        await mixin.flush_chat_history(ctx)
        await mixin.append_chat_history_message(ctx, BaseRole.ASSISTANT, "second")
        await mixin.flush_chat_history(ctx)

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

        await mixin.load_chat_history(ctx)
        await mixin.append_chat_history_message(ctx, BaseRole.ASSISTANT, "new")
        await mixin.flush_chat_history(ctx)

        ctx.storage.upsert.assert_not_awaited()
        ctx.storage.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_append_accumulates_messages_in_cache(self) -> None:
        """Multiple appends accumulate in the cached ChatHistory object."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "msg1")
        await mixin.append_chat_history_message(ctx, BaseRole.ASSISTANT, "msg2")
        await mixin.append_chat_history_message(ctx, BaseRole.USER, "msg3")

        history = await mixin.load_chat_history(ctx)
        if len(history.messages) != 3:
            pytest.fail(f"Expected 3 messages in cache, got {len(history.messages)}")
        ctx.storage.read.assert_awaited_once()


class TestBatchingBehavior:
    """Tests for batched flush behavior."""

    @pytest.mark.asyncio
    async def test_append_does_not_write_below_threshold(self) -> None:
        """Messages below threshold are buffered, not written to storage."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "hello")

        ctx.storage.upsert.assert_not_awaited()
        ctx.storage.update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_threshold_triggers_flush(self) -> None:
        """Reaching the threshold auto-flushes to storage."""
        mixin = _ConcreteMixin()
        mixin._ch_flush_threshold = 3
        ctx = _make_context()

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "1")
        await mixin.append_chat_history_message(ctx, BaseRole.USER, "2")
        ctx.storage.upsert.assert_not_awaited()

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "3")
        ctx.storage.upsert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_flush_clears_dirty_state(self) -> None:
        """After flush, dirty state is cleared — second flush is a no-op."""
        mixin = _ConcreteMixin()
        ctx = _make_context()

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "hello")
        await mixin.flush_chat_history(ctx)

        ctx.storage.upsert.reset_mock()
        await mixin.flush_chat_history(ctx)
        ctx.storage.upsert.assert_not_awaited()
        ctx.storage.update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flush_failure_leaves_dirty_for_retry(self) -> None:
        """If flush fails, dirty state is preserved for retry on next flush."""
        mixin = _ConcreteMixin()
        ctx = _make_context()
        ctx.storage.upsert = AsyncMock(side_effect=RuntimeError("storage down"))

        await mixin.append_chat_history_message(ctx, BaseRole.USER, "hello")
        await mixin.flush_chat_history(ctx)

        # Dirty state preserved
        if not mixin._ch_dirty:
            pytest.fail("Expected dirty state to be preserved after flush failure")

        # Fix storage and retry
        ctx.storage.upsert = AsyncMock(return_value=MagicMock(spec=StorageRecord))
        await mixin.flush_chat_history(ctx)

        ctx.storage.upsert.assert_awaited_once()
        if mixin._ch_dirty:
            pytest.fail("Expected dirty state to be cleared after successful retry")


# ---------------------------------------------------------------------------
# Tests exercising the real user pattern: TriggerHandler subclass
# ---------------------------------------------------------------------------

class _FakeInput:
    protocol: Literal["test"] = "test"


class _FakeOutput:
    pass


class _GoodTrigger(TriggerHandler):
    """User handler that correctly calls super().__init__()."""

    protocol = "test"
    description: ClassVar[str] = ""
    input_format = _FakeInput
    output_format = _FakeOutput

    def __init__(self, context: ModuleContext) -> None:
        super().__init__(context)
        self.custom_attr = "ok"

    async def handle(self, input_data: Any, setup_data: Any, context: ModuleContext) -> None:
        pass  # pragma: no cover


class _BadTrigger(TriggerHandler):
    """User handler that forgets super().__init__() — the exact production bug."""

    protocol = "test"
    description: ClassVar[str] = ""
    input_format = _FakeInput
    output_format = _FakeOutput

    def __init__(self, context: ModuleContext) -> None:
        # Deliberately missing super().__init__(context)
        self.custom_attr = "ok"

    async def handle(self, input_data: Any, setup_data: Any, context: ModuleContext) -> None:
        pass  # pragma: no cover


class _NoInitTrigger(TriggerHandler):
    """User handler that doesn't override __init__ at all (relies on inherited)."""

    protocol = "test"
    description: ClassVar[str] = ""
    input_format = _FakeInput
    output_format = _FakeOutput

    async def handle(self, input_data: Any, setup_data: Any, context: ModuleContext) -> None:
        pass  # pragma: no cover


def _make_mock_context() -> MagicMock:
    """Build a minimal mock ModuleContext for TriggerHandler instantiation."""
    ctx = MagicMock()
    ctx.session.mission_id = "test_mission"
    ctx.storage = AsyncMock()
    ctx.storage.read = AsyncMock(return_value=None)
    ctx.storage.upsert = AsyncMock(return_value=MagicMock(spec=StorageRecord))
    ctx.storage.update = AsyncMock(return_value=MagicMock(spec=StorageRecord))
    return ctx


class TestTriggerHandlerMixinInit:
    """Verify ChatHistoryMixin works through TriggerHandler — the real user path."""

    @pytest.mark.asyncio
    async def test_good_trigger_init_sets_cache(self) -> None:
        """Handler that calls super().__init__() gets mixin state via __init__ chain."""
        handler = _GoodTrigger(_make_mock_context())
        assert handler._ch_cache is not None
        assert handler.custom_attr == "ok"

    @pytest.mark.asyncio
    async def test_bad_trigger_lazy_init_on_load(self) -> None:
        """Handler that forgets super().__init__() still works via lazy init."""
        handler = _BadTrigger(_make_mock_context())
        ctx = _make_mock_context()

        # _ch_cache is still None (sentinel) because __init__ chain was broken
        history = await handler.load_chat_history(ctx)
        # Lazy init kicked in — no crash, returns empty history
        assert len(history.messages) == 0
        assert handler._ch_cache is not None

    @pytest.mark.asyncio
    async def test_bad_trigger_append_and_flush(self) -> None:
        """Full append+flush cycle works even with broken __init__ chain."""
        handler = _BadTrigger(_make_mock_context())
        ctx = _make_mock_context()

        await handler.append_chat_history_message(ctx, BaseRole.USER, "hello")
        await handler.flush_chat_history(ctx)

        ctx.storage.upsert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_init_trigger_works(self) -> None:
        """Handler that doesn't override __init__ inherits working chain."""
        handler = _NoInitTrigger(_make_mock_context())
        ctx = _make_mock_context()

        await handler.append_chat_history_message(ctx, BaseRole.USER, "test")
        history = await handler.load_chat_history(ctx)
        assert len(history.messages) == 1
