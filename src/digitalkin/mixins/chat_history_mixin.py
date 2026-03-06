"""Context mixins providing ergonomic access to service strategies.

This module provides mixins that wrap service strategy calls with cleaner APIs,
following Django/FastAPI patterns where context is passed explicitly to each method.
"""

import asyncio
import os
from typing import Any, Generic

from digitalkin.logger import logger
from digitalkin.mixins.callback_mixin import UserMessageMixin
from digitalkin.mixins.logger_mixin import LoggerMixin
from digitalkin.mixins.storage_mixin import StorageMixin
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.module_types import InputModelT, OutputModelT
from digitalkin.models.services.storage import BaseMessage, ChatHistory, Role

_FLUSH_THRESHOLD = int(os.environ.get("DIGITALKIN_CHAT_HISTORY_FLUSH_THRESHOLD", "10"))


class ChatHistoryMixin(UserMessageMixin, StorageMixin, LoggerMixin, Generic[InputModelT, OutputModelT]):
    """Mixin providing chat history operations through storage strategy.

    Chat histories are cached in memory after first load to avoid redundant
    gRPC reads.  Known-persisted keys use update_storage (1 call) instead of
    upsert_storage (2 calls).

    Writes are batched: messages accumulate in the cache and are flushed when
    the batch threshold is reached or flush_chat_history() is called.
    """

    CHAT_HISTORY_COLLECTION = "chat_history"
    CHAT_HISTORY_RECORD_ID = "full_chat_history"

    def _ensure_state(self) -> None:
        """Lazily initialise all mixin state once."""
        if not hasattr(self, "_ch_cache"):
            self._ch_cache: dict[str, ChatHistory] = {}
            self._ch_persisted: set[str] = set()
            self._ch_dirty: dict[str, int] = {}  # history_key -> pending count
            self._ch_flush_lock = asyncio.Lock()

    def _get_history_key(self, context: ModuleContext) -> str:
        """Get session-specific history key.

        Returns:
            Unique history key for the current session.
        """
        mission_id = context.session.mission_id or "default"
        return f"{self.CHAT_HISTORY_RECORD_ID}_{mission_id}"

    async def load_chat_history(self, context: ModuleContext) -> ChatHistory:
        """Load chat history for the current session.

        Returns cached history on subsequent calls to avoid gRPC reads.

        Args:
            context: Module context containing storage strategy

        Returns:
            Chat history object, empty if none exists or loading fails
        """
        self._ensure_state()
        history_key = self._get_history_key(context)

        if history_key in self._ch_cache:
            return self._ch_cache[history_key]

        raw = await self.read_storage(context, self.CHAT_HISTORY_COLLECTION, history_key)
        if raw is not None:
            history = ChatHistory.model_validate(raw.data)
            self._ch_persisted.add(history_key)
        else:
            history = ChatHistory(messages=[])

        self._ch_cache[history_key] = history
        return history

    async def append_chat_history_message(
        self,
        context: ModuleContext,
        role: Role,
        content: Any,
    ) -> None:
        """Append a message to chat history.

        The message is added to the in-memory cache immediately.  A storage
        write is deferred until the batch threshold is reached (default 10,
        env: DIGITALKIN_CHAT_HISTORY_FLUSH_THRESHOLD) or flush_chat_history().

        Args:
            context: Module context containing storage strategy
            role: Message role (user, assistant, system)
            content: Message content
        """
        self._ensure_state()
        history_key = self._get_history_key(context)
        chat_history = await self.load_chat_history(context)
        chat_history.messages.append(BaseMessage(role=role, content=content))

        pending = self._ch_dirty.get(history_key, 0) + 1
        self._ch_dirty[history_key] = pending

        if pending >= _FLUSH_THRESHOLD:
            await self._flush_key(context, history_key)

    async def flush_chat_history(self, context: ModuleContext) -> None:
        """Flush all dirty chat history keys to storage.

        Call at end of mission / trigger to persist buffered messages.

        Args:
            context: Module context containing storage strategy
        """
        self._ensure_state()
        for key in list(self._ch_dirty):
            await self._flush_key(context, key)

    async def _flush_key(self, context: ModuleContext, history_key: str) -> None:
        """Persist a single dirty history key to storage."""
        async with self._ch_flush_lock:
            if history_key not in self._ch_dirty:
                return

            chat_history = self._ch_cache.get(history_key)
            if chat_history is None:
                self._ch_dirty.pop(history_key, None)
                return

            self.log_debug(context, "Flushing chat history for session: %s", history_key)
            try:
                data = chat_history.model_dump()
                if history_key in self._ch_persisted:
                    await self.update_storage(context, self.CHAT_HISTORY_COLLECTION, history_key, data)
                else:
                    await self.upsert_storage(context, self.CHAT_HISTORY_COLLECTION, history_key, data)
                    self._ch_persisted.add(history_key)
            except Exception:
                logger.warning("Failed to flush chat history for %s, continuing", history_key, exc_info=True)
                return  # leave dirty for retry on next flush

            self._ch_dirty.pop(history_key, None)

    async def save_send_message(
        self,
        context: ModuleContext,
        output: OutputModelT,
        role: Role,
    ) -> None:
        """Save output to chat history and send response to the module request.

        Args:
            context: Module context containing storage strategy
            role: Message role (user, assistant, system)
            output: Message content as Pydantic Class
        """
        await self.append_chat_history_message(context=context, role=role, content=output.root)
        await self.send_message(context=context, output=output)
