"""Context mixins providing ergonomic access to service strategies.

This module provides mixins that wrap service strategy calls with cleaner APIs,
following Django/FastAPI patterns where context is passed explicitly to each method.
"""

from typing import Any, Generic

from digitalkin.logger import logger
from digitalkin.mixins.callback_mixin import UserMessageMixin
from digitalkin.mixins.logger_mixin import LoggerMixin
from digitalkin.mixins.storage_mixin import StorageMixin
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.module_types import InputModelT, OutputModelT
from digitalkin.models.services.storage import BaseMessage, ChatHistory, Role


class ChatHistoryMixin(UserMessageMixin, StorageMixin, LoggerMixin, Generic[InputModelT, OutputModelT]):
    """Mixin providing chat history operations through storage strategy.

    This mixin provides a higher-level API for managing chat history,
    using the storage strategy as the underlying persistence mechanism.

    Chat histories are cached in memory after first load to avoid
    redundant gRPC reads. Known-persisted keys allow using update_storage
    (1 gRPC call) instead of upsert_storage (2 gRPC calls).
    """

    CHAT_HISTORY_COLLECTION = "chat_history"
    CHAT_HISTORY_RECORD_ID = "full_chat_history"
    _chat_history_cache: dict[str, ChatHistory] | None = None
    _persisted_keys: set[str] | None = None

    def _get_history_key(self, context: ModuleContext) -> str:
        """Get session-specific history key.

        Args:
            context: Module context containing session information

        Returns:
            Unique history key for the current session
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
        if self._chat_history_cache is None:
            self._chat_history_cache = {}
        if self._persisted_keys is None:
            self._persisted_keys = set()

        history_key = self._get_history_key(context)
        if history_key in self._chat_history_cache:
            return self._chat_history_cache[history_key]

        if (raw_history := await self.read_storage(context, self.CHAT_HISTORY_COLLECTION, history_key)) is not None:
            history = ChatHistory.model_validate(raw_history.data)
            self._chat_history_cache[history_key] = history
            self._persisted_keys.add(history_key)
            return history

        history = ChatHistory(messages=[])
        self._chat_history_cache[history_key] = history
        return history

    async def append_chat_history_message(
        self,
        context: ModuleContext,
        role: Role,
        content: Any,
    ) -> None:
        """Append a message to chat history.

        Uses cached history to avoid gRPC reads. After first persist,
        uses update_storage (1 call) instead of upsert_storage (2 calls).

        Storage failures are caught and logged — they should not crash the
        task since chat history is non-critical compared to the main output.

        Args:
            context: Module context containing storage strategy
            role: Message role (user, assistant, system)
            content: Message content
        """
        if self._persisted_keys is None:
            self._persisted_keys = set()

        history_key = self._get_history_key(context)
        chat_history = await self.load_chat_history(context)
        chat_history.messages.append(BaseMessage(role=role, content=content))
        self.log_debug(context, f"Persisting chat history for session: {history_key}")

        try:
            if history_key in self._persisted_keys:
                await self.update_storage(context, self.CHAT_HISTORY_COLLECTION, history_key, chat_history.model_dump())
            else:
                await self.upsert_storage(context, self.CHAT_HISTORY_COLLECTION, history_key, chat_history.model_dump())
                self._persisted_keys.add(history_key)
        except Exception:
            logger.warning("Failed to persist chat history for session %s, continuing", history_key, exc_info=True)

    async def save_send_message(
        self,
        context: ModuleContext,
        output: OutputModelT,
        role: Role,
    ) -> None:
        """Save the output message to the chat history and send a response to the Module request.

        Args:
            context: Module context containing storage strategy
            role: Message role (user, assistant, system)
            output: Message content as Pydantic Class
        """
        await self.append_chat_history_message(context=context, role=role, content=output.root)
        await self.send_message(context=context, output=output)
