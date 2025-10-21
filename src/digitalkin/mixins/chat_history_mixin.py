"""Context mixins providing ergonomic access to service strategies.

This module provides mixins that wrap service strategy calls with cleaner APIs,
following Django/FastAPI patterns where context is passed explicitly to each method.
"""

from typing import Any, Generic

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
    """

    CHAT_HISTORY_COLLECTION = "chat_history"
    CHAT_HISTORY_RECORD_ID = "full_chat_history"

    def _get_history_key(self, context: ModuleContext) -> str:
        """Get session-specific history key.

        Args:
            context: Module context containing session information

        Returns:
            Unique history key for the current session
        """
        mission_id = getattr(context.session, "mission_id", None) or "default"
        return f"{self.CHAT_HISTORY_RECORD_ID}_{mission_id}"

    def load_chat_history(self, context: ModuleContext) -> ChatHistory:
        """Load chat history for the current session.

        Args:
            context: Module context containing storage strategy

        Returns:
            Chat history object, empty if none exists or loading fails
        """
        history_key = self._get_history_key(context)

        if (raw_history := self.read_storage(context, self.CHAT_HISTORY_COLLECTION, history_key)) is not None:
            return ChatHistory.model_validate(raw_history.data)
        return ChatHistory(messages=[])

    def append_chat_history_message(
        self,
        context: ModuleContext,
        role: Role,
        content: Any,  # noqa: ANN401
    ) -> None:
        """Append a message to chat history.

        Args:
            context: Module context containing storage strategy
            role: Message role (user, assistant, system)
            content: Message content

        Raises:
            StorageServiceError: If history update fails
        """
        history_key = self._get_history_key(context)
        chat_history = self.load_chat_history(context)

        chat_history.messages.append(BaseMessage(role=role, content=content))
        if len(chat_history.messages) == 1:
            # Create new record
            self.log_debug(context, f"Creating new chat history for session: {history_key}")
            self.store_storage(
                context,
                self.CHAT_HISTORY_COLLECTION,
                history_key,
                chat_history.model_dump(),
                data_type="OUTPUT",
            )
        else:
            self.log_debug(context, f"Updating chat history for session: {history_key}")
            self.update_storage(
                context,
                self.CHAT_HISTORY_COLLECTION,
                history_key,
                chat_history.model_dump(),
            )

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
        # TO-DO: we should define a default output message type to ease user experience
        self.append_chat_history_message(context=context, role=role, content=output.root)
        await self.send_message(context=context, output=output)
