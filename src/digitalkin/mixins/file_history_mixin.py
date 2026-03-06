"""Context mixins providing ergonomic access to service strategies.

This module provides mixins that wrap service strategy calls with cleaner APIs,
following Django/FastAPI patterns where context is passed explicitly to each method.
"""

from digitalkin.mixins.logger_mixin import LoggerMixin
from digitalkin.mixins.storage_mixin import StorageMixin
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.services.storage import FileHistory, FileModel


class FileHistoryMixin(StorageMixin, LoggerMixin):
    """Mixin providing File history operations through storage strategy.

    This mixin provides a higher-level API for managing File history,
    using the storage strategy as the underlying persistence mechanism.

    File histories are cached in memory after first load to avoid redundant
    gRPC reads.
    """

    FILE_HISTORY_COLLECTION = "file_history"
    FILE_HISTORY_RECORD_ID = "full_file_history"

    def _ensure_state(self) -> None:
        """Lazily initialise all mixin state once."""
        if not hasattr(self, "_fh_cache"):
            self._fh_cache: dict[str, FileHistory] = {}

    def _get_history_key(self, context: ModuleContext) -> str:
        """Get session-specific history key.

        Args:
            context: Module context containing session information

        Returns:
            Unique history key for the current session
        """
        mission_id = context.session.mission_id or "default"
        return f"{self.FILE_HISTORY_RECORD_ID}_{mission_id}"

    async def load_file_history(self, context: ModuleContext) -> FileHistory:
        """Load File history for the current session.

        Returns cached history on subsequent calls to avoid gRPC reads.

        Args:
            context: Module context containing storage strategy

        Returns:
            File history object, empty if none exists or loading fails
        """
        self._ensure_state()
        history_key = self._get_history_key(context)

        if history_key in self._fh_cache:
            return self._fh_cache[history_key]

        try:
            record = await self.read_storage(
                context,
                self.FILE_HISTORY_COLLECTION,
                history_key,
            )
            history = FileHistory.model_validate(record.data) if record and record.data else FileHistory(files=[])
        except Exception as e:
            self.log_warning(context, "Failed to load File history: %s", e)
            history = FileHistory(files=[])

        self._fh_cache[history_key] = history
        return history

    async def append_files_history(self, context: ModuleContext, files: list[FileModel]) -> None:
        """Append a message to File history.

        Args:
            context: Module context containing storage strategy
            files: list of files model

        Raises:
            StorageServiceError: If history update fails
        """
        history_key = self._get_history_key(context)
        file_history = await self.load_file_history(context)
        file_history.files.extend(files)
        self.log_debug(context, "Upserting file history for session: %s", history_key)
        await self.upsert_storage(context, self.FILE_HISTORY_COLLECTION, history_key, file_history.model_dump())
