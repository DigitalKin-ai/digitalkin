"""Context mixins providing ergonomic access to service strategies.

This module provides mixins that wrap service strategy calls with cleaner APIs,
following Django/FastAPI patterns where context is passed explicitly to each method.
"""

import asyncio
import os

from digitalkin.logger import logger
from digitalkin.mixins.logger_mixin import LoggerMixin
from digitalkin.mixins.storage_mixin import StorageMixin
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.services.storage import FileHistory, FileModel


class FileHistoryMixin(StorageMixin, LoggerMixin):
    """Mixin providing file history operations through storage strategy.

    File histories are cached in memory after first load to avoid redundant
    gRPC reads. Known-persisted keys use update_storage (1 call) instead of
    upsert_storage (2 calls).

    Writes are batched: files accumulate in the cache and are flushed when
    the batch threshold is reached or flush_file_history() is called.
    """

    FILE_HISTORY_COLLECTION = "file_history"
    FILE_HISTORY_RECORD_ID = "full_file_history"

    # Sentinel for lazy init — guards against broken super().__init__() chains
    _fh_cache: dict[str, FileHistory] = None  # type: ignore[assignment]
    _fh_persisted: set[str]
    _fh_dirty: dict[str, int]
    _fh_flush_locks: dict[str, asyncio.Lock]
    _fh_flush_threshold: int

    def __init__(self) -> None:
        """Initialize file history state."""
        super().__init__()
        self._ensure_fh_state()

    def _ensure_fh_state(self) -> None:
        """Idempotent state initialization (defensive against broken __init__ chains)."""
        if self._fh_cache is not None:
            return
        self._fh_cache = {}
        self._fh_persisted = set()
        self._fh_dirty = {}
        self._fh_flush_locks = {}
        self._fh_flush_threshold = int(os.environ.get("DIGITALKIN_FILE_HISTORY_FLUSH_THRESHOLD", "10"))

    def _get_fh_history_key(self, context: ModuleContext) -> str:
        """Get session-specific history key.

        Returns:
            Unique history key for the current session.
        """
        mission_id = context.session.mission_id or "default"
        return f"{self.FILE_HISTORY_RECORD_ID}_{mission_id}"

    async def load_file_history(self, context: ModuleContext) -> FileHistory:
        """Load file history for the current session.

        Returns cached history on subsequent calls to avoid gRPC reads.

        Args:
            context: Module context containing storage strategy.

        Returns:
            File history object, empty if none exists or loading fails.
        """
        self._ensure_fh_state()
        history_key = self._get_fh_history_key(context)

        if history_key in self._fh_cache:
            return self._fh_cache[history_key]

        raw = await self.get_storage(context, self.FILE_HISTORY_COLLECTION, history_key)
        if raw is not None:
            history = FileHistory.model_validate(raw.data)
            self._fh_persisted.add(history_key)
        else:
            history = FileHistory(files=[])

        self._fh_cache[history_key] = history
        return history

    async def append_files_history(self, context: ModuleContext, files: list[FileModel]) -> None:
        """Append files to file history.

        Files are added to the in-memory cache immediately. A storage
        write is deferred until the batch threshold is reached (default 10,
        env: DIGITALKIN_FILE_HISTORY_FLUSH_THRESHOLD) or flush_file_history().

        Args:
            context: Module context containing storage strategy.
            files: List of file models to append.
        """
        history_key = self._get_fh_history_key(context)
        file_history = await self.load_file_history(context)
        file_history.files.extend(files)

        pending = self._fh_dirty.get(history_key, 0) + 1
        self._fh_dirty[history_key] = pending

        if pending >= self._fh_flush_threshold:
            await self._flush_fh_key(context, history_key)

    async def flush_file_history(self, context: ModuleContext) -> None:
        """Flush the current mission's dirty file history to storage.

        Only flushes the key belonging to context's mission_id, preventing
        cross-mission contamination when handlers are shared.

        Args:
            context: Module context containing storage strategy.
        """
        self._ensure_fh_state()
        history_key = self._get_fh_history_key(context)
        if history_key in self._fh_dirty:
            await self._flush_fh_key(context, history_key)

    async def _flush_fh_key(self, context: ModuleContext, history_key: str) -> None:
        """Persist a single dirty history key to storage."""
        lock = self._fh_flush_locks.setdefault(history_key, asyncio.Lock())
        async with lock:
            if history_key not in self._fh_dirty:
                return

            file_history = self._fh_cache.get(history_key)
            if file_history is None:
                self._fh_dirty.pop(history_key, None)
                return

            self.log_debug(context, "Flushing file history for session: %s", history_key)
            try:
                data = file_history.model_dump()
                if history_key in self._fh_persisted:
                    await self.update_storage(context, self.FILE_HISTORY_COLLECTION, history_key, data)
                else:
                    await self.upsert_storage(context, self.FILE_HISTORY_COLLECTION, history_key, data)
                    self._fh_persisted.add(history_key)
            except Exception:
                logger.warning("Failed to flush file history for %s, continuing", history_key, exc_info=True)
                return  # leave dirty for retry on next flush

            self._fh_dirty.pop(history_key, None)

    def clear_fh_mission_cache(self, context: ModuleContext) -> None:
        """Remove a mission's entries from in-memory caches after flush.

        Args:
            context: Module context identifying the mission to clear.
        """
        self._ensure_fh_state()
        history_key = self._get_fh_history_key(context)
        self._fh_cache.pop(history_key, None)
        self._fh_persisted.discard(history_key)
        self._fh_dirty.pop(history_key, None)
        self._fh_flush_locks.pop(history_key, None)
