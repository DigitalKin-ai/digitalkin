"""Tests for StorageStrategy lock creation and cleanup."""

import pytest
from pydantic import BaseModel, Field

from digitalkin.models.services.storage import Visibility
from digitalkin.services.storage.storage_strategy import StorageRecord, StorageStrategy


class _SimpleModel(BaseModel):
    """Minimal model for lock tests."""

    value: str = Field(default="v")


class _InMemoryStorage(StorageStrategy):
    """Minimal concrete StorageStrategy backed by a dict."""

    def __init__(self) -> None:
        super().__init__("missions:m1", "s1", "setup_versions:sv1", {"items": _SimpleModel})
        self._store_data: dict[str, StorageRecord] = {}

    @staticmethod
    def _key(context: str, collection: str, record_id: str) -> str:
        return f"{context}|{collection}:{record_id}"

    async def _store(self, record: StorageRecord) -> StorageRecord:
        self._store_data[self._key(record.context, record.collection, record.record_id)] = record
        return record

    async def _read(
        self, collection: str, record_id: str, context: str, storage_id: str = ""
    ) -> StorageRecord | None:
        return self._store_data.get(self._key(context, collection, record_id))

    async def _update(
        self, collection: str, record_id: str, data: BaseModel, context: str
    ) -> StorageRecord | None:
        key = self._key(context, collection, record_id)
        rec = self._store_data.get(key)
        if rec is None:
            return None
        rec.data = data
        return rec

    async def _remove(self, collection: str, record_id: str, context: str) -> bool:
        return self._store_data.pop(self._key(context, collection, record_id), None) is not None

    async def _list(
        self,
        collection: str,
        context: str,
        visibilities: list[Visibility] | None = None,
        record_id: str = "",
        limit: int = 0,
        offset: int = 0,
    ) -> list[StorageRecord]:
        prefix = f"{context}|{collection}:"
        return [r for k, r in self._store_data.items() if k.startswith(prefix)]

    async def _remove_collection(self, collection: str, context: str, record_id: str = "") -> bool:
        prefix = f"{context}|{collection}:"
        keys = [k for k in self._store_data if k.startswith(prefix)]
        for k in keys:
            del self._store_data[k]
        return bool(keys)


_MISSION_LOCK_PREFIX = "missions:m1|items:"


class TestRecordLockAtomicity:
    """Tests for atomic lock creation via setdefault."""

    @pytest.mark.asyncio
    async def test_record_lock_returns_same_instance(self) -> None:
        """Consecutive calls for same key return the same Lock object."""
        storage = _InMemoryStorage()
        lock1 = storage._record_lock("missions:m1", "items", "r1")
        lock2 = storage._record_lock("missions:m1", "items", "r1")
        assert lock1 is lock2

    @pytest.mark.asyncio
    async def test_different_keys_get_different_locks(self) -> None:
        """Different collection:record_id pairs get independent locks."""
        storage = _InMemoryStorage()
        lock1 = storage._record_lock("missions:m1", "items", "r1")
        lock2 = storage._record_lock("missions:m1", "items", "r2")
        assert lock1 is not lock2

    @pytest.mark.asyncio
    async def test_different_contexts_get_different_locks(self) -> None:
        """Same collection:record_id under different contexts get independent locks."""
        storage = _InMemoryStorage()
        lock1 = storage._record_lock("missions:m1", "items", "r1")
        lock2 = storage._record_lock("setup_versions:sv1", "items", "r1")
        assert lock1 is not lock2


class TestRecordLockCleanup:
    """Tests for lock cleanup on remove and remove_collection."""

    @pytest.mark.asyncio
    async def test_remove_cleans_up_lock(self) -> None:
        """Removing a record also removes its lock entry."""
        storage = _InMemoryStorage()
        await storage.store("items", "r1", {"value": "x"})
        assert f"{_MISSION_LOCK_PREFIX}r1" in storage._record_locks

        result = await storage.remove("items", "r1")

        assert result is True
        assert f"{_MISSION_LOCK_PREFIX}r1" not in storage._record_locks

    @pytest.mark.asyncio
    async def test_remove_nonexistent_keeps_lock(self) -> None:
        """Removing a nonexistent record does not remove the lock."""
        storage = _InMemoryStorage()
        # Create lock by accessing it
        storage._record_lock("missions:m1", "items", "r1")
        assert f"{_MISSION_LOCK_PREFIX}r1" in storage._record_locks

        result = await storage.remove("items", "r1")

        assert result is False
        assert f"{_MISSION_LOCK_PREFIX}r1" in storage._record_locks

    @pytest.mark.asyncio
    async def test_remove_collection_cleans_up_locks(self) -> None:
        """Removing a collection removes all locks for that collection prefix."""
        storage = _InMemoryStorage()
        await storage.store("items", "r1", {"value": "a"})
        await storage.store("items", "r2", {"value": "b"})
        await storage.store("items", "r3", {"value": "c"})
        assert len([k for k in storage._record_locks if k.startswith(_MISSION_LOCK_PREFIX)]) == 3

        result = await storage.remove_collection("items")

        assert result is True
        assert not any(k.startswith(_MISSION_LOCK_PREFIX) for k in storage._record_locks)

    @pytest.mark.asyncio
    async def test_remove_collection_preserves_other_collection_locks(self) -> None:
        """Removing one collection does not affect locks for other collections."""
        storage = _InMemoryStorage()
        storage.config["other"] = _SimpleModel
        await storage.store("items", "r1", {"value": "a"})
        await storage.store("other", "r1", {"value": "b"})

        await storage.remove_collection("items")

        assert f"{_MISSION_LOCK_PREFIX}r1" not in storage._record_locks
        assert "missions:m1|other:r1" in storage._record_locks
