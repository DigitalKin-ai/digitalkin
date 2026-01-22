"""Tests for StorageStrategy lock creation and cleanup."""

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from digitalkin.models.services.storage import DataType, StorageRecord
from digitalkin.services.storage.storage_strategy import StorageStrategy


class _SimpleModel(BaseModel):
    """Minimal model for lock tests."""

    mission_id: str = Field(default="m1")
    value: str = Field(default="v")


class _InMemoryStorage(StorageStrategy):
    """Minimal concrete StorageStrategy backed by a dict."""

    def __init__(self) -> None:
        super().__init__("m1", "s1", "sv1", {"items": _SimpleModel})
        self._store_data: dict[str, StorageRecord] = {}

    async def create(
        self,
        collection: str,
        record_id: str | None,
        data: BaseModel,
        data_type: DataType = DataType.OUTPUT,
    ) -> StorageRecord:
        """Create a new record."""
        record_id = record_id or uuid4().hex
        validated_data = self._validate_data(collection, {**data} if isinstance(data, dict) else data.model_dump())
        record = self._create_storage_record(collection, record_id, validated_data, data_type)
        key = f"{collection}:{record_id}"
        self._store_data[key] = record
        self._record_lock(collection, record_id)
        return record

    async def get(self, collection: str, record_id: str) -> StorageRecord | None:
        """Get a record."""
        return self._store_data.get(f"{collection}:{record_id}")

    async def update(self, collection: str, record_id: str, data: BaseModel) -> StorageRecord | None:
        """Update a record."""
        key = f"{collection}:{record_id}"
        rec = self._store_data.get(key)
        if rec is None:
            return None
        rec.data = data
        return rec

    async def delete(self, collection: str, record_id: str) -> bool:
        """Delete a record and clean up its lock."""
        key = f"{collection}:{record_id}"
        removed = self._store_data.pop(key, None) is not None
        if removed:
            self._record_locks.pop(key, None)
        return removed

    async def list(self, collection: str) -> list[StorageRecord]:
        """List records in a collection."""
        return [r for k, r in self._store_data.items() if k.startswith(f"{collection}:")]

    async def delete_collection(self, collection: str) -> bool:
        """Delete all records in a collection and clean up locks."""
        prefix = f"{collection}:"
        keys = [k for k in self._store_data if k.startswith(prefix)]
        for k in keys:
            del self._store_data[k]
        lock_keys = [k for k in self._record_locks if k.startswith(prefix)]
        for k in lock_keys:
            del self._record_locks[k]
        return bool(keys)

    async def search(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented."""
        msg = "Search method not implemented yet."
        raise NotImplementedError(msg)

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        """Not implemented."""
        msg = "Upload method not implemented yet."
        raise NotImplementedError(msg)


class TestRecordLockAtomicity:
    """Tests for atomic lock creation via setdefault."""

    @pytest.mark.asyncio
    async def test_record_lock_returns_same_instance(self) -> None:
        """Consecutive calls for same key return the same Lock object."""
        storage = _InMemoryStorage()
        lock1 = storage._record_lock("items", "r1")
        lock2 = storage._record_lock("items", "r1")
        assert lock1 is lock2

    @pytest.mark.asyncio
    async def test_different_keys_get_different_locks(self) -> None:
        """Different collection:record_id pairs get independent locks."""
        storage = _InMemoryStorage()
        lock1 = storage._record_lock("items", "r1")
        lock2 = storage._record_lock("items", "r2")
        assert lock1 is not lock2


class TestRecordLockCleanup:
    """Tests for lock cleanup on remove and remove_collection."""

    @pytest.mark.asyncio
    async def test_remove_cleans_up_lock(self) -> None:
        """Removing a record also removes its lock entry."""
        storage = _InMemoryStorage()
        await storage.create("items", "r1", _SimpleModel(value="x"))
        assert "items:r1" in storage._record_locks

        result = await storage.delete("items", "r1")

        assert result is True
        assert "items:r1" not in storage._record_locks

    @pytest.mark.asyncio
    async def test_remove_nonexistent_keeps_lock(self) -> None:
        """Removing a nonexistent record does not remove the lock."""
        storage = _InMemoryStorage()
        # Create lock by accessing it
        storage._record_lock("items", "r1")
        assert "items:r1" in storage._record_locks

        result = await storage.delete("items", "r1")

        assert result is False
        assert "items:r1" in storage._record_locks

    @pytest.mark.asyncio
    async def test_remove_collection_cleans_up_locks(self) -> None:
        """Removing a collection removes all locks for that collection prefix."""
        storage = _InMemoryStorage()
        await storage.create("items", "r1", _SimpleModel(value="a"))
        await storage.create("items", "r2", _SimpleModel(value="b"))
        await storage.create("items", "r3", _SimpleModel(value="c"))
        assert len([k for k in storage._record_locks if k.startswith("items:")]) == 3

        result = await storage.delete_collection("items")

        assert result is True
        assert not any(k.startswith("items:") for k in storage._record_locks)

    @pytest.mark.asyncio
    async def test_remove_collection_preserves_other_collection_locks(self) -> None:
        """Removing one collection does not affect locks for other collections."""
        storage = _InMemoryStorage()
        storage.config["other"] = _SimpleModel
        await storage.create("items", "r1", _SimpleModel(value="a"))
        await storage.create("other", "r1", _SimpleModel(value="b"))

        await storage.delete_collection("items")

        assert "items:r1" not in storage._record_locks
        assert "other:r1" in storage._record_locks
