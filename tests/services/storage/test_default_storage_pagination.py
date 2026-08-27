"""storage_id stamping and record-scoped listing/removal on the local strategy."""

from pathlib import Path

import pytest
from pydantic import BaseModel

from digitalkin.models.services.storage import Visibility
from digitalkin.services.storage.default_storage import DefaultStorage


class _Payload(BaseModel):
    v: int


@pytest.fixture
def storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DefaultStorage:
    """A file-backed local storage rooted in a throwaway directory.

    DefaultStorage builds its path as ``<mission_id>_<storage_file_path>.json``, which stays
    relative however the argument is written — so the cwd is what actually keeps it out of the repo.
    """
    monkeypatch.chdir(tmp_path)
    return DefaultStorage("missions:m1", "setups:s1", "setup_versions:sv1", {"c": _Payload})


class TestStorageId:
    """Every stored record gets an addressable, unique storage_id."""

    async def test_store_stamps_a_prefixed_storage_id(self, storage: DefaultStorage) -> None:
        record = await storage.store("c", "r0", {"v": 0})
        assert record.storage_id.startswith("storage:")

    async def test_storage_ids_are_unique_per_record(self, storage: DefaultStorage) -> None:
        first = await storage.store("c", "r0", {"v": 0})
        second = await storage.store("c", "r1", {"v": 1})
        assert first.storage_id != second.storage_id

    async def test_read_accepts_the_matching_storage_id(self, storage: DefaultStorage) -> None:
        stored = await storage.store("c", "r0", {"v": 0})
        assert await storage.read("c", "r0", storage_id=stored.storage_id) is not None

    async def test_read_rejects_a_mismatched_storage_id(self, storage: DefaultStorage) -> None:
        """Addressing a revision that isn't there reads as absent, not as the wrong record."""
        await storage.store("c", "r0", {"v": 0})
        assert await storage.read("c", "r0", storage_id="storage:not-this-one") is None


class TestListPagination:
    """list() gained a record_id filter plus limit/offset."""

    async def test_limit_and_offset_window_the_results(self, storage: DefaultStorage) -> None:
        for i in range(5):
            await storage.store("c", f"r{i}", {"v": i})

        assert [r.record_id for r in await storage.list("c", limit=2, offset=1)] == ["r1", "r2"]

    async def test_record_id_narrows_to_one_record(self, storage: DefaultStorage) -> None:
        for i in range(3):
            await storage.store("c", f"r{i}", {"v": i})

        assert [r.record_id for r in await storage.list("c", record_id="r1")] == ["r1"]

    async def test_record_id_does_not_match_on_prefix(self, storage: DefaultStorage) -> None:
        """ "r1" must not drag in "r10" — the key sweep is prefix-based underneath."""
        await storage.store("c", "r1", {"v": 1})
        await storage.store("c", "r10", {"v": 10})

        assert [r.record_id for r in await storage.list("c", record_id="r1")] == ["r1"]

    async def test_visibility_filter_still_applies(self, storage: DefaultStorage) -> None:
        await storage.store("c", "pub", {"v": 0}, visibility=Visibility.PUBLIC)
        await storage.store("c", "priv", {"v": 1}, visibility=Visibility.PRIVATE)

        found = await storage.list("c", visibilities=[Visibility.PUBLIC])
        assert [r.record_id for r in found] == ["pub"]


class TestRecordScopedRemoval:
    """remove_collection() can now delete a single record instead of the whole collection."""

    async def test_record_id_removes_only_that_record(self, storage: DefaultStorage) -> None:
        for i in range(3):
            await storage.store("c", f"r{i}", {"v": i})

        assert await storage.remove_collection("c", record_id="r0") is True
        assert sorted(r.record_id for r in await storage.list("c")) == ["r1", "r2"]

    async def test_no_record_id_still_wipes_everything(self, storage: DefaultStorage) -> None:
        for i in range(3):
            await storage.store("c", f"r{i}", {"v": i})

        assert await storage.remove_collection("c") is True
        assert await storage.list("c") == []

    async def test_record_scoped_removal_does_not_evict_sibling_locks(self, storage: DefaultStorage) -> None:
        """Dropping "r1"'s lock must leave "r10"'s in place — a startswith sweep would not."""
        await storage.store("c", "r1", {"v": 1})
        await storage.store("c", "r10", {"v": 10})
        sibling_key = "missions:m1|c:r10"
        assert sibling_key in storage._record_locks

        await storage.remove_collection("c", record_id="r1")

        assert "missions:m1|c:r1" not in storage._record_locks
        assert sibling_key in storage._record_locks
