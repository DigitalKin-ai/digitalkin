"""This module implements the default storage strategy."""

import datetime
import json
import tempfile
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from digitalkin.logger import logger
from digitalkin.models.services.storage import DataType
from digitalkin.services.storage.storage_strategy import (
    StorageRecord,
    StorageStrategy,
)


class DefaultStorage(StorageStrategy):
    """Persist records in a local JSON file for quick local development.

    File format: a JSON object of
      { "<collection>:<record_id>": { ... StorageRecord fields ... },
    """

    @staticmethod
    def _json_default(o: Any) -> str:
        """JSON serializer for non-standard types (datetime → ISO).

        Args:
            o: The object to serialize

        Returns:
            str: The serialized object

        Raises:
            TypeError: If the object is not serializable
        """
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        msg = f"Type {o.__class__.__name__} not serializable"
        raise TypeError(msg)

    def _load_from_file(self) -> dict[str, StorageRecord]:
        """Load storage data from the file.

        Returns:
            A dictionary containing the loaded storage records
        """
        if not self.storage_file.exists():
            return {}

        try:  # noqa: PLW0717
            raw = json.loads(self.storage_file.read_text(encoding="utf-8"))
            out: dict[str, StorageRecord] = {}

            for key, rd in raw.items():
                # rd is a dict with the StorageRecord fields
                model_cls = self.config.get(rd["collection"])
                if not model_cls:
                    logger.warning("No model for collection '%s'", rd["collection"])
                    continue
                data_model = model_cls.model_validate(rd["data"])
                rec = StorageRecord(
                    context=rd["context"],
                    collection=rd["collection"],
                    record_id=rd["record_id"],
                    data=data_model,
                    data_type=DataType[rd["data_type"]],
                    creation_date=datetime.datetime.fromisoformat(rd["creation_date"])
                    if rd.get("creation_date")
                    else None,
                    update_date=datetime.datetime.fromisoformat(rd["update_date"]) if rd.get("update_date") else None,
                )
                out[key] = rec
        except Exception:
            logger.exception("Failed to load default storage file")
            return {}
        return out

    def _save_to_file(self) -> None:
        """Atomically write `self.storage` back to disk as JSON."""
        self.storage_file.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            delete=False,
            dir=str(self.storage_file.parent),
            suffix=".tmp",
        ) as temp:
            try:
                # Convert storage to a serializable format
                serial: dict[str, dict] = {}
                for key, record in self.storage.items():
                    serial[key] = {
                        "context": record.context,
                        "collection": record.collection,
                        "record_id": record.record_id,
                        "data_type": record.data_type.name,
                        "data": record.data.model_dump(),
                        "creation_date": record.creation_date.isoformat() if record.creation_date else None,
                        "update_date": record.update_date.isoformat() if record.update_date else None,
                    }
                json.dump(serial, temp, indent=2, default=self._json_default)
                temp.flush()
                Path(temp.name).replace(self.storage_file)
            except Exception:
                logger.exception("Unexpected error saving storage")

    async def _store(self, record: StorageRecord) -> StorageRecord:
        """Store a new record in the database and persist to file.

        Args:
            record: The record to store

        Returns:
            str: The ID of the new record

        Raises:
            ValueError: If the record already exists
        """
        key = self._key(record.context, record.collection, record.record_id)
        if key in self.storage:
            msg = f"Document {key!r} already exists"
            raise ValueError(msg)
        now = datetime.datetime.now(datetime.timezone.utc)
        record.creation_date = now
        record.update_date = now
        self.storage[key] = record
        self._save_to_file()
        logger.debug("Created %s", key)
        return record

    @staticmethod
    def _key(context: str, collection: str, record_id: str) -> str:
        return f"{context}|{collection}:{record_id}"

    async def _read(self, collection: str, record_id: str, context: str) -> StorageRecord | None:
        """Get a record from the database scoped to a specific context.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record
            context: Owner context scoping the lookup.

        Returns:
            StorageRecord: The corresponding record
        """
        return self.storage.get(self._key(context, collection, record_id))

    async def _update(self, collection: str, record_id: str, data: BaseModel, context: str) -> StorageRecord | None:
        """Update a record in the database scoped to a specific context.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record
            data: The data to modify
            context: Owner context scoping the update.

        Returns:
            StorageRecord: The modified record
        """
        key = self._key(context, collection, record_id)
        rec = self.storage.get(key)
        if not rec:
            return None
        rec.data = data
        rec.update_date = datetime.datetime.now(datetime.timezone.utc)
        self._save_to_file()
        logger.debug("Modified %s", key)
        return rec

    async def _remove(self, collection: str, record_id: str, context: str) -> bool:
        """Delete a record from the database scoped to a specific context.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record
            context: Owner context scoping the deletion.

        Returns:
            bool: True if the record was removed, False otherwise
        """
        key = self._key(context, collection, record_id)
        if key not in self.storage:
            return False
        del self.storage[key]
        self._save_to_file()
        logger.debug("Removed %s", key)
        return True

    async def _list(self, collection: str, context: str) -> list[StorageRecord]:
        """List records in a collection scoped to a specific context.

        Args:
            collection: The unique name to retrieve data for
            context: Owner context scoping the listing.

        Returns:
            A list of storage records
        """
        prefix = f"{context}|{collection}:"
        return [r for k, r in self.storage.items() if k.startswith(prefix)]

    async def _remove_collection(self, collection: str, context: str) -> bool:
        """Wipe a collection scoped to a specific context.

        Args:
            collection: The unique name to retrieve data for
            context: Owner context scoping the wipe.

        Returns:
            bool: True if the collection was removed, False otherwise
        """
        prefix = f"{context}|{collection}:"
        to_delete = [k for k in self.storage if k.startswith(prefix)]
        for k in to_delete:
            del self.storage[k]
        self._save_to_file()
        logger.debug("Removed collection %s (%d docs)", collection, len(to_delete))
        return True

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, type[BaseModel]],
        storage_file_path: str = "local_storage",
    ) -> None:
        """Initialize the storage."""
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id, config=config)
        self.storage_file_path = f"{self.mission_id}_{storage_file_path}.json"
        self.storage_file = Path(self.storage_file_path)
        self.storage = self._load_from_file()
