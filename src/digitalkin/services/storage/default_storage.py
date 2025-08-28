"""This module implements the default storage strategy."""

import datetime
import json
import tempfile
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from digitalkin.logger import logger
from digitalkin.services.storage.storage_strategy import (
    DataType,
    StorageRecord,
    StorageStrategy,
)


class DefaultStorage(StorageStrategy):
    """Persist records in a local JSON file for quick local development.

    File format: a JSON object of
      { "<collection>:<record_id>": { ... StorageRecord fields ... },
    """

    @staticmethod
    def _json_default(o: Any) -> str:  # noqa: ANN401
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

        try:
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
                    mission_id=rd["mission_id"],
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
                        "mission_id": record.mission_id,
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

    def _store(self, record: StorageRecord) -> StorageRecord:
        """Store a new record in the database and persist to file.

        Args:
            record: The record to store

        Returns:
            str: The ID of the new record

        Raises:
            ValueError: If the record already exists
        """
        key = f"{record.collection}:{record.record_id}"
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

    def _read(self, collection: str, record_id: str) -> StorageRecord | None:
        """Get records from the database.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record

        Returns:
            StorageRecord: The corresponding record
        """
        key = f"{collection}:{record_id}"
        return self.storage.get(key)

    def _update(self, collection: str, record_id: str, data: BaseModel) -> StorageRecord | None:
        """Update records in the database and persist to file.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record
            data: The data to modify

        Returns:
            StorageRecord: The modified record
        """
        key = f"{collection}:{record_id}"
        rec = self.storage.get(key)
        if not rec:
            return None
        rec.data = data
        rec.update_date = datetime.datetime.now(datetime.timezone.utc)
        self._save_to_file()
        logger.debug("Modified %s", key)
        return rec

    def _remove(self, collection: str, record_id: str) -> bool:
        """Delete records from the database and update file.

        Args:
            collection: The unique name to retrieve data for
            record_id: The unique ID of the record

        Returns:
            bool: True if the record was removed, False otherwise
        """
        key = f"{collection}:{record_id}"
        if key not in self.storage:
            return False
        del self.storage[key]
        self._save_to_file()
        logger.debug("Removed %s", key)
        return True

    def _list(self, collection: str) -> list[StorageRecord]:
        """Implements StorageStrategy._list.

        Args:
            collection: The unique name to retrieve data for

        Returns:
            A list of storage records
        """
        prefix = f"{collection}:"
        return [r for k, r in self.storage.items() if k.startswith(prefix)]

    def _remove_collection(self, collection: str) -> bool:
        """Implements StorageStrategy._remove_collection.

        Args:
            collection: The unique name to retrieve data for

        Returns:
            bool: True if the collection was removed, False otherwise
        """
        prefix = f"{collection}:"
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
        **kwargs,  # noqa: ANN003, ARG002
    ) -> None:
        """Initialize the storage."""
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id, config=config)
        self.storage_file_path = f"{self.mission_id}_{storage_file_path}.json"
        self.storage_file = Path(self.storage_file_path)
        self.storage = self._load_from_file()
