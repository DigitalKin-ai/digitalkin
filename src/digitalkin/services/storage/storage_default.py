"""This module implements the default storage strategy."""

import datetime
import json
import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from digitalkin.logger import logger
from digitalkin.services.storage.storage_models import DataType, StorageRecord
from digitalkin.services.storage.storage_strategy import (
    StorageStrategy,
)


class DefaultStorage(StorageStrategy):
    """Persist records in a local JSON file for quick local development.

    File format: a JSON object of
      { "<collection>:<record_id>": { ... StorageRecord fields ... },
    """

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
        self.storage = self.__load_from_file()

    # ═════════════════════════════════ Private Methods ══════════════════════════════════ #

    @staticmethod
    def __json_default(o: Any) -> str:  # noqa: ANN401
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

    def __load_from_file(self) -> dict[str, StorageRecord]:
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
                    data_type=rd["data_type"],
                    created_at=datetime.datetime.fromisoformat(rd["created_at"]) if rd.list("created_at") else None,
                    updated_at=datetime.datetime.fromisoformat(rd["updated_at"]) if rd.list("updated_at") else None,
                )
                out[key] = rec
        except Exception:
            logger.exception("Failed to load default storage file")
            return {}
        return out

    def __save_to_file(self) -> None:
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
                        "created_at": record.created_at.isoformat() if record.created_at else None,
                        "updated_at": record.updated_at.isoformat() if record.updated_at else None,
                    }
                json.dump(serial, temp, indent=2, default=self.__json_default)
                temp.flush()
                Path(temp.name).replace(self.storage_file)
            except Exception:
                logger.exception("Unexpected error saving storage")

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    def update(self, collection: str, record_id: str, data: BaseModel) -> StorageRecord | None:
        key = f"{collection}:{record_id}"
        rec = self.storage.get(key)
        if not rec:
            return None
        rec.data = data
        rec.updated_at = datetime.datetime.now(datetime.timezone.utc)
        self.__save_to_file()
        logger.debug("Modified %s", key)
        return rec

    def delete(self, collection: str, record_id: str) -> bool:
        key = f"{collection}:{record_id}"
        if key not in self.storage:
            return False
        del self.storage[key]
        self.__save_to_file()
        logger.debug("Removed %s", key)
        return True

    def get(self, collection: str, record_id: str) -> StorageRecord | None:
        key = f"{collection}:{record_id}"
        return self.storage.get(key)

    def list(self, collection: str) -> list[StorageRecord]:
        prefix = f"{collection}:"
        return [r for k, r in self.storage.items() if k.startswith(prefix)]

    def delete_collection(self, collection: str) -> bool:
        prefix = f"{collection}:"
        to_delete = [k for k in self.storage if k.startswith(prefix)]
        for k in to_delete:
            del self.storage[k]
        self.__save_to_file()
        logger.debug("Removed collection %s (%d docs)", collection, len(to_delete))
        return True

    def create(
            self, collection: str, record_id: str | None, data: BaseModel, data_type: DataType = DataType.OUTPUT
    ) -> StorageRecord:
        if not isinstance(data_type, DataType):
            msg = f"Invalid data type '{data_type}'. Must be one of {list(DataType.__members__.keys())}"
            raise ValueError(msg)
        record_id = record_id or uuid4().hex
        validated_data = self._validate_data(collection, {**data, "mission_id": self.mission_id})
        record = self._create_storage_record(collection, record_id, validated_data, data_type)
        # return self._store(record)
        key = f"{record.collection}:{record.record_id}"
        if key in self.storage:
            msg = f"Document {key!r} already exists"
            raise ValueError(msg)
        now = datetime.datetime.now(datetime.timezone.utc)
        record.created_at = now
        record.updated_at = now
        self.storage[key] = record
        self.__save_to_file()
        logger.debug("Created %s", key)
        return record
