"""This module implements the default storage strategy."""

import datetime
import json
import logging
import tempfile
from pathlib import Path

from pydantic import BaseModel

from digitalkin.services.storage.storage_strategy import DataType, StorageRecord, StorageStrategy

logger = logging.getLogger(__name__)


class DefaultStorage(StorageStrategy):
    """This class implements the default storage strategy with file persistence.

    The storage persists data in a local JSON file, enabling data retention
    across multiple requests or module instances.
    """

    def _load_from_file(self) -> dict[str, StorageRecord]:
        """Load storage data from the file.

        Returns:
            A dictionary containing the loaded storage records
        """
        file_path = Path(self.storage_file_path)
        if not file_path.exists():
            return {}

        try:
            records = {}
            file_content = json.loads(file_path.read_text(encoding="utf-8"))

            for key, record_dict in file_content.items():
                # Get the stored record data
                name = record_dict.get("name", "")
                model_class = self.config.get(name)

                if not model_class:
                    logger.warning("No model found for record %s", name)
                    continue

                # Create a model instance from the stored data
                data_dict = record_dict.get("data", {})
                try:
                    data_model = model_class.model_validate(data_dict)
                except Exception:
                    logger.exception("Failed to validate data for record %s", name)
                    continue

                # Create a StorageRecord object
                record = StorageRecord(
                    mission_id=record_dict.get("mission_id", ""),
                    name=name,
                    data=data_model,
                    data_type=DataType[record_dict.get("data_type", "OUTPUT")],
                )

                # Set dates if they exist
                if "creation_date" in record_dict:
                    record.creation_date = datetime.datetime.fromisoformat(record_dict["creation_date"])
                if "update_date" in record_dict:
                    record.update_date = datetime.datetime.fromisoformat(record_dict["update_date"])

                records[key] = record
        except json.JSONDecodeError:
            logger.exception("Error decoding JSON from file")
            return {}
        except FileNotFoundError:
            logger.info("Storage file not found, starting with empty storage")
            return {}
        except Exception:
            logger.exception("Unexpected error loading storage")
            return {}
        return records

    def _save_to_file(self) -> None:
        """Save storage data to the file using a safe write pattern."""
        # Usage of pathlib for file operations
        file_path = Path(self.storage_file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Convert storage to a serializable format
            serializable_data = {}
            for key, record in self.storage.items():
                record_dict = {
                    "mission_id": record.mission_id,
                    "name": record.name,
                    "data_type": record.data_type.name,  # Convert enum to string
                    "data": record.data.model_dump(),  # Convert Pydantic model to dict
                }

                # Handle dates (convert to ISO format strings)
                if record.creation_date:
                    record_dict["creation_date"] = record.creation_date.isoformat()
                if record.update_date:
                    record_dict["update_date"] = record.update_date.isoformat()

                serializable_data[key] = record_dict

            # usage of NamedTemporaryFile for atomic writes
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=str(file_path.parent), delete=False, suffix=".tmp"
            ) as temp_file:
                json.dump(serializable_data, temp_file, indent=2)
                temp_path = temp_file.name

            # Creation of a backup if the file already exists
            if file_path.exists():
                backup_path = f"{self.storage_file_path}.bak"
                file_path.replace(backup_path)

            # Remplacement du fichier (opération atomique) avec pathlib
            Path(temp_path).replace(str(file_path))

        except PermissionError:
            logger.exception("Permission denied when saving to file")
        except OSError:
            logger.exception("OS error when saving to file")
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
        name = record.name
        if name in self.storage:
            msg = f"Record with name {name} already exists"
            raise ValueError(msg)
        self.storage[name] = record
        self.storage[name].creation_date = datetime.datetime.now(datetime.timezone.utc)
        self.storage[name].update_date = datetime.datetime.now(datetime.timezone.utc)

        # Persist to file
        self._save_to_file()

        logger.info("CREATE %s:%s successful", name, record)
        return self.storage[name]

    def _read(self, name: str) -> StorageRecord | None:
        """Get records from the database.

        Args:
            name: The unique name to retrieve data for

        Returns:
            StorageRecord: The corresponding record
        """
        logger.info("GET record linked to the key = %s", name)
        if name not in self.storage:
            logger.info("GET key = %s: DOESN'T EXIST", name)
            return None
        return self.storage[name]

    def _modify(self, name: str, data: BaseModel) -> StorageRecord | None:
        """Update records in the database and persist to file.

        Args:
            name: The unique name to store the data under
            data: The data to modify

        Returns:
            StorageRecord: The modified record
        """
        if name not in self.storage:
            logger.info("UPDATE key = %s: DOESN'T EXIST", name)
            return None
        self.storage[name].data = data
        self.storage[name].update_date = datetime.datetime.now(datetime.timezone.utc)

        # Persist to file
        self._save_to_file()

        return self.storage[name]

    def _remove(self, name: str) -> bool:
        """Delete records from the database and update file.

        Args:
            name: The unique name to remove a record

        Returns:
            bool: True if the record was removed, False otherwise
        """
        if name not in self.storage:
            logger.info("DELETE key = %s: DOESN'T EXIST", name)
            return False
        del self.storage[name]

        # Persist to file
        self._save_to_file()

        return True

    def __init__(
        self,
        mission_id: str,
        config: dict[str, type[BaseModel]],
        storage_file_path: str = "local_storage",
        **kwargs,  # noqa: ANN003, ARG002
    ) -> None:
        """Initialize the storage."""
        super().__init__(mission_id=mission_id, config=config)
        self.storage_file_path = f"{self.mission_id}_{storage_file_path}.json"
        self.storage = self._load_from_file()
