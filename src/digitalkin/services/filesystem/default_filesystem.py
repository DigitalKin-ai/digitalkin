"""Default filesystem."""

import logging
import os
import tempfile
from pathlib import Path

from digitalkin.services.filesystem.filesystem_strategy import (
    FilesystemData,
    FilesystemServiceError,
    FilesystemStrategy,
    FileType,
)

logger = logging.getLogger(__name__)


class DefaultFilesystem(FilesystemStrategy):
    """Default state filesystem strategy."""

    def __init__(self, mission_id: str, setup_version_id: str, config: dict[str, str]) -> None:
        """Initialize the default filesystem strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_version_id: The ID of the setup version this strategy is associated with
            config: A dictionary mapping names to Pydantic model classes
        """
        super().__init__(mission_id, setup_version_id, config)
        self.temp_root: str = self.config.get("temp_root", "") or tempfile.gettempdir()
        os.makedirs(self.temp_root, exist_ok=True)
        self.db: dict[str, FilesystemData] = {}

    def _get_kin_context_temp_dir(self, kin_context: str) -> str:
        """Get the temporary directory path for a specific kin_context.

        Args:
            kin_context: The mission ID or setup ID.

        Returns:
            str: Path to the kin_context's temporary directory
        """
        # Create a kin_context-specific directory to organize files
        kin_context_dir = os.path.join(self.temp_root, kin_context.replace(":", "_"))
        os.makedirs(kin_context_dir, exist_ok=True)
        return kin_context_dir

    def upload(self, content: bytes, name: str, file_type: FileType) -> FilesystemData:
        """Create a new file in the file system.

        Args:
            content: The content of the file to be uploaded
            name: The name of the file to be created
            file_type: The type of data being uploaded

        Returns:
            FilesystemData: Metadata about the uploaded file

        Raises:
            FileExistsError: If the file already exists
            FilesystemServiceError: If there is an error during upload
        """
        if self.db.get(name):
            msg = f"File with name {name} already exists."
            logger.error(msg)
            raise FileExistsError(msg)
        try:
            kin_context_dir = self._get_kin_context_temp_dir(self.mission_id)
            file_path = os.path.join(kin_context_dir, name)
            Path(file_path).write_bytes(content)
            url = str(Path(file_path).resolve())
            return FilesystemData(
                kin_context=self.mission_id,
                name=name,
                file_type=file_type,
                url=url,
            )
        except Exception:
            msg = f"Error uploading file {name}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)

    def get(self, name: str) -> FilesystemData:
        """Get file from the filesystem.

        Args:
            name: The name of the file to be retrieved

        Returns:
            FilesystemData: Metadata about the retrieved file

        Raises:
            FileNotFoundError: If the file does not exist
            FilesystemServiceError: If the file does not exist
        """
        try:
            return self.db[name]
        except KeyError:
            # If the file does not exist in the database, raise an error
            msg = f"File with name {name} does not exist."
            logger.exception(msg)
            raise FileNotFoundError(msg)
        except Exception:
            msg = f"Error getting file {name}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)

    def update(self, name: str, content: bytes, file_type: FileType) -> FilesystemData:
        """Update files in the filesystem.

        Args:
            content: The new content of the file
            name: The name of the file to be updated
            file_type: The type of data being updated

        Returns:
            FilesystemData: Metadata about the updated file

        Raises:
            FileNotFoundError: If the file does not exist
            FilesystemServiceError: If there is an error during update
        """
        if name not in self.db:
            msg = f"File with name {name} does not exist."
            logger.error(msg)
            raise FileNotFoundError(msg)
        try:
            kin_context_dir = self._get_kin_context_temp_dir(self.mission_id)
            file_path = os.path.join(kin_context_dir, name)
            Path(file_path).write_bytes(content)
            url = str(Path(file_path).resolve())
            file = FilesystemData(
                kin_context=self.mission_id,
                name=name,
                file_type=file_type,
                url=url,
            )
            self.db[name] = file
        except Exception:
            msg = f"Error updating file {name}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)
        else:
            return file

    def delete(self, name: str) -> bool:
        """Delete files from the filesystem.

        Args:
            name: The name of the file to be deleted

        Returns:
            int: 1 if the file was deleted successfully

        Raises:
            FileNotFoundError: If the file does not exist
            FilesystemServiceError: If there is an error during deletion
        """
        # First check if the file exists in the database
        if name not in self.db:
            msg = f"File with name {name} does not exist in the database."
            logger.error(msg)
            raise FileNotFoundError(msg)

        # Get the file path
        kin_context_dir = self._get_kin_context_temp_dir(self.mission_id)
        file_path = os.path.join(kin_context_dir, name)

        # Check if the file exists in the filesystem
        if not os.path.exists(file_path):
            msg = f"File {name} exists in database but not in filesystem at {file_path}."
            logger.error(msg)
            # We could decide to just remove from DB here, but that might hide a larger issue
            # So we're raising a custom error to alert about the inconsistency
            raise FilesystemServiceError(msg)

        try:
            os.remove(file_path)
            del self.db[name]
            logger.info("File %s successfully deleted.", name)

        except OSError:
            msg = f"Error deleting file {name} from filesystem"
            logger.exception(msg)
            raise FilesystemServiceError(msg)
        except Exception:
            msg = f"Unexpected error deleting file {name}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)
        else:
            return True

    def get_all(self) -> list[FilesystemData]:
        """Get all files from the filesystem.

        Returns:
            list[FilesystemData]: A list of all files in the filesystem
        """
        return list(self.db.values())

    def get_batch(self, names: list[str]) -> dict[str, FilesystemData | None]:
        """Get files from the filesystem.

        Args:
            names: The names of the files to be retrieved

        Returns:
            dict[FilesystemData | None]: Metadata about the retrieved files
        """
        return {name: self.db.get(name, None) for name in names}
