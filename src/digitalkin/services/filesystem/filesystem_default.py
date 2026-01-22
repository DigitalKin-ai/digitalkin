"""Default filesystem implementation."""

import hashlib
import os
import tempfile
import uuid
from typing import Any, Literal

from agentic_mesh_protocol.pagination.v1.pagination_pb2 import PaginationRequest
from anyio import Path as AsyncPath

from digitalkin.exception.filesystem import FilesystemServiceError
from digitalkin.logger import logger
from digitalkin.models.services.filesystem import FileFilter, FileStatus, FilesystemRecord, FileType, UploadFileData
from digitalkin.services.filesystem.filesystem_strategy import FilesystemStrategy


class DefaultFilesystem(FilesystemStrategy):
    """Default filesystem implementation.

    This implementation provides a local filesystem-based storage solution
    with support for all filesystem operations defined in the strategy.
    Files are stored in a temporary directory with proper metadata tracking.
    """

    def __init__(self, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        """Initialize the default filesystem strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version this strategy is associated with
        """
        super().__init__(mission_id, setup_id, setup_version_id)
        self.temp_root: str = tempfile.mkdtemp()
        os.makedirs(self.temp_root, exist_ok=True)
        self.db: dict[str, FilesystemRecord] = {}
        logger.debug("DefaultFilesystem initialized with temp_root: %s", self.temp_root)

    # ═════════════════════════════════ Private Methods ══════════════════════════════════ #

    @staticmethod
    def __calculate_checksum(content: bytes) -> str:
        """Calculate SHA-256 checksum of content.

        Args:
            content: The content to calculate checksum for

        Returns:
            str: The SHA-256 checksum
        """
        return hashlib.sha256(content).hexdigest()

    def __filter_db(
        self,
        filters: FileFilter,
    ) -> list[FilesystemRecord]:
        """Filter the in-memory database based on provided filters.

        Args:
            filters: Filter criteria for the files

        Returns:
            list[FilesystemRecord]: List of files matching the filters
        """
        logger.debug("Filtering db with filters: %s", filters)
        return [
            f
            for f in self.db.values()
            if (not filters.names or f.name in filters.names)
            and (not filters.ids or f.id in filters.ids)
            and (not filters.types or f.type in filters.types)
            and (not filters.status or f.status == filters.status)
            and (not filters.content_type_prefix or f.content_type.startswith(filters.content_type_prefix))
            and (not filters.min_size_bytes or f.size_bytes >= filters.min_size_bytes)
            and (not filters.max_size_bytes or f.size_bytes <= filters.max_size_bytes)
            and (not filters.prefix or f.name.startswith(filters.prefix))
            and (not filters.content_type or f.content_type == filters.content_type)
        ]

    # ══════════════════════════════ Protected Methods ═══════════════════════════════ #

    def _get_context_temp_dir(self, context: str) -> str:
        """Get the temporary directory path for a specific context.

        Args:
            context: The mission ID or setup ID.

        Returns:
            str: Path to the context's temporary directory
        """
        # Create a context-specific directory to organize files
        context_dir = os.path.join(self.temp_root, context.replace(":", "_"))
        os.makedirs(context_dir, exist_ok=True)
        return context_dir

    async def upload(
        self,
        files: list[UploadFileData],
    ) -> tuple[list[FilesystemRecord], int, int]:
        """Upload files to the local filesystem.

        Returns:
            Tuple of (uploaded files, upload count, failure count).

        Raises:
            FilesystemServiceError: If a single file upload fails.
        """
        uploaded_files: list[FilesystemRecord] = []
        total_uploaded = 0
        total_failed = 0

        for file in files:
            try:
                # Check if file with same name exists in the context
                context_dir = self._get_context_temp_dir(self.setup_id)
                file_path = os.path.join(context_dir, file.name)
                if await AsyncPath(file_path).exists() and not file.replace_if_exists:
                    msg = f"File with name {file.name} already exists."
                    logger.error(msg)
                    raise FilesystemServiceError(msg)  # noqa: TRY301

                await AsyncPath(file_path).write_bytes(file.content)
                storage_uri = str(await AsyncPath(file_path).resolve())
                file_data = FilesystemRecord(
                    id=str(uuid.uuid4()),
                    context=self.setup_id,
                    name=file.name,
                    type=file.type,
                    content_type=file.content_type or "application/octet-stream",
                    size_bytes=len(file.content),
                    checksum=self.__calculate_checksum(file.content),
                    metadata=file.metadata,
                    storage_uri=storage_uri,
                    url=storage_uri,
                    status=FileStatus.ACTIVE,
                )

                self.db[file_data.id] = file_data
                uploaded_files.append(file_data)
                total_uploaded += 1
                logger.debug("Uploaded file %s", file_data)
            except Exception as e:  # noqa: PERF203
                logger.exception("Error uploading file %s: %s", file.name, e)
                total_failed += 1
                # If only one file and it failed, propagate the error for pytest.raises
                if len(files) == 1:
                    raise

        return uploaded_files, total_uploaded, total_failed

    async def get(
        self,
        file_id: str,
        _context: Literal["mission", "setup"] = "mission",
        *,
        include_content: bool = False,
    ) -> FilesystemRecord:
        """Retrieve a file by ID from local storage.

        Returns:
            The requested file record.

        Raises:
            FilesystemServiceError: If file not found or retrieval error.
        """
        try:
            logger.debug("Getting file with id: %s", file_id)
            file_data: FilesystemRecord | None = None
            if file_id:
                file_data = self.db.get(file_id)

            if not file_data:
                msg = f"File not found with id {file_id}"
                logger.error(msg)
                raise FilesystemServiceError(msg)  # noqa: TRY301

            if include_content:
                file_path = file_data.storage_uri
                if await AsyncPath(file_path).exists():
                    file_data.content = await AsyncPath(file_path).read_bytes()

        except Exception as e:
            msg = f"Error getting file: {e!s}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)
        else:
            return file_data

    async def list(
        self,
        filters: FileFilter,
        *,
        pagination: PaginationRequest = PaginationRequest(limit=100, offset=0, order=None),
        include_content: bool = False,
    ) -> tuple[list[FilesystemRecord], int]:
        """List files matching filters with pagination.

        Returns:
            Tuple of (paginated files, total count).

        Raises:
            FilesystemServiceError: If listing error occurs.
        """
        try:
            logger.debug("Listing files with filters: %s", filters)
            # Filter files based on provided criteria
            filtered_files = self.__filter_db(filters)
            if not filtered_files:
                return [], 0
            # Sort if order is specified
            # TODO

            # Apply pagination
            start_idx = pagination.offset
            end_idx = start_idx + pagination.limit
            paginated_files = filtered_files[start_idx:end_idx]

            if include_content:
                for file in paginated_files:
                    file.content = await AsyncPath(file.storage_uri).read_bytes()

        except Exception as e:
            msg = f"Error listing files: {e!s}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)
        else:
            return paginated_files, len(filtered_files)

    async def update(
        self,
        file_id: str,
        content: bytes | None = None,
        type: FileType | None = None,  # noqa: A002
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        new_name: str | None = None,
        status: FileStatus | None = None,
    ) -> FilesystemRecord:
        """Update file metadata, content, or both in local storage.

        Returns:
            The updated file record.

        Raises:
            FilesystemServiceError: If file not found or update error.
        """
        logger.debug("Updating file with id: %s", file_id)
        if file_id not in self.db:
            msg = f"File with id {file_id} does not exist."
            logger.error(msg)
            raise FilesystemServiceError(msg)

        try:
            context_dir = self._get_context_temp_dir(self.setup_id)
            file_path = os.path.join(context_dir, file_id)
            existing_file = self.db[file_id]

            if content is not None:
                await AsyncPath(file_path).write_bytes(content)
                existing_file.size_bytes = len(content)
                existing_file.checksum = self.__calculate_checksum(content)

            if type is not None:
                existing_file.type = type

            if content_type is not None:
                existing_file.content_type = content_type

            if metadata is not None:
                existing_file.metadata = metadata

            if status is not None:
                existing_file.status = status

            if new_name is not None:
                new_path = os.path.join(context_dir, new_name)
                await AsyncPath(file_path).rename(new_path)
                existing_file.name = new_name
                existing_file.storage_uri = str(await AsyncPath(new_path).resolve())

            self.db[file_id] = existing_file

        except Exception as e:
            msg = f"Error updating file {file_id}: {e!s}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)
        else:
            return existing_file

    async def delete(
        self,
        filters: FileFilter,
        *,
        permanent: bool = False,
        _force: bool = False,  # API interface parameter, not used in local filesystem
    ) -> tuple[dict[str, bool], int, int]:
        """Delete files matching filters from local storage.

        Returns:
            Tuple of (results dict, deleted count, failed count).

        Raises:
            FilesystemServiceError: If deletion error occurs.
        """
        logger.debug("Deleting files with filters: %s", filters)
        results: dict[str, bool] = {}  # id -> success
        total_deleted = 0
        total_failed = 0

        try:
            # Determine which files to delete
            files_to_delete = [f.id for f in self.__filter_db(filters)]

            if not files_to_delete:
                logger.info("No files match the deletion criteria.")
                return results, total_deleted, total_failed

            for file_id in files_to_delete:
                file_data = self.db[file_id]
                if not file_data:
                    results[file_id] = False
                    total_failed += 1
                    continue

                try:
                    file_path = file_data.storage_uri
                    if await AsyncPath(file_path).exists():
                        if permanent:
                            await AsyncPath(file_path).unlink()
                            del self.db[file_id]
                        else:
                            file_data.status = FileStatus.DELETED
                            self.db[file_id] = file_data
                        results[file_id] = True
                        total_deleted += 1
                    else:
                        results[file_id] = False
                        total_failed += 1
                except Exception as e:
                    logger.exception("Error deleting file %s: %s", file_id, e)
                    results[file_id] = False
                    total_failed += 1

        except Exception as e:
            msg = f"Error in delete_files: {e!s}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)

        else:
            return results, total_deleted, total_failed
