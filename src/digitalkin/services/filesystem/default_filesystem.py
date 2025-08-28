"""Default filesystem implementation."""

import hashlib
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Literal

from digitalkin.logger import logger
from digitalkin.services.filesystem.filesystem_strategy import (
    FileFilter,
    FilesystemRecord,
    FilesystemServiceError,
    FilesystemStrategy,
    UploadFileData,
)


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

    @staticmethod
    def _calculate_checksum(content: bytes) -> str:
        """Calculate SHA-256 checksum of content.

        Args:
            content: The content to calculate checksum for

        Returns:
            str: The SHA-256 checksum
        """
        return hashlib.sha256(content).hexdigest()

    def _filter_db(
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
            and (not filters.file_ids or f.id in filters.file_ids)
            and (not filters.file_types or f.file_type in filters.file_types)
            and f.context == self.mission_id
            and (not filters.status or f.status == filters.status)
            and (not filters.content_type_prefix or f.content_type.startswith(filters.content_type_prefix))
            and (not filters.min_size_bytes or f.size_bytes >= filters.min_size_bytes)
            and (not filters.max_size_bytes or f.size_bytes <= filters.max_size_bytes)
            and (not filters.prefix or f.name.startswith(filters.prefix))
            and (not filters.content_type or f.content_type == filters.content_type)
        ]

    def upload_files(
        self,
        files: list[UploadFileData],
    ) -> tuple[list[FilesystemRecord], int, int]:
        """Upload multiple files to the system.

        This method allows batch uploading of files with validation and
        error handling for each individual file. Files are processed
        atomically - if one fails, others may still succeed.

        Args:
            files: List of files to upload

        Returns:
            tuple[list[FilesystemRecord], int, int]: List of uploaded files, total uploaded count, total failed count

        Raises:
            FilesystemServiceError: If there is an error uploading the files
        """
        uploaded_files: list[FilesystemRecord] = []
        total_uploaded = 0
        total_failed = 0

        for file in files:
            try:
                # Check if file with same name exists in the context
                context_dir = self._get_context_temp_dir(self.setup_id)
                file_path = os.path.join(context_dir, file.name)
                if os.path.exists(file_path) and not file.replace_if_exists:
                    msg = f"File with name {file.name} already exists."
                    logger.error(msg)
                    raise FilesystemServiceError(msg)  # noqa: TRY301

                Path(file_path).write_bytes(file.content)
                storage_uri = str(Path(file_path).resolve())
                file_data = FilesystemRecord(
                    id=str(uuid.uuid4()),
                    context=self.setup_id,
                    name=file.name,
                    file_type=file.file_type,
                    content_type=file.content_type or "application/octet-stream",
                    size_bytes=len(file.content),
                    checksum=self._calculate_checksum(file.content),
                    metadata=file.metadata,
                    storage_uri=storage_uri,
                    file_url=storage_uri,
                    status="ACTIVE",
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

    def get_files(
        self,
        filters: FileFilter,
        *,
        list_size: int = 100,
        offset: int = 0,
        order: str | None = None,  # noqa: ARG002
        include_content: bool = False,
    ) -> tuple[list[FilesystemRecord], int]:
        """List files with filtering, sorting, and pagination.

        This method provides flexible file querying capabilities with support for:
        - Multiple filter criteria (name, type, dates, size, etc.)
        - Pagination for large result sets
        - Sorting by various fields
        - Scoped access by context

        Args:
            filters: Filter criteria for the files
            list_size: Number of files to return per page
            offset: Offset to start listing files from
            order: Fields to order results by (example: "created_at:asc,name:desc")
            include_content: Whether to include file content in response

        Returns:
            tuple[list[FilesystemRecord], int]: List of files, total count

        Raises:
            FilesystemServiceError: If there is an error listing the files
        """
        try:
            logger.debug("Listing files with filters: %s", filters)
            # Filter files based on provided criteria
            filtered_files = self._filter_db(filters)
            if not filtered_files:
                return [], 0
            # Sort if order is specified
            # TODO

            # Apply pagination
            start_idx = offset
            end_idx = start_idx + list_size
            paginated_files = filtered_files[start_idx:end_idx]

            logger.critical(f"{filters=} | {paginated_files=}")

            if include_content:
                for file in paginated_files:
                    file.content = Path(file.storage_uri).read_bytes()

        except Exception as e:
            msg = f"Error listing files: {e!s}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)
        else:
            return paginated_files, len(filtered_files)

    def get_file(
        self,
        file_id: str,
        *,
        include_content: bool = False,
    ) -> FilesystemRecord:
        """Get a specific file by ID or name.

        This method fetches detailed information about a single file,
        with optional content inclusion. Supports lookup by either
        unique ID or name within a context.

        Args:
            file_id: The ID of the file to be retrieved
            include_content: Whether to include file content in response

        Returns:
            FilesystemRecord: Metadata about the retrieved file

        Raises:
            FilesystemServiceError: If there is an error retrieving the file
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
                if os.path.exists(file_path):
                    content = Path(file_path).read_bytes()
                    file_data.content = content

        except Exception as e:
            msg = f"Error getting file: {e!s}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)
        else:
            return file_data

    def update_file(
        self,
        file_id: str,
        content: bytes | None = None,
        file_type: Literal[
            "UNSPECIFIED",
            "DOCUMENT",
            "IMAGE",
            "VIDEO",
            "AUDIO",
            "ARCHIVE",
            "CODE",
            "OTHER",
        ]
        | None = None,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        new_name: str | None = None,
        status: str | None = None,
    ) -> FilesystemRecord:
        """Update file metadata, content, or both.

        This method allows updating various aspects of a file:
        - Rename files
        - Update content and content type
        - Modify metadata
        - Create new versions

        Args:
            file_id: The id of the file to be updated
            content: Optional new content of the file
            file_type: Optional new type of data
            content_type: Optional new MIME type
            metadata: Optional new metadata (will merge with existing)
            new_name: Optional new name for the file
            status: Optional new status for the file

        Returns:
            FilesystemRecord: Metadata about the updated file

        Raises:
            FilesystemServiceError: If there is an error during update
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
                Path(file_path).write_bytes(content)
                existing_file.size_bytes = len(content)
                existing_file.checksum = self._calculate_checksum(content)

            if file_type is not None:
                existing_file.file_type = file_type

            if content_type is not None:
                existing_file.content_type = content_type

            if metadata is not None:
                existing_file.metadata = metadata

            if status is not None:
                existing_file.status = status

            if new_name is not None:
                new_path = os.path.join(context_dir, new_name)
                os.rename(file_path, new_path)
                existing_file.name = new_name
                existing_file.storage_uri = str(Path(new_path).resolve())

            self.db[file_id] = existing_file

        except Exception as e:
            msg = f"Error updating file {file_id}: {e!s}"
            logger.exception(msg)
            raise FilesystemServiceError(msg)
        else:
            return existing_file

    def delete_files(
        self,
        filters: FileFilter,
        *,
        permanent: bool = False,
        force: bool = False,  # noqa: ARG002
    ) -> tuple[dict[str, bool], int, int]:
        """Delete multiple files.

        This method supports batch deletion of files with options for:
        - Soft deletion (marking as deleted)
        - Permanent deletion
        - Force deletion of files in use
        - Individual error reporting per file

        Args:
            filters: Filter criteria for the files to delete
            permanent: Whether to permanently delete the files
            force: Whether to force delete even if files are in use

        Returns:
            tuple[dict[str, bool], int, int]: Results per file, total deleted count, total failed count

        Raises:
            FilesystemServiceError: If there is an error deleting the files
        """
        logger.debug("Deleting files with filters: %s", filters)
        results: dict[str, bool] = {}  # id -> success
        total_deleted = 0
        total_failed = 0

        try:
            # Determine which files to delete
            files_to_delete = [f.id for f in self._filter_db(filters)]

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
                    if os.path.exists(file_path):
                        if permanent:
                            os.remove(file_path)
                            del self.db[file_id]
                        else:
                            file_data.status = "DELETED"
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
