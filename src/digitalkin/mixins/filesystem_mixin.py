"""Filesystem Mixin to ease filesystem use."""

from digitalkin.logger import logger
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.services.filesystem import FileMetadata, FileType, FileUploadMetadata
from digitalkin.models.services.services import Context
from digitalkin.models.services.storage import Visibility
from digitalkin.services.filesystem.filesystem_strategy import FileFilter, FilesystemRecord, UploadFileData


class FilesystemMixin:
    """Mixin providing filesystem operations through the filesystem strategy.

    This mixin wraps filesystem strategy calls to provide a cleaner API
    for file operations in trigger handlers.
    """

    @staticmethod
    async def upload_files(
        context: ModuleContext, files: list[UploadFileData]
    ) -> tuple[list[FilesystemRecord], int, int]:
        """Upload files using the filesystem strategy.

        Args:
            context: Module context containing the filesystem strategy
            files: List of files to upload

        Returns:
            Tuple of (all_files, succeeded_files, failed_files)

        Raises:
            FilesystemServiceError: If upload operation fails
        """
        return await context.filesystem.upload_files(files)

    @staticmethod
    async def upload_file(
        context: ModuleContext,
        name: str,
        content: bytes,
        content_type: str,
        type: FileType | None = None,  # Matches the canonical field name # noqa: A002
        visibility: Visibility = Visibility.UNSPECIFIED,
        *,
        sdk_created: bool = False,
    ) -> FileMetadata | None:
        """Upload a single file, inferring its type from the MIME type when not given.

        Args:
            context: Module context containing the filesystem strategy
            name: The name of the file
            content: The content of the file
            content_type: The MIME type of the file
            type: The category of the file; inferred from `content_type` when omitted
            visibility: Read-access scope; UNSPECIFIED lets the service default it
            sdk_created: Whether this file is a module deliverable

        Returns:
            The uploaded file, or None when the upload failed.
        """
        try:
            records, _uploaded, failed = await context.filesystem.upload_files([
                UploadFileData(
                    content=content,
                    name=name,
                    type=type or FileType.from_content_type(content_type),
                    content_type=content_type,
                    metadata=FileUploadMetadata(sdk_created=sdk_created, filename=name),
                    replace_if_exists=True,
                    visibility=visibility,
                )
            ])
        except Exception:
            logger.exception("Failed to upload file '%s' (%s)", name, content_type)
            return None
        if failed or not records:
            logger.error("Upload rejected for file '%s' (%s)", name, content_type)
            return None
        return records[0]

    @staticmethod
    async def get_file(
        context: ModuleContext,
        file_id: str,
        file_context: Context = Context.MISSIONS,
        *,
        include_content: bool = True,
    ) -> FilesystemRecord:
        """Retrieve a file by ID, with its content by default.

        Args:
            context: Module context containing the filesystem strategy
            file_id: Unique identifier for the file
            file_context: The owner context to look the file up in
            include_content: Whether to include file content in the response

        Returns:
            File object with metadata and optionally content

        Raises:
            FilesystemServiceError: If file retrieval fails
        """
        return await context.filesystem.get_file(file_id, context=file_context, include_content=include_content)

    @staticmethod
    async def get_files(
        context: ModuleContext,
        filters: FileFilter,
        *,
        include_content: bool = False,
    ) -> list[FilesystemRecord]:
        """List files matching the given filters.

        Args:
            context: Module context containing the filesystem strategy
            filters: Filter criteria for the files
            include_content: Whether to include file content in the response

        Returns:
            Matching file records, empty when the lookup fails.
        """
        try:
            records, _total = await context.filesystem.get_files(filters, include_content=include_content)
        except Exception:
            logger.exception("Failed to list files")
            return []
        return records

    @staticmethod
    async def delete_file(
        context: ModuleContext,
        file_id: str,
        file_context: Context = Context.MISSIONS,
        *,
        permanent: bool = False,
    ) -> bool:
        """Delete a file.

        Args:
            context: Module context containing the filesystem strategy
            file_id: Unique identifier for the file
            file_context: The owner context to look the file up in
            permanent: Whether to permanently delete the file

        Returns:
            True when the file was deleted.
        """
        try:
            results, _deleted, _failed = await context.filesystem.delete_files(
                FileFilter(context=file_context, file_ids=[file_id]),
                permanent=permanent,
            )
        except Exception:
            logger.exception("Failed to delete file '%s'", file_id)
            return False
        return results.get(file_id, False)

    @staticmethod
    async def resolve_files(
        context: ModuleContext,
        files: list[FileMetadata],
        file_context: Context = Context.MISSIONS,
    ) -> list[FileMetadata]:
        """Backfill empty fields from what the filesystem service holds.

        A setup form may submit little more than an id; the persisted entry has to stay
        displayable, so anything the caller left empty is filled from the stored record.
        A file the service cannot return is kept as submitted rather than dropped.

        Args:
            context: Module context containing the filesystem strategy
            files: The files as submitted
            file_context: The owner context the files live in; setup-time uploads are
                scoped to the setup, not the mission

        Returns:
            The files, with empty fields filled in where the service could resolve them.
        """
        resolved: list[FileMetadata] = []
        for file in files:
            try:
                record = await context.filesystem.get_file(file.id, context=file_context, include_content=False)
            except Exception:
                logger.warning("Could not resolve file '%s'; keeping it as submitted", file.id, exc_info=True)
                resolved.append(file)
                continue
            resolved.append(
                file.model_copy(
                    update={
                        "name": file.name or record.name,
                        "type": file.type if file.type is not FileType.UNSPECIFIED else record.type,
                        "content_type": file.content_type or record.content_type,
                        "size_bytes": file.size_bytes or record.size_bytes,
                        "file_url": file.file_url or record.file_url,
                    }
                )
            )
        return resolved

    @staticmethod
    async def set_visibility(context: ModuleContext, file_id: str, visibility: Visibility) -> FilesystemRecord:
        """Change the read-access scope of an already uploaded file.

        Visibility is otherwise write-once at upload time, so this is the only way to
        widen or narrow access to a file the module has already produced.

        Args:
            context: Module context containing the filesystem strategy
            file_id: Unique identifier for the file
            visibility: The new read-access scope; UNSPECIFIED leaves it unchanged

        Returns:
            The updated file record.
        """
        return await context.filesystem.update_file(file_id, visibility=visibility)
