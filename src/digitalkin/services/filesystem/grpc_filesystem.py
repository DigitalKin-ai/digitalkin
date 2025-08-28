"""gRPC filesystem implementation."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Literal

from digitalkin_proto.digitalkin.filesystem.v1 import filesystem_pb2, filesystem_service_pb2_grpc
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.models import ClientConfig
from digitalkin.logger import logger
from digitalkin.services.filesystem.filesystem_strategy import (
    FileFilter,
    FilesystemRecord,
    FilesystemServiceError,
    FilesystemStrategy,
    UploadFileData,
)


class GrpcFilesystem(FilesystemStrategy, GrpcClientWrapper):
    """Default state filesystem strategy."""

    @staticmethod
    @contextmanager
    def _handle_grpc_errors(operation: str) -> Generator[Any, Any, Any]:
        """Context manager for consistent gRPC error handling.

        Yields:
            Allow error handling in context.

        Args:
            operation: Description of the operation being performed.

        Raises:
            ValueError: Error with the model validation.
            ServerError: from gRPC Client.
            FilesystemServiceError: Filesystem service internal.
        """
        try:
            yield
        except ServerError as e:
            msg = f"gRPC {operation} failed: {e}"
            logger.exception(msg)
            raise ServerError(msg) from e
        except Exception as e:
            msg = f"Unexpected error in {operation}"
            logger.exception(msg)
            raise FilesystemServiceError(msg) from e

    @staticmethod
    def _file_type_to_enum(file_type: str) -> filesystem_pb2.FileType:
        """Convert a file type string to a FileType enum.

        Args:
            file_type: The file type string to convert

        Returns:
            filesystem_pb2.FileType: The converted file type enum
        """
        if not file_type.upper().startswith("FILE_TYPE_"):
            file_type = f"FILE_TYPE_{file_type.upper()}"
        try:
            return getattr(filesystem_pb2.FileType, file_type.upper())
        except AttributeError:
            return filesystem_pb2.FileType.FILE_TYPE_UNSPECIFIED

    @staticmethod
    def _file_status_to_enum(file_status: str) -> filesystem_pb2.FileStatus:
        """Convert a file status string to a FileStatus enum.

        Args:
            file_status: The file status string to convert

        Returns:
            filesystem_pb2.FileStatus: The converted file status enum
        """
        if not file_status.upper().startswith("FILE_STATUS_"):
            file_status = f"FILE_STATUS_{file_status.upper()}"
        try:
            return getattr(filesystem_pb2.FileStatus, file_status.upper())
        except AttributeError:
            return filesystem_pb2.FileStatus.FILE_STATUS_UNSPECIFIED

    def _filter_to_proto(self, filters: FileFilter) -> filesystem_pb2.FileFilter:
        """Convert a FileFilter to a FileFilter proto message.

        Args:
            filters: The FileFilter to convert

        Returns:
            filesystem_pb2.FileFilter: The converted FileFilter proto message
        """
        return filesystem_pb2.FileFilter(
            context=self.setup_id,
            **filters.model_dump(exclude={"file_types", "status"}),
            file_types=[self._file_type_to_enum(file_type) for file_type in filters.file_types]
            if filters.file_types
            else None,
            status=self._file_status_to_enum(filters.status) if filters.status else None,
        )

    def _file_proto_to_data(self, file: filesystem_pb2.File) -> FilesystemRecord:
        """Convert a File proto message to FilesystemRecord.

        Args:
            file: The File proto message to convert

        Returns:
            FilesystemRecord: The converted data
        """
        return FilesystemRecord(
            id=file.file_id,
            context=self.setup_id,
            name=file.name,
            file_type=filesystem_pb2.FileType.Name(file.file_type),
            content_type=file.content_type,
            size_bytes=file.size_bytes,
            checksum=file.checksum,
            metadata=MessageToDict(file.metadata),
            storage_uri=file.storage_uri,
            file_url=file.file_url,
            status=filesystem_pb2.FileStatus.Name(file.status),
            content=file.content,
        )

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        client_config: ClientConfig,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the gRPC filesystem strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version this strategy is associated with
            client_config: Configuration for the gRPC client connection
            config: Configuration for the filesystem strategy
        """
        super().__init__(mission_id, setup_id, setup_version_id, config)
        self.service_name = "FilesystemService"
        channel = self._init_channel(client_config)
        self.stub = filesystem_service_pb2_grpc.FilesystemServiceStub(channel)
        logger.debug("Channel client 'Filesystem' initialized succesfully")

    def upload_files(
        self,
        files: list[UploadFileData],
    ) -> tuple[list[FilesystemRecord], int, int]:
        """Upload multiple files to the filesystem.

        Args:
            files: List of tuples containing (content, name, file_type, content_type, metadata, replace_if_exists)

        Returns:
            tuple[list[FilesystemRecord], int, int]: List of uploaded files, total uploaded count, total failed count
        """
        logger.debug("Uploading %d files", len(files))
        with GrpcFilesystem._handle_grpc_errors("UploadFiles"):
            upload_files: list[filesystem_pb2.UploadFileData] = []
            for file in files:
                metadata_struct: struct_pb2.Struct | None = None
                if file.metadata:
                    metadata_struct = struct_pb2.Struct()
                    metadata_struct.update(file.metadata)
                upload_files.append(
                    filesystem_pb2.UploadFileData(
                        context=self.setup_id,
                        name=file.name,
                        file_type=self._file_type_to_enum(file.file_type),
                        content_type=file.content_type or "application/octet-stream",
                        content=file.content,
                        metadata=metadata_struct,
                        status=filesystem_pb2.FileStatus.FILE_STATUS_UPLOADING,
                        replace_if_exists=file.replace_if_exists,
                    )
                )
            request = filesystem_pb2.UploadFilesRequest(files=upload_files)
            response: filesystem_pb2.UploadFilesResponse = self.exec_grpc_query("UploadFiles", request)
            results = [self._file_proto_to_data(result.file) for result in response.results if result.HasField("file")]
            logger.debug("Uploaded files: %s", results)
            return results, response.total_uploaded, response.total_failed

    def get_file(
        self,
        file_id: str,
        *,
        include_content: bool = False,
    ) -> FilesystemRecord:
        """Get a file from the filesystem.

        Args:
            file_id: The ID of the file to be retrieved
            include_content: Whether to include file content in response

        Returns:
            FilesystemRecord: Metadata about the retrieved file

        Raises:
            FilesystemServiceError: If there is an error retrieving the file
        """
        with GrpcFilesystem._handle_grpc_errors("GetFile"):
            request = filesystem_pb2.GetFileRequest(
                context=self.setup_id,
                file_id=file_id,
                include_content=include_content,
            )

            response: filesystem_pb2.GetFileResponse = self.exec_grpc_query("GetFile", request)

            return self._file_proto_to_data(response.file)

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
        """Update a file in the filesystem.

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
        with GrpcFilesystem._handle_grpc_errors("UpdateFile"):
            request = filesystem_pb2.UpdateFileRequest(
                context=self.setup_id,
                file_id=file_id,
                content=content,
                file_type=self._file_type_to_enum(file_type) if file_type else None,
                content_type=content_type,
                new_name=new_name,
                status=self._file_status_to_enum(status) if status else None,
            )

            if metadata:
                request.metadata.update(metadata)

            response: filesystem_pb2.UpdateFileResponse = self.exec_grpc_query("UpdateFile", request)
            return self._file_proto_to_data(response.result.file)

    def delete_files(
        self,
        filters: FileFilter,
        *,
        permanent: bool = False,
        force: bool = False,
    ) -> tuple[dict[str, bool], int, int]:
        """Delete multiple files from the filesystem.

        Args:
            filters: Filter criteria for the files
            permanent: Whether to permanently delete the files
            force: Whether to force delete even if files are in use

        Returns:
            tuple[dict[str, bool], int, int]: Results per file, total deleted count, total failed count
        """
        with GrpcFilesystem._handle_grpc_errors("DeleteFiles"):
            request = filesystem_pb2.DeleteFilesRequest(
                context=self.setup_id,
                filters=self._filter_to_proto(filters),
                permanent=permanent,
                force=force,
            )

            response: filesystem_pb2.DeleteFilesResponse = self.exec_grpc_query("DeleteFiles", request)
            return dict(response.results), response.total_deleted, response.total_failed

    def get_files(
        self,
        filters: FileFilter,
        *,
        list_size: int = 100,
        offset: int = 0,
        order: str | None = None,
        include_content: bool = False,
    ) -> tuple[list[FilesystemRecord], int]:
        """Get multiple files from the filesystem.

        Args:
            filters: Filter criteria for the files
            list_size: Number of files to return per page
            offset: Offset to start from
            order: Field to order results by
            include_content: Whether to include file content in response

        Returns:
            tuple[list[FilesystemRecord], int]: List of files and total count
        """
        with GrpcFilesystem._handle_grpc_errors("GetFiles"):
            request = filesystem_pb2.GetFilesRequest(
                context=self.setup_id,
                filters=self._filter_to_proto(filters),
                include_content=include_content,
                list_size=list_size,
                offset=offset,
                order=order,
            )
            response: filesystem_pb2.GetFilesResponse = self.exec_grpc_query("GetFiles", request)

            return [self._file_proto_to_data(file) for file in response.files], response.total_count
