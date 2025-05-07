"""Grpc filesystem."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from digitalkin_proto.digitalkin.filesystem.v2 import filesystem_pb2, filesystem_service_pb2_grpc
from digitalkin_proto.digitalkin.filesystem.v2.filesystem_pb2 import FileType as FileTypeProto

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.models import ClientConfig
from digitalkin.services.filesystem.filesystem_strategy import (
    FilesystemData,
    FilesystemServiceError,
    FilesystemStrategy,
    FileType,
)

logger = logging.getLogger(__name__)


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

    def __init__(
        self,
        mission_id: str,
        setup_version_id: str,
        config: dict[str, str],
        client_config: ClientConfig,
        **kwargs,  # noqa: ANN003, ARG002
    ) -> None:
        """Initialize the default filesystem strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_version_id: The ID of the setup version this strategy is associated with
            config: A dictionary mapping names to Pydantic model classes
            client_config: The server configuration object
            kwargs: other optional arguments to pass to the parent class constructor
        """
        super().__init__(mission_id, setup_version_id, config)
        channel = self._init_channel(client_config)
        self.stub = filesystem_service_pb2_grpc.FilesystemServiceStub(channel)
        logger.info("Channel client 'Filesystem' initialized succesfully")

    def upload(self, content: bytes, name: str, file_type: FileType) -> FilesystemData:
        """Create a new file in the file system.

        Args:
            content: The content of the file to be uploaded
            name: The name of the file to be created
            file_type: The type of data being uploaded

        Returns:
            FilesystemData: Metadata about the uploaded file

        Raises:
            ValueError: If the file already exists
        """
        with GrpcFilesystem._handle_grpc_errors("UploadFile"):
            request = filesystem_pb2.UploadFileRequest(
                kin_context=self.mission_id,
                name=name,
                file_type=file_type.name,
                content=content,
            )
            response: filesystem_pb2.UploadFileResponse = self.exec_grpc_query("UploadFile", request)
            return FilesystemData(
                kin_context=response.file.kin_context,
                name=response.file.name,
                file_type=FileType[FileTypeProto.Name(response.file.file_type)],
                url=response.file.url,
            )

    def get(self, name: str) -> FilesystemData:
        """Get file from the filesystem.

        Args:
            name: The name of the file to be retrieved

        Returns:
            FilesystemData: Metadata about the retrieved file
        """
        with GrpcFilesystem._handle_grpc_errors("GetFileByName"):
            request = filesystem_pb2.GetFileByNameRequest(name=name)
            response: filesystem_pb2.GetFileByNameResponse = self.exec_grpc_query("GetFileByName", request)
            return FilesystemData(
                kin_context=response.file.kin_context,
                name=response.file.name,
                file_type=FileType[FileTypeProto.Name(response.file.file_type)],
                url=response.file.url,
            )

    def update(self, name: str, content: bytes, file_type: FileType) -> FilesystemData:
        """Update files in the filesystem.

        Args:
            name: The name of the file to be updated
            content: The new content of the file
            file_type: The type of data being uploaded

        Returns:
            FilesystemData: Metadata about the updated file
        """
        with GrpcFilesystem._handle_grpc_errors("UpdateFile"):
            request = filesystem_pb2.UpdateFileRequest(
                kin_context=self.mission_id,
                name=name,
                file_type=file_type.name,
                content=content,
            )
            response: filesystem_pb2.UpdateFileResponse = self.exec_grpc_query("UpdateFile", request)
            return FilesystemData(
                kin_context=response.file.kin_context,
                name=response.file.name,
                file_type=FileType[FileTypeProto.Name(response.file.file_type)],
                url=response.file.url,
            )

    def delete(self, name: str) -> bool:
        """Delete files from the filesystem.

        Args:
            name: The name of the file to be deleted

        Returns:
            int: 1 if the file was deleted successfully, 0 if it didn't exist, None on error
        """
        with GrpcFilesystem._handle_grpc_errors("DeleteFile"):
            request = filesystem_pb2.DeleteFileRequest(name=name)
            _: filesystem_pb2.DeleteFileResponse = self.exec_grpc_query("DeleteFile", request)
            return True

    def get_all(self) -> list[FilesystemData]:
        """Get all files from the filesystem.

        Returns:
            list[FilesystemData]: A list of all files in the filesystem
        """
        with GrpcFilesystem._handle_grpc_errors("GetFilesByKinContext"):
            request = filesystem_pb2.GetFilesByKinContextRequest(kin_context=self.mission_id)
            response: filesystem_pb2.GetFilesByKinContextResponse = self.exec_grpc_query(
                "GetFilesByKinContext", request
            )
            return [
                FilesystemData(
                    kin_context=file.kin_context,
                    name=file.name,
                    file_type=FileType[FileTypeProto.Name(file.file_type)],
                    url=file.url,
                )
                for file in response.files
            ]

    def get_batch(self, names: list[str]) -> dict[str, FilesystemData | None]:
        """Get files from the filesystem.

        Args:
            names: The names of the files to be retrieved

        Returns:
            list[FilesystemData]: A list of metadata about the retrieved files
        """
        with GrpcFilesystem._handle_grpc_errors("GetFilesByNames"):
            request = filesystem_pb2.GetFilesByNamesRequest(names=names)
            response: filesystem_pb2.GetFilesByNamesResponse = self.exec_grpc_query("GetFilesByNames", request)
            result: dict[str, FilesystemData | None] = {}
            for name, file_result in response.files.items():
                which_field = file_result.WhichOneof("result")
                if which_field == "file":
                    result[name] = FilesystemData(
                        kin_context=file_result.file.kin_context,
                        name=file_result.file.name,
                        file_type=FileType[FileTypeProto.Name(file_result.file.file_type)],
                        url=file_result.file.url,
                    )
                elif which_field == "error":
                    # Handle error case
                    result[name] = None
                    logger.warning("Error retrieving file '%s': %s", name, file_result.error)
            return result
