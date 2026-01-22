"""gRPC filesystem implementation."""

from typing import Any, Literal

from agentic_mesh_protocol.filesystem.v1 import filesystem_dto_pb2, filesystem_messages_pb2, filesystem_service_pb2_grpc
from agentic_mesh_protocol.pagination.v1.pagination_pb2 import PaginationRequest
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.filesystem.filesystem_models import (
    FileFilter,
    FileStatus,
    FilesystemRecord,
    FileType,
    UploadFileData,
)
from digitalkin.services.filesystem.filesystem_strategy import (
    FilesystemServiceError,
    FilesystemStrategy,
)


class GrpcFilesystem(FilesystemStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """Default state filesystem strategy."""

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
        logger.debug("Channel client 'Filesystem' initialized successfully")

    # ═════════════════════════════════ Private Methods ══════════════════════════════════ #

    @staticmethod
    def __file_proto_to_data(file: filesystem_messages_pb2.File) -> FilesystemRecord:
        """Convert a File proto message to FilesystemRecord.

        Args:
            file: The File proto message to convert

        Returns:
            FilesystemRecord: The converted data
        """
        return FilesystemRecord(
            id=file.id,
            context=file.context,
            name=file.name,
            type=FileType.from_proto(file.type),
            content_type=file.content_type,
            size_bytes=file.size_bytes,
            checksum=file.checksum,
            metadata=MessageToDict(file.metadata),
            storage_uri=file.storage_uri,
            url=file.url,
            status=FileStatus.from_proto(file.status),
            content=file.content,
        )

    # ════════════════════════════════ Protected Methods ═════════════════════════════════ #

    @staticmethod
    def _filter_to_proto(filters: FileFilter) -> filesystem_messages_pb2.FileFilter:
        """Convert a FileFilter to a FileFilter proto message.

        Args:
            filters: The FileFilter to convert

        Returns:
            filesystem_pb2.FileFilter: The converted FileFilter proto message
        """
        return filesystem_messages_pb2.FileFilter(
            **filters.model_dump(exclude={"types", "status"}),
            types=[file_type.to_proto() for file_type in filters.types] if filters.types else None,
            status=filters.status.to_proto() if filters.status else None,
        )

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    def upload(
        self,
        files: list[UploadFileData],
    ) -> tuple[list[FilesystemRecord], int, int]:
        logger.debug("Uploading %d files", len(files))
        with self.handle_grpc_errors("UploadFiles", FilesystemServiceError):
            upload_files: list[filesystem_messages_pb2.UploadFileData] = []
            for file in files:
                metadata_struct: struct_pb2.Struct | None = None
                if file.metadata:
                    metadata_struct = struct_pb2.Struct()
                    metadata_struct.update(file.metadata)
                upload_files.append(
                    filesystem_messages_pb2.UploadFileData(
                        context=self.mission_id,
                        name=file.name,
                        type=file.type.to_proto(),
                        content_type=file.content_type or "application/octet-stream",
                        content=file.content,
                        metadata=metadata_struct,
                        status=FileStatus.UPLOADING.to_proto(),
                        replace_if_exists=file.replace_if_exists,
                    )
                )
            request = filesystem_dto_pb2.UploadFilesRequest(files=upload_files)
            response: filesystem_dto_pb2.UploadFilesResponse = self.exec_grpc_query("UploadFiles", request)
            results = [self.__file_proto_to_data(result.file) for result in response.result if result.HasField("file")]
            logger.debug("Uploaded files: %s", results)
            return results, response.bulk.total_process, response.bulk.total_failed

    def get(
        self,
        file_id: str,
        context: Literal["mission", "setup"] = "mission",
        *,
        include_content: bool = False,
    ) -> FilesystemRecord:
        match context:
            case "setup":
                context_id = self.setup_id
            case "mission":
                context_id = self.mission_id
        with self.handle_grpc_errors("GetFile", FilesystemServiceError):
            request = filesystem_dto_pb2.GetFileRequest(
                context=context_id,
                id=file_id,
                include_content=include_content,
            )

            response: filesystem_dto_pb2.GetFileResponse = self.exec_grpc_query("GetFile", request)

            return self.__file_proto_to_data(response.result.file)

    def list(
            self,
            filters: FileFilter,
            *,
            pagination=PaginationRequest(limit=100, offset=0, order=None),
            include_content: bool = False,
    ) -> tuple[list[FilesystemRecord], int]:
        match filters.context:
            case "setup":
                context_id = self.setup_id
            case "mission":
                context_id = self.mission_id
        with self.handle_grpc_errors("ListFiles", FilesystemServiceError):
            request = filesystem_dto_pb2.ListFilesRequest(
                context=context_id,
                filters=self._filter_to_proto(filters),
                include_content=include_content,
                pagination=pagination,
            )
            response: filesystem_dto_pb2.ListFilesResponse = self.exec_grpc_query("ListFiles", request)

            return [self.__file_proto_to_data(file.file) for file in response.result], response.bulk.total_process

    def delete(
            self,
            filters: FileFilter,
            *,
            permanent: bool = False,
            force: bool = False,
    ) -> tuple[dict[str, bool], int, int]:
        with self.handle_grpc_errors("DeleteFiles", FilesystemServiceError):
            request = filesystem_dto_pb2.DeleteFilesRequest(
                context=self.mission_id,
                filters=self._filter_to_proto(filters),
                permanent=permanent,
                force=force,
            )

            response: filesystem_dto_pb2.DeleteFilesResponse = self.exec_grpc_query("DeleteFiles", request)

            # Extract file IDs from FileResult objects and create results dict
            results = {file_result.file.id: True for file_result in response.result}

            return results, response.bulk.total_process, response.bulk.total_failed

    def update(
        self,
        file_id: str,
        content: bytes | None = None,
            type: FileType | None = None,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        new_name: str | None = None,
            status: FileStatus | None = None,
    ) -> FilesystemRecord:
        with self.handle_grpc_errors("UpdateFile", FilesystemServiceError):
            request = filesystem_dto_pb2.UpdateFileRequest(
                context=self.mission_id,
                id=file_id,
                content=content,
                type=type.to_proto() if type else None,
                content_type=content_type,
                new_name=new_name,
                status=status.to_proto() if status else None,
            )

            if metadata:
                request.metadata.update(metadata)

            response: filesystem_dto_pb2.UpdateFileResponse = self.exec_grpc_query("UpdateFile", request)
            return self.__file_proto_to_data(response.result.file)
