"""gRPC filesystem implementation."""

from typing import Any, Literal, cast

from agentic_mesh_protocol.common.v1 import common_enums_pb2
from agentic_mesh_protocol.filesystem.v1 import (
    filesystem_dto_pb2,
    filesystem_enums_pb2,
    filesystem_messages_pb2,
    filesystem_service_pb2_grpc,
)
from agentic_mesh_protocol.pagination.v1 import pagination_pb2
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.services import Context
from digitalkin.models.services.storage import Visibility
from digitalkin.services.filesystem.exceptions import FilesystemServiceError
from digitalkin.services.filesystem.filesystem_strategy import (
    FileFilter,
    FilesystemRecord,
    FilesystemStrategy,
    UploadFileData,
)


class GrpcFilesystem(FilesystemStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC client implementation for the Filesystem service."""

    service_name: str = "FilesystemService"

    @staticmethod
    def _file_type_to_enum(file_type: str) -> filesystem_enums_pb2.FileType:
        """Convert a file type name to the wire enum.

        Args:
            file_type: The file type name, with or without the legacy ``FILE_TYPE_`` prefix.

        Returns:
            The matching ``FileType``, ``FILE_TYPE_UNSPECIFIED`` for an unknown name.
        """
        name = file_type.upper().removeprefix("FILE_TYPE_")
        if name not in filesystem_enums_pb2.FileType.DESCRIPTOR.values_by_name:
            return filesystem_enums_pb2.FILE_TYPE_UNSPECIFIED
        return cast("filesystem_enums_pb2.FileType", filesystem_enums_pb2.FileType.Value(name))

    @staticmethod
    def _file_status_to_enum(file_status: str) -> filesystem_enums_pb2.FileStatus:
        """Convert a file status name to the wire enum.

        Args:
            file_status: The file status name, with or without the legacy ``FILE_STATUS_`` prefix.

        Returns:
            The matching ``FileStatus``, ``FILE_STATUS_UNSPECIFIED`` for an unknown name.
        """
        name = file_status.upper().removeprefix("FILE_STATUS_")
        if name not in filesystem_enums_pb2.FileStatus.DESCRIPTOR.values_by_name:
            return filesystem_enums_pb2.FILE_STATUS_UNSPECIFIED
        return cast("filesystem_enums_pb2.FileStatus", filesystem_enums_pb2.FileStatus.Value(name))

    @staticmethod
    def _file_proto_to_data(file: filesystem_messages_pb2.File) -> FilesystemRecord:
        """Convert a File proto message to FilesystemRecord.

        ``file_type`` and ``status`` keep the prefixed names (``FILE_TYPE_DOCUMENT``,
        ``FILE_STATUS_ACTIVE``) this client has always returned.

        Args:
            file: The File proto message to convert

        Returns:
            FilesystemRecord: The converted data
        """
        return FilesystemRecord(
            id=file.id,
            context=file.context,
            name=file.name,
            file_type="FILE_TYPE_" + filesystem_enums_pb2.FileType.Name(file.type).removeprefix("FILE_TYPE_"),
            content_type=file.content_type,
            size_bytes=file.size_bytes,
            checksum=file.checksum,
            metadata=MessageToDict(file.metadata),
            storage_uri=file.storage_uri,
            file_url=file.url,
            status="FILE_STATUS_" + filesystem_enums_pb2.FileStatus.Name(file.status).removeprefix("FILE_STATUS_"),
            content=file.content,
            visibility=Visibility[common_enums_pb2.Visibility.Name(file.visibility).removeprefix("VISIBILITY_")],
        )

    @staticmethod
    def _context_enum(context: Context) -> filesystem_enums_pb2.FileContext:
        """Map a context kind to the wire's context-kind enum.

        The request carries only the kind; the concrete id is resolved server-side from
        the request metadata stamped by ``RequestIdClientInterceptor``. USERS/ORGANIZATIONS
        are read-only cross-owner scopes whose owner is derived server-side as well.

        Args:
            context: The context kind.

        Returns:
            The matching ``FileContext`` wire enum, ``FILE_CONTEXT_UNSPECIFIED`` otherwise.
        """
        # TODO(validate): remove after prod validation
        # [VALIDATE CTXENUM] server resolves the concrete id from metadata
        match context:
            case Context.SETUP:
                return filesystem_enums_pb2.SETUPS
            case Context.MISSIONS:
                return filesystem_enums_pb2.MISSIONS
            case Context.USERS:
                return filesystem_enums_pb2.USERS
            case Context.ORGANIZATIONS:
                return filesystem_enums_pb2.ORGANIZATIONS
        return filesystem_enums_pb2.FILE_CONTEXT_UNSPECIFIED

    @staticmethod
    def _visibility_enum(visibility: Visibility) -> common_enums_pb2.Visibility:
        """Map an SDK ``Visibility`` to its wire enum.

        Args:
            visibility: The SDK visibility level.

        Returns:
            The matching wire enum (``VISIBILITY_UNSPECIFIED`` by default).
        """
        match visibility:
            case Visibility.PUBLIC:
                return common_enums_pb2.PUBLIC
            case Visibility.PRIVATE:
                return common_enums_pb2.PRIVATE
            case Visibility.INTERNAL:
                return common_enums_pb2.INTERNAL
            case _:
                return common_enums_pb2.VISIBILITY_UNSPECIFIED

    def _filter_to_proto(self, filters: FileFilter) -> filesystem_messages_pb2.FileFilter:
        """Convert a FileFilter to a FileFilter proto message.

        The filter context is not part of the wire filter; it rides on the request.

        Args:
            filters: The FileFilter to convert

        Returns:
            filesystem_messages_pb2.FileFilter: The converted FileFilter proto message
        """
        return filesystem_messages_pb2.FileFilter(
            **filters.model_dump(exclude={"context", "file_ids", "file_types", "status", "visibilities"}),
            ids=filters.file_ids,
            types=[self._file_type_to_enum(file_type) for file_type in filters.file_types or []],
            status=self._file_status_to_enum(filters.status or ""),
            visibilities=[self._visibility_enum(v) for v in filters.visibilities or []],
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
        self._init_channel(client_config)
        self.stub = self._get_or_create_stub(filesystem_service_pb2_grpc.FilesystemServiceStub)
        logger.debug("Channel client 'Filesystem' initialized successfully")

    async def close(self) -> None:
        """Release this instance's pooled gRPC channel ref."""
        await self.close_channel()

    async def upload_files(
        self,
        files: list[UploadFileData],
    ) -> tuple[list[FilesystemRecord], int, int]:
        """Upload multiple files to the filesystem.

        Files the service rejects are logged and left out of the returned list.

        Args:
            files: List of tuples containing (content, name, file_type, content_type, metadata, replace_if_exists)

        Returns:
            tuple[list[FilesystemRecord], int, int]: List of uploaded files, total uploaded count, total failed count
        """
        logger.debug("Uploading %d files", len(files))
        async with self.handle_grpc_errors("UploadFiles", FilesystemServiceError):
            upload_files: list[filesystem_messages_pb2.UploadFileData] = []
            for file in files:
                metadata_struct: struct_pb2.Struct | None = None
                if file.metadata:
                    metadata_struct = struct_pb2.Struct()
                    metadata_struct.update(file.metadata)
                upload_files.append(
                    filesystem_messages_pb2.UploadFileData(
                        context=filesystem_enums_pb2.MISSIONS,
                        name=file.name,
                        type=self._file_type_to_enum(file.file_type),
                        content_type=file.content_type or "application/octet-stream",
                        content=file.content,
                        metadata=metadata_struct,
                        status=filesystem_enums_pb2.UPLOADING,
                        replace_if_exists=file.replace_if_exists,
                        visibility=self._visibility_enum(file.visibility),
                    )
                )
            request = filesystem_dto_pb2.UploadFilesRequest(files=upload_files)
            response: filesystem_dto_pb2.UploadFilesResponse = await self.exec_grpc_query("UploadFiles", request)
            results = [
                self._file_proto_to_data(result.file)
                for result in self.successful_results("UploadFiles", response.results)
            ]
            logger.debug("Uploaded files: %s", results)
            return results, response.bulk.total_processed - response.bulk.total_failed, response.bulk.total_failed

    async def get_file(
        self,
        file_id: str,
        context: Context = Context.MISSIONS,
        *,
        include_content: bool = False,
    ) -> FilesystemRecord:
        """Get a file from the filesystem.

        Args:
            file_id: The ID of the file to be retrieved
            context: The context of the file (mission/setup, or user/organization for cross-owner reads)
            include_content: Whether to include file content in response

        Returns:
            FilesystemRecord: Metadata about the retrieved file

        Raises:
            FilesystemServiceError: If there is an error retrieving the file
        """
        logger.debug("debug:get_file file_id=%s context=%s", file_id, context)
        async with self.handle_grpc_errors("GetFile", FilesystemServiceError):
            request = filesystem_dto_pb2.GetFileRequest(
                context=self._context_enum(context),
                file_id=file_id,
                include_content=include_content,
            )
            response: filesystem_dto_pb2.GetFileResponse = await self.exec_grpc_query("GetFile", request)
            self.raise_on_error(response.result, FilesystemServiceError)
            return self._file_proto_to_data(response.result.file)

    async def update_file(
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
        visibility: Visibility = Visibility.UNSPECIFIED,
    ) -> FilesystemRecord:
        """Update a file in the filesystem.

        Only the given fields are sent; every other field is left unchanged by the service.

        Args:
            file_id: The id of the file to be updated
            content: Optional new content of the file
            file_type: Optional new type of data; UNSPECIFIED leaves it unchanged
            content_type: Optional new MIME type
            metadata: Optional new metadata (will merge with existing)
            new_name: Optional new name for the file
            status: Optional new status for the file; an unknown status leaves it unchanged
            visibility: Optional new read-access scope; UNSPECIFIED leaves it unchanged

        Returns:
            FilesystemRecord: Metadata about the updated file

        Raises:
            FilesystemServiceError: If there is an error during update
        """
        async with self.handle_grpc_errors("UpdateFile", FilesystemServiceError):
            request = filesystem_dto_pb2.UpdateFileRequest(
                context=filesystem_enums_pb2.MISSIONS,
                file_id=file_id,
                new_name=new_name,
                content=content,
                content_type=content_type,
                type=self._file_type_to_enum(file_type or "") or None,
                status=self._file_status_to_enum(status or "") or None,
                visibility=self._visibility_enum(visibility),
            )
            if metadata:
                request.metadata.update(metadata)

            response: filesystem_dto_pb2.UpdateFileResponse = await self.exec_grpc_query("UpdateFile", request)
            self.raise_on_error(response.result, FilesystemServiceError)
            return self._file_proto_to_data(response.result.file)

    async def delete_files(
        self,
        filters: FileFilter,
        *,
        permanent: bool = False,
        force: bool = False,
    ) -> tuple[dict[str, bool], int, int]:
        """Delete multiple files from the filesystem.

        The service only accepts a filter naming the files by ids, names or prefix.

        Args:
            filters: Filter criteria for the files
            permanent: Whether to permanently delete the files
            force: Whether to force delete even if files are in use

        Returns:
            tuple[dict[str, bool], int, int]: Results per file, total deleted count, total failed count
        """
        logger.debug("debug:delete_files permanent=%s force=%s", permanent, force)
        async with self.handle_grpc_errors("DeleteFiles", FilesystemServiceError):
            request = filesystem_dto_pb2.DeleteFilesRequest(
                context=filesystem_enums_pb2.MISSIONS,
                filter=self._filter_to_proto(filters),
                permanent=permanent,
                force=force,
            )
            response: filesystem_dto_pb2.DeleteFilesResponse = await self.exec_grpc_query("DeleteFiles", request)
            deleted = {result.identifier for result in self.successful_results("DeleteFiles", response.results)}
            return (
                {result.identifier: result.identifier in deleted for result in response.results},
                response.bulk.total_processed - response.bulk.total_failed,
                response.bulk.total_failed,
            )

    async def get_files(
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
            list_size: Number of files to return per page (1 to 100)
            offset: Offset to start from
            order: Field to order results by, optionally suffixed ``:desc`` (e.g. ``created_at:desc``)
            include_content: Whether to include file content in response

        Returns:
            tuple[list[FilesystemRecord], int]: List of files and total count
        """
        async with self.handle_grpc_errors("ListFiles", FilesystemServiceError):
            order_field, _, direction = (order or "").partition(":")
            request = filesystem_dto_pb2.ListFilesRequest(
                context=self._context_enum(filters.context),
                filter=self._filter_to_proto(filters),
                include_content=include_content,
                pagination=pagination_pb2.PaginationRequest(
                    order=order_field,
                    descending=direction.lower() == "desc",
                    limit=list_size,
                    offset=offset,
                ),
            )
            response: filesystem_dto_pb2.ListFilesResponse = await self.exec_grpc_query("ListFiles", request)
            return [
                self._file_proto_to_data(result.file)
                for result in self.successful_results("ListFiles", response.results)
            ], response.bulk.pagination.total_count
