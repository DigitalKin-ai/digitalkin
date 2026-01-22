"""Test file for Filesystem Servicer from the client side."""

import secrets
import string
from datetime import datetime, timezone
from typing import Any

import grpc
from agentic_mesh_protocol.filesystem.v1 import (
    filesystem_messages_pb2,
    filesystem_service_pb2_grpc, filesystem_dto_pb2,
)
from agentic_mesh_protocol.pagination.v1 import bulk_pb2
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict
from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.models.services.filesystem import FilesystemRecord, FileFilter, FileType, FileStatus


class MockFilesystemServicer(filesystem_service_pb2_grpc.FilesystemServiceServicer):
    """Implementation of the MockFilesystemServicer."""

    alphabet = string.ascii_letters + string.digits

    def __init__(self) -> None:
        """Initialize the filesystem servicer with an empty files dictionary."""
        super().__init__()
        self.files: dict[str, dict[str, FilesystemRecord]] = {}  # context -> {id: file_data}

    @staticmethod
    def __model_to_proto(model: dict[str, Any]) -> filesystem_messages_pb2.File:
        """Convert a database model to a proto message.

        Args:
            model: The database model

        Returns:
            File: The proto message
        """

        metadata = struct_pb2.Struct()
        if model.get("metadata"):
            metadata.update(model["metadata"])
        return filesystem_messages_pb2.File(
            id=str(model.get("id")) if model.get("id") else "",
            context=str(model.get("context")) if model.get("context") else "",
            name=model.get("name"),
            type=model["type"].to_proto(),
            content_type=model.get("content_type"),
            size_bytes=model.get("size_bytes"),
            checksum=model.get("checksum"),
            metadata=metadata,
            storage_uri=model.get("storage_uri"),
            url=model.get("url"),
            status=model["status"].to_proto(),
        )

    def __generate_url(self, context: str, name: str) -> str:
        """Generate a fake URL for a file.

        Args:
            context: The context of the file
            name: The name of the file

        Returns:
            str: A fake URL for the file
        """
        random_id = "".join(secrets.choice(self.alphabet) for _ in range(8))
        return f"https://storage.example.com/{context}/{random_id}/{name}"

    @staticmethod
    def __matches_filters(file_data: FilesystemRecord, filters: FileFilter) -> bool:
        """Check if a file matches the given filters.

        Args:
            file_data: The file data to check
            filters: The filter criteria

        Returns:
            bool: True if the file matches all filters, False otherwise
        """
        if filters.names and file_data.name not in filters.names:
            return False
        if filters.ids and file_data.id not in filters.ids:
            return False
        if filters.types and file_data.type not in filters.types:
            return False
        if filters.status and file_data.status != filters.status:
            return False
        if filters.content_type_prefix and not file_data.content_type.startswith(filters.content_type_prefix):
            return False
        if filters.min_size_bytes and file_data.size_bytes < filters.min_size_bytes:
            return False
        if filters.max_size_bytes and file_data.size_bytes > filters.max_size_bytes:
            return False
        if filters.prefix and not file_data.name.startswith(filters.prefix):
            return False
        return not (filters.content_type and file_data.content_type != filters.content_type)

    def UploadFiles(
            self, request: filesystem_dto_pb2.UploadFilesRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.UploadFilesResponse:
        """Upload multiple files to the mock filesystem.

        Args:
            request: The UploadFilesRequest containing the files to upload
            grpc_context: The gRPC context

        Returns:
            filesystem_pb2.UploadFilesResponse: The response containing the uploaded files
        """
        try:
            results = []
            total_uploaded = 0
            total_failed = 0

            for file_data in request.files:
                context = file_data.context
                name = file_data.name

                # Initialize the context dict if it doesn't exist
                if context not in self.files:
                    self.files[context] = {}

                # Check if file already exists
                if name in self.files[context] and not file_data.replace_if_exists:
                    msg = f"File {name} already exists in context {context}"
                    logger.warning(msg)
                    grpc_context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                    grpc_context.set_details(msg)
                    results.append(filesystem_messages_pb2.FileResult(error=msg))
                    total_failed += 1
                    continue

                try:
                    # Create the file data
                    url = self.__generate_url(context, name)
                    file_id = secrets.token_hex(16)
                    datetime.now(timezone.utc)
                    file_data_obj = FilesystemRecord(
                        id=file_id,
                        context=context,
                        name=name,
                        type=FileType.from_proto(file_data.type),
                        content_type=file_data.content_type or "application/octet-stream",
                        size_bytes=len(file_data.content),
                        checksum=secrets.token_hex(32),  # Mock checksum
                        metadata=MessageToDict(file_data.metadata) if file_data.HasField("metadata") else None,
                        storage_uri=url,
                        url=url,
                        status=FileStatus.from_proto(file_data.status),
                    )

                    # Store the file
                    self.files[context][file_id] = file_data_obj
                    logger.debug(f"Uploaded file {name} to context {context}")
                    file_proto = self.__model_to_proto(file_data_obj.model_dump())
                    results.append(filesystem_messages_pb2.FileResult(file=file_proto))
                    total_uploaded += 1

                except Exception as e:
                    msg = f"Error uploading file {name}: {e!s}"
                    logger.exception(msg)
                    results.append(filesystem_messages_pb2.FileResult(error=msg))
                    total_failed += 1

            bulk = bulk_pb2.BulkResponse(total_process=total_uploaded, total_failed=total_failed)
            return filesystem_dto_pb2.UploadFilesResponse(
                result=results,
                bulk=bulk
            )

        except ValidationError as e:
            msg = f"Validation error: {e!s}"
            logger.exception(msg)
            grpc_context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            grpc_context.set_details(msg)
            return filesystem_dto_pb2.UploadFilesResponse()
        except Exception as e:
            msg = f"Unexpected error in UploadFiles: {e!s}"
            logger.exception(msg)
            grpc_context.set_code(grpc.StatusCode.INTERNAL)
            grpc_context.set_details(msg)
            return filesystem_dto_pb2.UploadFilesResponse()

    def GetFile(
            self, request: filesystem_dto_pb2.GetFileRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.GetFileResponse:
        """Get a file by ID from the mock filesystem.

        Args:
            request: The GetFileRequest containing the ID of the file to get
            grpc_context: The gRPC context

        Returns:
            filesystem_pb2.GetFileResponse: The response containing the file
        """
        try:
            context = request.context
            file_id = request.id

            # Check if context exists
            if context not in self.files:
                msg = f"Context {context} does not exist"
                logger.warning(msg)
                grpc_context.set_code(grpc.StatusCode.NOT_FOUND)
                grpc_context.set_details(msg)
                result = filesystem_messages_pb2.FileResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND), message=msg),
                                                            success=False)
                return filesystem_dto_pb2.GetFileResponse(result=result)

            # Check if file exists
            if file_id not in self.files[context]:
                msg = f"File with ID {file_id} does not exist in context {context}"
                logger.warning(msg)
                grpc_context.set_code(grpc.StatusCode.NOT_FOUND)
                result = filesystem_messages_pb2.FileResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND), message=msg),
                                                            success=False)
                return filesystem_dto_pb2.GetFileResponse(result=result)

            # Return the file
            file_data = self.files[context][file_id]
            file_proto = self.__model_to_proto(file_data.model_dump())
            result = filesystem_messages_pb2.FileResult(file=file_proto, success=True)

            return filesystem_dto_pb2.GetFileResponse(result=result)
        except Exception as e:
            msg = f"Unexpected error in GetFile: {e!s}"
            logger.exception(msg)
            grpc_context.set_code(grpc.StatusCode.INTERNAL)
            grpc_context.set_details(msg)
            result = filesystem_messages_pb2.FileResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL), message=msg),
                                                        success=False)
            return filesystem_dto_pb2.GetFileResponse(result=result)

    def ListFiles(
            self, request: filesystem_dto_pb2.ListFilesRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.ListFilesResponse:
        """Get files based on filter criteria.

        Args:
            request: The GetFilesRequest containing filter criteria
            grpc_context: The gRPC context

        Returns:
            filesystem_pb2.GetFilesResponse: The response containing matching files
        """
        try:
            context = request.context
            filters = FileFilter(**MessageToDict(request.filters))

            # Check if context exists
            if context not in self.files:
                # Return empty list rather than error, as this is a common case
                logger.debug(f"Context {context} does not exist or is empty")
                bulk = bulk_pb2.BulkResponse(total_process=0, total_failed=0)
                return filesystem_dto_pb2.ListFilesResponse(result=[], bulk=bulk)

            # Apply filters
            filtered_files = []
            logger.info(f"Filters: {filters}")
            logger.info(f"Files: {self.files[context]}")
            for file_data in self.files[context].values():
                if self.__matches_filters(file_data, filters):
                    file_proto = self.__model_to_proto(file_data.model_dump())
                    filtered_files.append(file_proto)

            # Apply pagination
            total_count = len(filtered_files)
            start_idx = request.pagination.offset
            end_idx = start_idx + request.pagination.limit
            paginated_files = filtered_files[start_idx:end_idx]

            result_files = [filesystem_messages_pb2.FileResult(file=file, identifier='1') for file in paginated_files]
            bulk = bulk_pb2.BulkResponse(total_process=total_count, total_failed=0)
            return filesystem_dto_pb2.ListFilesResponse(result=result_files, bulk=bulk)
        except Exception as e:
            msg = f"Unexpected error in GetFiles: {e!s}"
            logger.exception(msg)
            grpc_context.set_code(grpc.StatusCode.INTERNAL)
            grpc_context.set_details(msg)
            bulk = bulk_pb2.BulkResponse(total_process=0, total_failed=0)
            return filesystem_dto_pb2.ListFilesResponse(result=[], bulk=bulk)

    def UpdateFile(
            self, request: filesystem_dto_pb2.UpdateFileRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.UpdateFileResponse:
        """Update a file in the mock filesystem.

        Args:
            request: The UpdateFileRequest containing the file to update
            grpc_context: The gRPC context
        Returns:
            filesystem_pb2.UpdateFileResponse: The response containing the updated file
        """
        try:
            context = request.context
            file_id = request.id

            # Check if context exists
            if context not in self.files:
                msg = f"Context {context} does not exist"
                logger.warning(msg)
                grpc_context.set_code(grpc.StatusCode.NOT_FOUND)
                grpc_context.set_details(msg)
                return filesystem_dto_pb2.UpdateFileResponse()

            # Check if file exists
            if file_id not in self.files[context]:
                msg = f"File with ID {file_id} does not exist in context {context}"
                logger.warning(msg)
                grpc_context.set_code(grpc.StatusCode.NOT_FOUND)
                grpc_context.set_details(msg)
                return filesystem_dto_pb2.UpdateFileResponse()

            # Update the file data
            file_data = self.files[context][file_id]
            if request.content:
                file_data.size_bytes = len(request.content)
                file_data.checksum = secrets.token_hex(32)  # Mock checksum
            if request.type:
                file_data.type = FileType.from_proto(request.type)
            if request.content_type:
                file_data.content_type = request.content_type
            if request.metadata:
                file_data.metadata = MessageToDict(request.metadata)
            if request.new_name:
                file_data.name = request.new_name
                file_data.storage_uri = self.__generate_url(context, request.new_name)
            if request.status:
                file_data.status = FileStatus.from_proto(request.status)

            # Convert to proto and return
            file_proto = self.__model_to_proto(file_data.model_dump())

            return filesystem_dto_pb2.UpdateFileResponse(result=filesystem_messages_pb2.FileResult(file=file_proto))
        except ValidationError as e:
            msg = f"Validation error: {e!s}"
            logger.exception(msg)
            grpc_context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            grpc_context.set_details(msg)
            return filesystem_dto_pb2.UpdateFileResponse()
        except Exception as e:
            msg = f"Unexpected error in UpdateFile: {e!s}"
            logger.exception(msg)
            grpc_context.set_code(grpc.StatusCode.INTERNAL)
            grpc_context.set_details(msg)
            return filesystem_dto_pb2.UpdateFileResponse()

    def DeleteFiles(
            self, request: filesystem_dto_pb2.DeleteFilesRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.DeleteFilesResponse:
        """Delete multiple files from the mock filesystem.

        Args:
            request: The DeleteFilesRequest containing filter criteria
            grpc_context: The gRPC context

        Returns:
            filesystem_pb2.DeleteFilesResponse: The response indicating success or failure
        """
        try:
            context = request.context
            filters = FileFilter(**MessageToDict(request.filters))
            permanent = request.permanent

            # Check if context exists
            if context not in self.files:
                msg = f"Context {context} does not exist"
                logger.warning(msg)
                grpc_context.set_code(grpc.StatusCode.NOT_FOUND)
                grpc_context.set_details(msg)
                return filesystem_dto_pb2.DeleteFilesResponse()

            results = {}
            total_deleted = 0
            total_failed = 0
            deleted_files = []  # Store file data for response

            # Find files matching the filters
            files_to_delete = []
            for file_id, file_data in self.files[context].items():
                if self.__matches_filters(file_data, filters):
                    files_to_delete.append((file_id, file_data))

            # Delete the files
            for file_id, file_data in files_to_delete:
                try:
                    # Store file proto before deletion for response
                    file_proto = self.__model_to_proto(file_data.model_dump())
                    deleted_files.append(file_proto)

                    if permanent:
                        del self.files[context][file_id]
                    else:
                        self.files[context][file_id].status = FileStatus.DELETED
                    results[file_id] = True
                    total_deleted += 1
                except Exception as e:
                    msg = f"Error deleting file {file_id}: {e!s}"
                    logger.exception(msg)
                    results[file_id] = False
                    total_failed += 1

            bulk = bulk_pb2.BulkResponse(total_process=total_deleted, total_failed=total_failed)
            file_result = [filesystem_messages_pb2.FileResult(file=file, identifier='-1') for file in deleted_files]
            return filesystem_dto_pb2.DeleteFilesResponse(
                result=file_result,
                bulk=bulk
            )
        except Exception as e:
            msg = f"Unexpected error in DeleteFiles: {e!s}"
            logger.exception(msg)
            grpc_context.set_code(grpc.StatusCode.INTERNAL)
            grpc_context.set_details(msg)
            return filesystem_dto_pb2.DeleteFilesResponse()
