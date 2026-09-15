"""In-memory FilesystemService used to answer the client under test."""

import hashlib
import secrets

import grpc
from agentic_mesh_protocol.filesystem.v1 import (
    filesystem_dto_pb2,
    filesystem_enums_pb2,
    filesystem_messages_pb2,
    filesystem_service_pb2_grpc,
)
from agentic_mesh_protocol.pagination.v1 import bulk_pb2, pagination_pb2
from google.protobuf import struct_pb2
from tests.fixtures.grpc_fixtures import FakeContext


class MockFilesystemServicer(filesystem_service_pb2_grpc.FilesystemServiceServicer):
    """FilesystemService double following the result/bulk response contract."""

    def __init__(self) -> None:
        """Start with no stored file."""
        super().__init__()
        self.files: dict[str, dict[str, filesystem_messages_pb2.File]] = {}  # context id -> {file id: File}

    @staticmethod
    def _resolve_context(kind: int) -> str:
        """Resolve a ``FileContext`` kind to the concrete test id, as the server does from metadata.

        Args:
            kind: ``FileContext`` value from the request.

        Returns:
            The concrete context id.
        """
        return "setups:1" if kind == filesystem_enums_pb2.SETUPS else "missions:test_mission"

    @staticmethod
    def error(identifier: str, code: str, message: str) -> filesystem_messages_pb2.FileResult:
        """Build a ``FileResult`` holding an ``OperationError``.

        Args:
            identifier: The processed item identifier.
            code: The error code.
            message: The error message.

        Returns:
            The error result.
        """
        return filesystem_messages_pb2.FileResult(
            identifier=identifier, error=bulk_pb2.OperationError(code=code, message=message)
        )

    def seed(self, *names: str) -> list[str]:
        """Store mission DOCUMENT files directly, bypassing the client.

        Args:
            names: The names of the files to store.

        Returns:
            The ids of the stored files.
        """
        metadata = struct_pb2.Struct()
        metadata.update({"key": "value"})
        request = filesystem_dto_pb2.UploadFilesRequest(
            files=[
                filesystem_messages_pb2.UploadFileData(
                    context=filesystem_enums_pb2.MISSIONS,
                    name=name,
                    type=filesystem_enums_pb2.DOCUMENT,
                    status=filesystem_enums_pb2.UPLOADING,
                    content_type="text/plain",
                    content=b"content",
                    metadata=metadata,
                )
                for name in names
            ]
        )
        return [result.file.id for result in self.UploadFiles(request, FakeContext()).results]

    @staticmethod
    def _matches(file: filesystem_messages_pb2.File, file_filter: filesystem_messages_pb2.FileFilter) -> bool:
        """Check a stored file against a wire filter; an empty criterion matches everything.

        Args:
            file: The stored file.
            file_filter: The request filter.

        Returns:
            Whether the file matches every criterion.
        """
        return (
                (not file_filter.ids or file.id in file_filter.ids)
                and (not file_filter.names or file.name in file_filter.names)
                and (not file_filter.types or file.type in file_filter.types)
                and (not file_filter.status or file.status == file_filter.status)
                and (not file_filter.visibilities or file.visibility in file_filter.visibilities)
                and (not file_filter.prefix or file.name.startswith(file_filter.prefix))
                and (not file_filter.content_type or file.content_type == file_filter.content_type)
                and (not file_filter.content_type_prefix or file.content_type.startswith(
            file_filter.content_type_prefix))
        )

    def UploadFiles(
            self, request: filesystem_dto_pb2.UploadFilesRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.UploadFilesResponse:
        """Store every file; a duplicate name without ``replace_if_exists`` fails on its own.

        Returns:
            One result per file and the batch summary.
        """
        results = []
        for data in request.files:
            context = self._resolve_context(data.context)
            stored = self.files.setdefault(context, {})
            if not data.replace_if_exists and any(file.name == data.name for file in stored.values()):
                results.append(self.error(data.name, "ALREADY_EXISTS", f"File {data.name} already exists"))
                continue
            file = filesystem_messages_pb2.File(
                id=f"files:{secrets.token_hex(8)}",
                context=context,
                name=data.name,
                type=data.type,
                status=data.status,
                visibility=data.visibility,
                content_type=data.content_type,
                size_bytes=len(data.content),
                checksum=hashlib.sha256(data.content).hexdigest(),
                storage_uri=f"gs://test-bucket/{context}/{data.name}",
                url=f"https://storage.example.com/{context}/{data.name}",
                metadata=data.metadata,
            )
            stored[file.id] = file
            results.append(filesystem_messages_pb2.FileResult(identifier=file.id, file=file))
        return filesystem_dto_pb2.UploadFilesResponse(
            results=results,
            bulk=bulk_pb2.BulkResponse(
                total_processed=len(results),
                total_failed=sum(result.WhichOneof("outcome") == "error" for result in results),
            ),
        )

    def GetFile(
            self, request: filesystem_dto_pb2.GetFileRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.GetFileResponse:
        """Return the file, or a NOT_FOUND ``OperationError``.

        Returns:
            The file result.
        """
        file = self.files.get(self._resolve_context(request.context), {}).get(request.file_id)
        if file is None:
            return filesystem_dto_pb2.GetFileResponse(
                result=self.error(request.file_id, "NOT_FOUND", f"File {request.file_id} does not exist")
            )
        return filesystem_dto_pb2.GetFileResponse(
            result=filesystem_messages_pb2.FileResult(identifier=file.id, file=file)
        )

    def ListFiles(
            self, request: filesystem_dto_pb2.ListFilesRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.ListFilesResponse:
        """Return one page of the files matching the filter.

        Returns:
            The page of results and the listing summary.
        """
        matched = [
            file
            for file in self.files.get(self._resolve_context(request.context), {}).values()
            if self._matches(file, request.filter)
        ]
        offset = request.pagination.offset
        page = matched[offset: offset + (request.pagination.limit or 100)]
        return filesystem_dto_pb2.ListFilesResponse(
            results=[filesystem_messages_pb2.FileResult(identifier=file.id, file=file) for file in page],
            bulk=bulk_pb2.BulkResponse(
                total_processed=len(page),
                pagination=pagination_pb2.PaginationResponse(total_count=len(matched)),
            ),
        )

    def UpdateFile(
            self, request: filesystem_dto_pb2.UpdateFileRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.UpdateFileResponse:
        """Apply the fields present on the request; absent fields stay unchanged.

        Returns:
            The updated file, or a NOT_FOUND ``OperationError``.
        """
        file = self.files.get(self._resolve_context(request.context), {}).get(request.file_id)
        if file is None:
            return filesystem_dto_pb2.UpdateFileResponse(
                result=self.error(request.file_id, "NOT_FOUND", f"File {request.file_id} does not exist")
            )
        if request.HasField("new_name"):
            file.name = request.new_name
        if request.HasField("content"):
            file.size_bytes = len(request.content)
            file.checksum = hashlib.sha256(request.content).hexdigest()
        if request.HasField("content_type"):
            file.content_type = request.content_type
        if request.HasField("type"):
            file.type = request.type
        if request.HasField("status"):
            file.status = request.status
        if request.visibility:
            file.visibility = request.visibility
        if request.HasField("metadata"):
            file.metadata.MergeFrom(request.metadata)
        return filesystem_dto_pb2.UpdateFileResponse(
            result=filesystem_messages_pb2.FileResult(identifier=file.id, file=file)
        )

    def DeleteFiles(
            self, request: filesystem_dto_pb2.DeleteFilesRequest, grpc_context: grpc.ServicerContext
    ) -> filesystem_dto_pb2.DeleteFilesResponse:
        """Delete (or mark DELETED) every file matching the filter.

        Returns:
            One result per matched file and the batch summary.
        """
        stored = self.files.get(self._resolve_context(request.context), {})
        results = []
        for file in [file for file in stored.values() if self._matches(file, request.filter)]:
            if request.permanent:
                del stored[file.id]
            else:
                file.status = filesystem_enums_pb2.DELETED
            results.append(filesystem_messages_pb2.FileResult(identifier=file.id, file=file))
        return filesystem_dto_pb2.DeleteFilesResponse(
            results=results, bulk=bulk_pb2.BulkResponse(total_processed=len(results))
        )
