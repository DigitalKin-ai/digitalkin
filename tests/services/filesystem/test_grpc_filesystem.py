"""Test the grpc filesystem service."""

import asyncio
import logging
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from typing import Any
from unittest.mock import AsyncMock

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.common.v1 import common_enums_pb2
from agentic_mesh_protocol.filesystem.v1 import (
    filesystem_dto_pb2,
    filesystem_enums_pb2,
    filesystem_messages_pb2,
    filesystem_service_pb2,
    filesystem_service_pb2_grpc,
)
from agentic_mesh_protocol.pagination.v1 import bulk_pb2, pagination_pb2
from grpc.framework.foundation import logging_pool
from hypothesis import given
from hypothesis import strategies as st
from mock_filesystem_servicer import MockFilesystemServicer
from tests.fixtures.grpc_fixtures import FakeContext

from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.services import Context
from digitalkin.models.services.storage import Visibility
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.filesystem.exceptions import FilesystemServiceError
from digitalkin.services.filesystem.filesystem_strategy import (
    FileFilter,
    FilesystemRecord,
    UploadFileData,
)
from digitalkin.services.filesystem.grpc_filesystem import GrpcFilesystem

service_name = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
client_execution_thread_pool = logging_pool.pool(max_workers=10)

Start = Callable[[Coroutine[Any, Any, Any], str], tuple[Future, Any, Any]]


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Mock a gRPC channel.

    Returns:
        Mock gRPC Channel
    """
    return grpc_testing.channel([service_name], grpc_testing.strict_real_time())


@pytest.fixture
def mock_servicer() -> MockFilesystemServicer:
    """Return an instance of the mock servicer.

    Returns:
        Mock Filesystem Servicer
    """
    return MockFilesystemServicer()


@pytest.fixture
def client(test_channel: grpc_testing.Channel) -> GrpcFilesystem:
    """Instantiate a GrpcFilesystem client that uses the test channel.

    Returns:
        gRPC client as GrpcFilesystem
    """
    dummy_config = ClientConfig(
        host="[::]",
        port=50151,
        mode=ControlFlow.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )
    client = GrpcFilesystem("test_mission", "setup:1", "setup_version:1", dummy_config)
    client.stub = filesystem_service_pb2_grpc.FilesystemServiceStub(test_channel)

    async def _test_exec_grpc_query(query_endpoint: str, request: Any) -> Any:  # ruff: ignore[unused-async]
        return getattr(client.stub, query_endpoint)(request)

    client.exec_grpc_query = _test_exec_grpc_query  # type: ignore[method-assign]
    return client


@pytest.fixture
def start(test_channel: grpc_testing.Channel) -> Start:
    """Run a client coroutine in a thread and intercept its single RPC.

    Returns:
        A callable returning the pending future, the intercepted request and the RPC to terminate.
    """

    def _start(coro: Coroutine[Any, Any, Any], method: str) -> tuple[Future, Any, Any]:
        future = client_execution_thread_pool.submit(asyncio.run, coro)
        _, request, rpc = test_channel.take_unary_unary(service_name.methods_by_name[method])
        rpc.send_initial_metadata(())
        return future, request, rpc

    return _start


@pytest.fixture
def sample_file_data() -> bytes:
    """Generate sample file data for testing.

    Returns:
        bytes: Sample file data
    """
    return b"This is sample file content for testing."


class TestUploadFiles:
    """Tests for Filesystem.upload_files() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_upload_files_success(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer, sample_file_data: bytes
    ) -> None:
        """The upload request carries the renamed fields and the reply maps back onto a record."""
        upload = UploadFileData(
            content=sample_file_data,
            name="report.txt",
            file_type="DOCUMENT",
            content_type="text/plain",
            metadata={"key": "value"},
        )
        future, request, rpc = start(client.upload_files([upload]), "UploadFiles")

        sent = request.files[0]
        assert sent.context == filesystem_enums_pb2.MISSIONS
        assert sent.type == filesystem_enums_pb2.DOCUMENT
        assert sent.status == filesystem_enums_pb2.UPLOADING
        assert sent.content_type == "text/plain"
        assert sent.content == sample_file_data
        assert dict(sent.metadata) == {"key": "value"}
        assert sent.replace_if_exists is False

        rpc.terminate(mock_servicer.UploadFiles(request, FakeContext()), (), grpc.StatusCode.OK, "")
        files, total_uploaded, total_failed = future.result(timeout=5.0)

        assert (total_uploaded, total_failed) == (1, 0)
        record = files[0]
        assert isinstance(record, FilesystemRecord)
        assert record.id.startswith("files:")
        assert record.context == "missions:test_mission"
        assert record.name == "report.txt"
        assert record.file_type == "FILE_TYPE_DOCUMENT"
        assert record.status == "FILE_STATUS_UPLOADING"
        assert record.content_type == "text/plain"
        assert record.size_bytes == len(sample_file_data)
        assert record.metadata == {"key": "value"}
        assert record.storage_uri == "gs://test-bucket/missions:test_mission/report.txt"
        assert record.file_url == "https://storage.example.com/missions:test_mission/report.txt"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_upload_files_duplicate_is_dropped_and_logged(
        self,
        client: GrpcFilesystem,
            start: Start,
        mock_servicer: MockFilesystemServicer,
            monkeypatch: pytest.MonkeyPatch,
            caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A file refused with an OperationError is left out of the records and logged with its error."""
        mock_servicer.seed("dup.txt")
        upload = UploadFileData(content=b"x", name="dup.txt", file_type="DOCUMENT")
        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)

        with caplog.at_level(logging.WARNING, logger="digitalkin"):
            future, request, rpc = start(client.upload_files([upload]), "UploadFiles")
            rpc.terminate(mock_servicer.UploadFiles(request, FakeContext()), (), grpc.StatusCode.OK, "")
            files, total_uploaded, total_failed = future.result(timeout=5.0)

        assert files == []
        assert (total_uploaded, total_failed) == (0, 1)
        assert any(
            "UploadFiles dropped result dup.txt: ALREADY_EXISTS File dup.txt already exists" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_upload_files_mixed_batch(self, client: GrpcFilesystem) -> None:
        """A partially failed batch keeps the uploaded files and reports the counts from ``bulk``."""
        client.exec_grpc_query = AsyncMock(  # type: ignore[method-assign]
            return_value=filesystem_dto_pb2.UploadFilesResponse(
                results=[
                    filesystem_messages_pb2.FileResult(
                        identifier="files:ok", file=filesystem_messages_pb2.File(id="files:ok", name="ok.txt")
                    ),
                    MockFilesystemServicer.error("ko.txt", "ALREADY_EXISTS", "ko.txt failed"),
                ],
                bulk=bulk_pb2.BulkResponse(total_processed=2, total_failed=1),
            )
        )

        files, total_uploaded, total_failed = await client.upload_files([
            UploadFileData(content=b"a", name="ok.txt", file_type="DOCUMENT"),
            UploadFileData(content=b"b", name="ko.txt", file_type="DOCUMENT"),
        ])

        assert [f.id for f in files] == ["files:ok"]
        assert (total_uploaded, total_failed) == (1, 1)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_upload_files_transport_error(self, client: GrpcFilesystem, start: Start) -> None:
        """A failed UploadFiles call surfaces as FilesystemServiceError."""
        upload = UploadFileData(content=b"x", name="f.txt", file_type="DOCUMENT")
        future, _request, rpc = start(client.upload_files([upload]), "UploadFiles")
        rpc.terminate(None, (), grpc.StatusCode.INTERNAL, "gRPC error occurred")

        with pytest.raises(FilesystemServiceError):
            future.result(timeout=5.0)


class TestGetFile:
    """Tests for Filesystem.get_file() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_file_success(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer
    ) -> None:
        """get_file sends the id and kind, and decodes the File held by the result."""
        (file_id,) = mock_servicer.seed("a.txt")
        future, request, rpc = start(client.get_file(file_id, include_content=True), "GetFile")

        assert request.file_id == file_id
        assert request.context == filesystem_enums_pb2.MISSIONS
        assert request.include_content is True

        rpc.terminate(mock_servicer.GetFile(request, FakeContext()), (), grpc.StatusCode.OK, "")
        record = future.result(timeout=5.0)

        assert record.id == file_id
        assert record.name == "a.txt"
        assert record.file_type == "FILE_TYPE_DOCUMENT"
        assert record.status == "FILE_STATUS_UPLOADING"
        assert record.metadata == {"key": "value"}

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_get_file_operation_error_raises(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer
    ) -> None:
        """A result holding an OperationError raises with its code and message."""
        future, request, rpc = start(client.get_file("files:missing"), "GetFile")
        rpc.terminate(mock_servicer.GetFile(request, FakeContext()), (), grpc.StatusCode.OK, "")

        with pytest.raises(FilesystemServiceError, match="files:missing: NOT_FOUND"):
            future.result(timeout=5.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_get_file_transport_not_found(self, client: GrpcFilesystem, start: Start) -> None:
        """A NOT_FOUND status surfaces as FilesystemServiceError."""
        future, _request, rpc = start(client.get_file("files:missing"), "GetFile")
        rpc.terminate(None, (), grpc.StatusCode.NOT_FOUND, "File not found")

        with pytest.raises(FilesystemServiceError):
            future.result(timeout=5.0)


class TestGetFiles:
    """Tests for Filesystem.get_files() method (ListFiles RPC)."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_files_success(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer
    ) -> None:
        """Filter, pagination and ordering go out on ListFiles; the page and total come back."""
        file_ids = mock_servicer.seed("a.txt", "b.txt", "c.txt")
        filters = FileFilter(file_types=["DOCUMENT"], status="UPLOADING", names=["a.txt", "b.txt", "c.txt"])
        future, request, rpc = start(
            client.get_files(filters, list_size=10, offset=0, order="created_at:desc"), "ListFiles"
        )

        assert request.context == filesystem_enums_pb2.MISSIONS
        assert list(request.filter.types) == [filesystem_enums_pb2.DOCUMENT]
        assert request.filter.status == filesystem_enums_pb2.UPLOADING
        assert list(request.filter.names) == ["a.txt", "b.txt", "c.txt"]
        assert request.pagination == pagination_pb2.PaginationRequest(
            order="created_at", descending=True, limit=10, offset=0
        )

        rpc.terminate(mock_servicer.ListFiles(request, FakeContext()), (), grpc.StatusCode.OK, "")
        files, total_count = future.result(timeout=5.0)

        assert total_count == 3
        assert sorted(f.id for f in files) == sorted(file_ids)
        assert all(f.file_type == "FILE_TYPE_DOCUMENT" for f in files)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_files_pagination(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer
    ) -> None:
        """The total comes from ``bulk.pagination``, not from the page size; no order means none sent."""
        mock_servicer.seed(*(f"f{i}.txt" for i in range(5)))
        future, request, rpc = start(client.get_files(FileFilter(), list_size=2, offset=4), "ListFiles")

        assert request.pagination == pagination_pb2.PaginationRequest(limit=2, offset=4)

        rpc.terminate(mock_servicer.ListFiles(request, FakeContext()), (), grpc.StatusCode.OK, "")
        files, total_count = future.result(timeout=5.0)

        assert len(files) == 1
        assert total_count == 5

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_filter_fields_are_renamed_on_the_wire(self, client: GrpcFilesystem, start: Start) -> None:
        """``file_ids`` / ``file_types`` go out as ``ids`` / ``types``; legacy prefixed names still map."""
        filters = FileFilter(
            file_ids=["files:1"],
            file_types=["IMAGE"],
            status="FILE_STATUS_ACTIVE",
            prefix="reports/",
            content_type_prefix="image/",
            min_size_bytes=1,
            max_size_bytes=10,
        )
        future, request, rpc = start(client.get_files(filters), "ListFiles")

        assert request.filter == filesystem_messages_pb2.FileFilter(
            ids=["files:1"],
            types=[filesystem_enums_pb2.IMAGE],
            status=filesystem_enums_pb2.ACTIVE,
            prefix="reports/",
            content_type_prefix="image/",
            min_size_bytes=1,
            max_size_bytes=10,
        )

        rpc.terminate(filesystem_dto_pb2.ListFilesResponse(), (), grpc.StatusCode.OK, "")
        assert future.result(timeout=5.0) == ([], 0)

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_get_files_drops_error_results(
            self, client: GrpcFilesystem, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A listed result holding an OperationError is dropped and logged; the rest is kept."""
        client.exec_grpc_query = AsyncMock(  # type: ignore[method-assign]
            return_value=filesystem_dto_pb2.ListFilesResponse(
                results=[
                    filesystem_messages_pb2.FileResult(
                        identifier="files:ok", file=filesystem_messages_pb2.File(id="files:ok", name="ok.txt")
                    ),
                    MockFilesystemServicer.error("files:broken", "INTERNAL", "files:broken failed"),
                ],
                bulk=bulk_pb2.BulkResponse(
                    total_processed=2, total_failed=1, pagination=pagination_pb2.PaginationResponse(total_count=2)
                ),
            )
        )
        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)

        with caplog.at_level(logging.WARNING, logger="digitalkin"):
            files, total_count = await client.get_files(FileFilter())

        assert [f.id for f in files] == ["files:ok"]
        assert total_count == 2
        assert any(
            "ListFiles dropped result files:broken: INTERNAL files:broken failed" in r.getMessage()
            for r in caplog.records
        )


class TestUpdateFile:
    """Tests for Filesystem.update_file() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_update_file_success(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer
    ) -> None:
        """Every given field is sent and applied; metadata is merged by the service."""
        (file_id,) = mock_servicer.seed("a.txt")
        future, request, rpc = start(
            client.update_file(
                file_id,
                content=b"Updated content",
                file_type="IMAGE",
                content_type="image/png",
                metadata={"new_key": "new_value"},
                new_name="updated_file.png",
                status="ACTIVE",
                visibility=Visibility.PRIVATE,
            ),
            "UpdateFile",
        )

        assert request.file_id == file_id
        assert request.new_name == "updated_file.png"
        assert request.content == b"Updated content"
        assert request.content_type == "image/png"
        assert request.type == filesystem_enums_pb2.IMAGE
        assert request.status == filesystem_enums_pb2.ACTIVE
        assert request.visibility == common_enums_pb2.PRIVATE
        assert dict(request.metadata) == {"new_key": "new_value"}

        rpc.terminate(mock_servicer.UpdateFile(request, FakeContext()), (), grpc.StatusCode.OK, "")
        record = future.result(timeout=5.0)

        assert record.id == file_id
        assert record.name == "updated_file.png"
        assert record.file_type == "FILE_TYPE_IMAGE"
        assert record.status == "FILE_STATUS_ACTIVE"
        assert record.content_type == "image/png"
        assert record.size_bytes == len(b"Updated content")
        assert record.visibility is Visibility.PRIVATE
        assert record.metadata == {"key": "value", "new_key": "new_value"}

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.regression
    def test_update_file_sends_only_what_changes(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer
    ) -> None:
        """A partial update leaves every other optional field absent, so the service keeps it."""
        (file_id,) = mock_servicer.seed("a.txt")
        future, request, rpc = start(
            client.update_file(file_id, file_type="UNSPECIFIED", status="ARCHIVED"), "UpdateFile"
        )

        assert request.HasField("status")
        for field in ("new_name", "content", "content_type", "type", "metadata"):
            assert not request.HasField(field), field
        assert request.visibility == common_enums_pb2.VISIBILITY_UNSPECIFIED

        rpc.terminate(mock_servicer.UpdateFile(request, FakeContext()), (), grpc.StatusCode.OK, "")
        record = future.result(timeout=5.0)

        assert record.name == "a.txt"
        assert record.file_type == "FILE_TYPE_DOCUMENT"
        assert record.status == "FILE_STATUS_ARCHIVED"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_update_file_operation_error_raises(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer
    ) -> None:
        """A result holding an OperationError raises with its code and message."""
        future, request, rpc = start(client.update_file("files:missing", status="ACTIVE"), "UpdateFile")
        rpc.terminate(mock_servicer.UpdateFile(request, FakeContext()), (), grpc.StatusCode.OK, "")

        with pytest.raises(FilesystemServiceError, match="files:missing: NOT_FOUND"):
            future.result(timeout=5.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_update_file_transport_not_found(self, client: GrpcFilesystem, start: Start) -> None:
        """A NOT_FOUND status surfaces as FilesystemServiceError."""
        future, _request, rpc = start(client.update_file("files:missing", content=b"new"), "UpdateFile")
        rpc.terminate(None, (), grpc.StatusCode.NOT_FOUND, "File not found")

        with pytest.raises(FilesystemServiceError):
            future.result(timeout=5.0)


class TestDeleteFiles:
    """Tests for Filesystem.delete_files() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_delete_files_success(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer
    ) -> None:
        """Targeted deletion maps every result to True and reports the counts from ``bulk``."""
        file_ids = mock_servicer.seed("a.txt", "b.txt", "c.txt")
        future, request, rpc = start(client.delete_files(FileFilter(file_ids=file_ids), permanent=True), "DeleteFiles")

        assert request.context == filesystem_enums_pb2.MISSIONS
        assert list(request.filter.ids) == file_ids
        assert request.permanent is True
        assert request.force is False

        rpc.terminate(mock_servicer.DeleteFiles(request, FakeContext()), (), grpc.StatusCode.OK, "")
        results, total_deleted, total_failed = future.result(timeout=5.0)

        assert results == dict.fromkeys(file_ids, True)
        assert (total_deleted, total_failed) == (3, 0)
        assert mock_servicer.files["missions:test_mission"] == {}

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_delete_files_reports_failed_items(
            self, client: GrpcFilesystem, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A result holding an OperationError maps to False and is logged with its error."""
        client.exec_grpc_query = AsyncMock(  # type: ignore[method-assign]
            return_value=filesystem_dto_pb2.DeleteFilesResponse(
                results=[
                    filesystem_messages_pb2.FileResult(
                        identifier="files:a", file=filesystem_messages_pb2.File(id="files:a")
                    ),
                    MockFilesystemServicer.error("files:b", "FAILED_PRECONDITION", "files:b failed"),
                ],
                bulk=bulk_pb2.BulkResponse(total_processed=2, total_failed=1),
            )
        )
        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)

        with caplog.at_level(logging.WARNING, logger="digitalkin"):
            results, total_deleted, total_failed = await client.delete_files(
                FileFilter(file_ids=["files:a", "files:b"])
            )

        assert results == {"files:a": True, "files:b": False}
        assert (total_deleted, total_failed) == (1, 1)
        assert any(
            "DeleteFiles dropped result files:b: FAILED_PRECONDITION files:b failed" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_delete_files_nothing_matched(self, client: GrpcFilesystem, start: Start) -> None:
        """An empty deletion returns empty results and zero counts."""
        future, _request, rpc = start(client.delete_files(FileFilter(names=["ghost.txt"])), "DeleteFiles")
        rpc.terminate(filesystem_dto_pb2.DeleteFilesResponse(bulk=bulk_pb2.BulkResponse()), (), grpc.StatusCode.OK, "")

        assert future.result(timeout=5.0) == ({}, 0, 0)


class TestFileLifecycle:
    """Upload, update, read and soft-delete one file through the mock service."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_file_status_handling(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer, sample_file_data: bytes
    ) -> None:
        """The status moves UPLOADING -> ACTIVE -> DELETED across the RPCs."""
        upload = UploadFileData(content=sample_file_data, name="life.txt", file_type="DOCUMENT")
        future, request, rpc = start(client.upload_files([upload]), "UploadFiles")
        rpc.terminate(mock_servicer.UploadFiles(request, FakeContext()), (), grpc.StatusCode.OK, "")
        files, _, _ = future.result(timeout=5.0)
        file_id = files[0].id
        assert files[0].status == "FILE_STATUS_UPLOADING"

        future, request, rpc = start(client.update_file(file_id, status="ACTIVE"), "UpdateFile")
        rpc.terminate(mock_servicer.UpdateFile(request, FakeContext()), (), grpc.StatusCode.OK, "")
        assert future.result(timeout=5.0).status == "FILE_STATUS_ACTIVE"

        future, request, rpc = start(client.get_file(file_id), "GetFile")
        rpc.terminate(mock_servicer.GetFile(request, FakeContext()), (), grpc.StatusCode.OK, "")
        assert future.result(timeout=5.0).status == "FILE_STATUS_ACTIVE"

        future, request, rpc = start(client.delete_files(FileFilter(file_ids=[file_id])), "DeleteFiles")
        rpc.terminate(mock_servicer.DeleteFiles(request, FakeContext()), (), grpc.StatusCode.OK, "")
        assert future.result(timeout=5.0) == ({file_id: True}, 1, 0)
        assert mock_servicer.files["missions:test_mission"][file_id].status == filesystem_enums_pb2.DELETED


class TestEnumConversion:
    """SDK file type / status names -> wire enums, legacy prefixed names included."""

    @pytest.mark.unit
    @pytest.mark.contract
    @pytest.mark.parametrize(
        ("name", "wire"),
        [
            ("DOCUMENT", filesystem_enums_pb2.DOCUMENT),
            ("image", filesystem_enums_pb2.IMAGE),
            ("FILE_TYPE_CODE", filesystem_enums_pb2.CODE),
            ("UNSPECIFIED", filesystem_enums_pb2.FILE_TYPE_UNSPECIFIED),
            ("bogus", filesystem_enums_pb2.FILE_TYPE_UNSPECIFIED),
        ],
    )
    def test_file_type_to_enum(self, name: str, wire: int) -> None:
        """Each name maps to its FileType; an unknown one to FILE_TYPE_UNSPECIFIED."""
        assert GrpcFilesystem._file_type_to_enum(name) == wire

    @pytest.mark.unit
    @pytest.mark.contract
    @pytest.mark.parametrize(
        ("name", "wire"),
        [
            ("ACTIVE", filesystem_enums_pb2.ACTIVE),
            ("archived", filesystem_enums_pb2.ARCHIVED),
            ("FILE_STATUS_DELETED", filesystem_enums_pb2.DELETED),
            ("", filesystem_enums_pb2.FILE_STATUS_UNSPECIFIED),
            ("bogus", filesystem_enums_pb2.FILE_STATUS_UNSPECIFIED),
        ],
    )
    def test_file_status_to_enum(self, name: str, wire: int) -> None:
        """Each name maps to its FileStatus; an unknown one to FILE_STATUS_UNSPECIFIED."""
        assert GrpcFilesystem._file_status_to_enum(name) == wire

    @pytest.mark.unit
    @pytest.mark.regression
    def test_record_keeps_prefixed_names(self) -> None:
        """Records keep the ``FILE_TYPE_`` / ``FILE_STATUS_`` names callers compare against."""
        record = GrpcFilesystem._file_proto_to_data(
            filesystem_messages_pb2.File(id="files:1", type=filesystem_enums_pb2.IMAGE)
        )
        assert record.file_type == "FILE_TYPE_IMAGE"
        assert record.status == "FILE_STATUS_UNSPECIFIED"


class TestContextScopes:
    """Tests that the context kind rides on the request, never on the filter."""

    @pytest.mark.grpc
    @pytest.mark.parametrize(
        ("context", "wire"),
        [
            (Context.MISSIONS, filesystem_enums_pb2.MISSIONS),
            (Context.SETUP, filesystem_enums_pb2.SETUPS),
            (Context.USERS, filesystem_enums_pb2.USERS),
            (Context.ORGANIZATIONS, filesystem_enums_pb2.ORGANIZATIONS),
            (Context.UNSPECIFIED, filesystem_enums_pb2.FILE_CONTEXT_UNSPECIFIED),
        ],
    )
    def test_get_files_forwards_context_kind(
        self,
        context: Context,
            wire: int,
        client: GrpcFilesystem,
            start: Start,
        mock_servicer: MockFilesystemServicer,
    ) -> None:
        """get_files emits the matching context kind on the request.

        Covers the cross-owner scopes USERS / ORGANIZATIONS added alongside the
        existing MISSIONS / SETUP; the concrete owner id is resolved server-side.
        """
        future, request, rpc = start(client.get_files(FileFilter(context=context)), "ListFiles")

        assert request.context == wire

        rpc.terminate(mock_servicer.ListFiles(request, FakeContext()), (), grpc.StatusCode.OK, "")
        assert future.result(timeout=5.0) == ([], 0)

    @pytest.mark.grpc
    def test_get_file_forwards_cross_owner_context(
            self, client: GrpcFilesystem, start: Start, mock_servicer: MockFilesystemServicer
    ) -> None:
        """get_file under USERS emits USERS on the wire."""
        (file_id,) = mock_servicer.seed("mine.txt")
        future, request, rpc = start(client.get_file(file_id, context=Context.USERS), "GetFile")

        assert request.context == filesystem_enums_pb2.USERS

        rpc.terminate(mock_servicer.GetFile(request, FakeContext()), (), grpc.StatusCode.OK, "")
        assert future.result(timeout=5.0).id == file_id


class TestContextEnumContract:
    """SDK ``Context`` kind -> filesystem wire enum (``_context_enum``)."""

    _WIRE = (
        (Context.MISSIONS, filesystem_enums_pb2.MISSIONS),
        (Context.SETUP, filesystem_enums_pb2.SETUPS),
        (Context.USERS, filesystem_enums_pb2.USERS),
        (Context.ORGANIZATIONS, filesystem_enums_pb2.ORGANIZATIONS),
        (Context.UNSPECIFIED, filesystem_enums_pb2.FILE_CONTEXT_UNSPECIFIED),
    )

    @pytest.mark.unit
    @pytest.mark.contract
    @pytest.mark.parametrize(("ctx", "wire"), _WIRE)
    def test_context_enum_maps_to_wire(self, ctx: Context, wire: int) -> None:
        """Each Context kind maps to its ``FileContext`` constant."""
        assert GrpcFilesystem._context_enum(ctx) == wire

    @pytest.mark.property
    @given(ctx=st.sampled_from(list(Context)))
    def test_context_enum_is_total(self, ctx: Context) -> None:
        """Every Context kind maps to a defined filesystem wire enum (never crashes)."""
        assert GrpcFilesystem._context_enum(ctx) in {wire for _, wire in self._WIRE}


class TestFilesystemRefusalAndFailures:
    """Refusals (PERMISSION_DENIED) propagate; other gRPC failures wrap as FilesystemServiceError."""

    @pytest.mark.grpc
    @pytest.mark.regression
    async def test_permission_denied_propagates(self, client: GrpcFilesystem) -> None:
        """Authz refusals are re-raised as-is, never masked as a service error."""
        client.exec_grpc_query = AsyncMock(side_effect=PermissionDeniedError("denied"))  # type: ignore[method-assign]
        with pytest.raises(PermissionDeniedError):
            await client.get_file("f")
        with pytest.raises(PermissionDeniedError):
            await client.get_files(FileFilter())

    @pytest.mark.grpc
    @pytest.mark.chaos
    async def test_grpc_failure_wrapped(self, client: GrpcFilesystem) -> None:
        """A generic gRPC failure surfaces as FilesystemServiceError on reads."""
        client.exec_grpc_query = AsyncMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
        with pytest.raises(FilesystemServiceError):
            await client.get_file("f")
        with pytest.raises(FilesystemServiceError):
            await client.get_files(FileFilter())


class TestVisibilityOnTheWire:
    """Visibility is encoded onto every write path and decoded back off File."""

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_upload_forwards_per_file_visibility(self, client: GrpcFilesystem, start: Start) -> None:
        """An explicit visibility goes out as the common wire enum."""
        upload = UploadFileData(content=b"x", name="f.txt", file_type="DOCUMENT", visibility=Visibility.INTERNAL)
        future, request, rpc = start(client.upload_files([upload]), "UploadFiles")

        assert request.files[0].visibility == common_enums_pb2.INTERNAL

        rpc.terminate(filesystem_dto_pb2.UploadFilesResponse(), (), grpc.StatusCode.OK, "")
        assert future.result(timeout=5.0) == ([], 0, 0)

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_upload_defaults_to_unspecified(self, client: GrpcFilesystem, start: Start) -> None:
        """Unspecified means "let the service decide", so it must go out as the zero enum."""
        upload = UploadFileData(content=b"x", name="f.txt", file_type="DOCUMENT")
        future, request, rpc = start(client.upload_files([upload]), "UploadFiles")

        assert request.files[0].visibility == common_enums_pb2.VISIBILITY_UNSPECIFIED

        rpc.terminate(filesystem_dto_pb2.UploadFilesResponse(), (), grpc.StatusCode.OK, "")
        future.result(timeout=5.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_update_forwards_visibility(self, client: GrpcFilesystem, start: Start) -> None:
        """An update carrying only a visibility sends it and nothing else."""
        future, request, rpc = start(client.update_file("files:1", visibility=Visibility.PUBLIC), "UpdateFile")

        assert request.visibility == common_enums_pb2.PUBLIC
        assert not request.HasField("status")

        rpc.terminate(
            filesystem_dto_pb2.UpdateFileResponse(
                result=filesystem_messages_pb2.FileResult(
                    identifier="files:1", file=filesystem_messages_pb2.File(id="files:1")
                )
            ),
            (),
            grpc.StatusCode.OK,
            "",
        )
        assert future.result(timeout=5.0).id == "files:1"

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_filter_forwards_visibilities(self, client: GrpcFilesystem, start: Start) -> None:
        """Filter visibilities go out as the common wire enums, in order."""
        filters = FileFilter(visibilities=[Visibility.PRIVATE, Visibility.INTERNAL])
        future, request, rpc = start(client.get_files(filters), "ListFiles")

        assert list(request.filter.visibilities) == [common_enums_pb2.PRIVATE, common_enums_pb2.INTERNAL]

        rpc.terminate(filesystem_dto_pb2.ListFilesResponse(), (), grpc.StatusCode.OK, "")
        future.result(timeout=5.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_visibility_is_decoded_onto_the_record(self, client: GrpcFilesystem, start: Start) -> None:
        """The File visibility and renamed fields land on the record."""
        future, _request, rpc = start(client.get_file("files:1"), "GetFile")
        rpc.terminate(
            filesystem_dto_pb2.GetFileResponse(
                result=filesystem_messages_pb2.FileResult(
                    identifier="files:1",
                    file=filesystem_messages_pb2.File(
                        id="files:1",
                        context="missions:m1",
                        name="f.txt",
                        storage_uri="uri",
                        url="https://example.com/f.txt",
                        visibility=common_enums_pb2.INTERNAL,
                    ),
                )
            ),
            (),
            grpc.StatusCode.OK,
            "",
        )

        record = future.result(timeout=5.0)
        assert record.visibility is Visibility.INTERNAL
        assert record.file_url == "https://example.com/f.txt"
