"""Test the grpc filesystem service."""

import logging
import secrets
import string

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.filesystem.v1 import (
    filesystem_pb2,
    filesystem_service_pb2,
    filesystem_service_pb2_grpc,
)
from google.protobuf import struct_pb2
from grpc.framework.foundation import logging_pool
from mock_filesystem_servicer import MockFilesystemServicer
from tests.fixtures.grpc_fixtures import FakeContext

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode, ServerMode
from digitalkin.services.filesystem.filesystem_strategy import (
    FileFilter,
    FilesystemRecord,
    UploadFileData,
)
from digitalkin.services.filesystem.grpc_filesystem import GrpcFilesystem

service_instance = MockFilesystemServicer()
service_name = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]

alphabet = string.ascii_letters + string.digits
test_logger = logging.getLogger(__name__)
client_execution_thread_pool = logging_pool.pool(max_workers=10)


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Mock a gRPC channel.

    Returns:
        Mock gRPC Channel
    """
    test_logger.info("Creating test channel...")
    # Create a strict real time test clock
    test_clock = grpc_testing.strict_real_time()
    # Create a test channel with our service descriptor and our fake servicer
    channel = grpc_testing.channel([service_name], test_clock)
    test_logger.info("Test channel created")
    return channel


@pytest.fixture
def mock_servicer() -> MockFilesystemServicer:
    """Return an instance of the mock servicer.

    Returns:
        Mock Filesystem Servicer
    """
    test_logger.info("Creating mock servicer...")
    servicer = MockFilesystemServicer()
    test_logger.info("Mock servicer created")
    return servicer


@pytest.fixture
def client(test_channel: grpc_testing.Channel) -> GrpcFilesystem:
    """Instantiate a GrpcFilesystem client that uses the test channel.

    Returns:
        gRPC client as GrpcFilesystem
    """
    test_logger.info("Creating client...")
    # Create a dummy ServerConfig; its values are not used since we override _init_channel.
    dummy_config = ClientConfig(
        host="[::]",
        port=50151,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )

    mission_id = "test_mission"
    setup_id = "setup:1"
    setup_version_id = "setup_version:1"
    client = GrpcFilesystem(mission_id, setup_id, setup_version_id, dummy_config)

    # Override the channel and stub to use our test channel
    client.stub = filesystem_service_pb2_grpc.FilesystemServiceStub(test_channel)
    test_logger.info("Client created")
    return client


@pytest.fixture
def sample_file_data() -> bytes:
    """Generate sample file data for testing.

    Returns:
        bytes: Sample file data
    """
    return b"This is sample file content for testing."


@pytest.fixture
def file_metadata() -> dict:
    """Generate file metadata for testing.

    Returns:
        dict: File metadata with all required fields for FilesystemRecord
    """
    name = f"test_file_{secrets.token_hex(4)}.txt"
    return {
        "id": f"file_{secrets.token_hex(8)}",
        "context": "setup",
        "name": name,
        "file_type": "DOCUMENT",
        "content_type": "text/plain",
        "size_bytes": 40,
        "checksum": "a1b2c3d4e5f6",
        "metadata": {"key": "value"},
        "storage_uri": f"gs://test-bucket/setup/{name}",
        "file_url": f"https://storage.example.com/setup/{name}",
        "status": "UPLOADING",
    }


class TestUploadFiles:
    """Tests for Filesystem.upload_files() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_upload_files_success(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockFilesystemServicer,
        sample_file_data: bytes,
        file_metadata: dict,
    ) -> None:
        """Test successful upload with a good request.

        Verifies that upload creates the correct request and returns the expected response.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock filesystem servicer
            sample_file_data: Sample file data for testing
            file_metadata: File metadata for testing
        """
        # Create upload file data
        upload_file = UploadFileData(
            content=sample_file_data,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            replace_if_exists=False,
        )

        # Start the client call in a separate thread
        future = client_execution_thread_pool.submit(client.upload_files, [upload_file])

        # Get the service and method descriptor
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["UploadFiles"]

        # Intercept the pending unary-unary call
        _, _, rpc = test_channel.take_unary_unary(method_desc)

        # Create a request object for the mock servicer
        metadata_struct = struct_pb2.Struct()
        if file_metadata["metadata"]:
            metadata_struct.update(file_metadata["metadata"])
        else:
            metadata_struct = None

        # Create a response with all required fields
        file_result = filesystem_pb2.FileResult(
            file=filesystem_pb2.File(
                file_id=file_metadata["id"],
                context=file_metadata["context"],
                name=file_metadata["name"],
                file_type=GrpcFilesystem._file_type_to_enum(file_metadata["file_type"]),
                content_type=file_metadata["content_type"],
                size_bytes=file_metadata["size_bytes"],
                checksum=file_metadata["checksum"],
                metadata=metadata_struct,
                storage_uri=file_metadata["storage_uri"],
                file_url=file_metadata["file_url"],
                status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
            )
        )
        response = filesystem_pb2.UploadFilesResponse(
            results=[file_result],
            total_uploaded=1,
            total_failed=0,
        )

        # Use grpc_testing to send the response back to the client
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        # Verify the client call returns the expected FilesystemRecord
        result = future.result(timeout=5.0)
        assert isinstance(result, tuple)
        files, total_uploaded, total_failed = result

        # Verify the response counts
        assert len(files) == 1
        assert total_uploaded == 1
        assert total_failed == 0

        # Verify the file data
        file_data = files[0]
        assert isinstance(file_data, FilesystemRecord)
        assert file_data.id == file_metadata["id"]
        assert file_data.context == file_metadata["context"]
        assert file_data.name == file_metadata["name"]
        # Accept either enum-prefixed or plain values depending on transport layer
        assert file_data.file_type in {
            file_metadata["file_type"],
            "FILE_TYPE_" + file_metadata["file_type"],
        }
        assert file_data.content_type == file_metadata["content_type"]
        assert file_data.size_bytes == file_metadata["size_bytes"]
        assert file_data.checksum == file_metadata["checksum"]
        assert file_data.metadata == file_metadata["metadata"]
        assert file_data.storage_uri == file_metadata["storage_uri"]
        assert file_data.file_url == file_metadata["file_url"]
        assert file_data.status in {
            file_metadata["status"],
            "FILE_STATUS_" + file_metadata["status"],
        }
        assert file_data.storage_uri is not None
        assert file_data.file_url is not None
        assert file_data.size_bytes == len(sample_file_data)
        assert file_data.checksum is not None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_upload_files_duplicate_error(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockFilesystemServicer,
        sample_file_data: bytes,
        file_metadata: dict,
    ) -> None:
        """Test that uploading a duplicate file raises an error when replace_if_exists is False.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock filesystem servicer
            sample_file_data: Sample file data for testing
            file_metadata: File metadata for testing
        """
        # First upload a file
        upload_file = UploadFileData(
            content=sample_file_data,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            replace_if_exists=False,
        )

        # Upload the file first time
        future = client_execution_thread_pool.submit(client.upload_files, [upload_file])
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["UploadFiles"]
        _, _, rpc = test_channel.take_unary_unary(method_desc)
        metadata_struct = struct_pb2.Struct()
        metadata_struct.update(file_metadata["metadata"])
        upload_request = filesystem_pb2.UploadFilesRequest(
            files=[
                filesystem_pb2.UploadFileData(
                    context=file_metadata["context"],
                    name=file_metadata["name"],
                    file_type=GrpcFilesystem._file_type_to_enum(file_metadata["file_type"]),
                    content_type=file_metadata["content_type"],
                    content=sample_file_data,
                    metadata=metadata_struct,
                    status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
                    replace_if_exists=False,
                )
            ]
        )
        response = mock_servicer.UploadFiles(upload_request, FakeContext())
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")
        future.result()

        # Try to upload the same file again
        future = client_execution_thread_pool.submit(client.upload_files, [upload_file])
        _, _, rpc = test_channel.take_unary_unary(method_desc)
        response = mock_servicer.UploadFiles(upload_request, FakeContext())
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.ALREADY_EXISTS, "File already exists")

        with pytest.raises(ServerError):
            future.result()


class TestGetFile:
    """Tests for Filesystem.get_file() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_file_success(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockFilesystemServicer,
        sample_file_data: bytes,
        file_metadata: dict,
    ) -> None:
        """Test successful get_file operation.

        First uploads a file, then retrieves it by ID.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock filesystem servicer
            sample_file_data: Sample file data for testing
            file_metadata: File metadata for testing
        """
        # First upload a file to the mock servicer
        metadata_struct = struct_pb2.Struct()
        if file_metadata["metadata"]:
            metadata_struct.update(file_metadata["metadata"])

        upload_request = filesystem_pb2.UploadFilesRequest(
            files=[
                filesystem_pb2.UploadFileData(
                    context=file_metadata["context"],
                    name=file_metadata["name"],
                    file_type=GrpcFilesystem._file_type_to_enum(file_metadata["file_type"]),
                    content_type=file_metadata["content_type"],
                    content=sample_file_data,
                    metadata=metadata_struct,
                    status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
                    replace_if_exists=False,
                )
            ]
        )
        upload_response = mock_servicer.UploadFiles(upload_request, FakeContext())
        file_id = upload_response.results[0].file.file_id

        # Start the client call to get the file
        future = client_execution_thread_pool.submit(client.get_file, file_id)

        # Get the service and method descriptor
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["GetFile"]

        # Intercept the pending unary-unary call
        _, _, rpc = test_channel.take_unary_unary(method_desc)

        # Create a request object for the mock servicer
        get_request = filesystem_pb2.GetFileRequest(
            context=file_metadata["context"],
            file_id=file_id,
            include_content=False,
        )

        # Use grpc_testing to send the response back to the client
        rpc.send_initial_metadata(())
        rpc.terminate(mock_servicer.GetFile(get_request, FakeContext()), (), grpc.StatusCode.OK, "")

        # Verify the client call returns the expected FilesystemRecord
        result = future.result(timeout=5.0)
        assert isinstance(result, FilesystemRecord)
        assert result.id == file_id
        assert result.context == file_metadata["context"]
        assert result.name == file_metadata["name"]
        assert result.file_type == "FILE_TYPE_" + file_metadata["file_type"]
        assert result.content_type == file_metadata["content_type"]
        assert result.metadata == file_metadata["metadata"]
        assert result.status == "FILE_STATUS_" + file_metadata["status"]
        assert result.storage_uri is not None
        assert result.file_url is not None
        assert result.size_bytes == len(sample_file_data)
        assert result.checksum is not None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_get_file_not_found(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
    ) -> None:
        """Test that getting a non-existent file raises an error.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
        """
        future = client_execution_thread_pool.submit(client.get_file, "nonexistent_file_id")
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["GetFile"]
        _, _, rpc = test_channel.take_unary_unary(method_desc)
        rpc.send_initial_metadata(())
        rpc.terminate(None, (), grpc.StatusCode.NOT_FOUND, "File not found")

        with pytest.raises(ServerError):
            future.result()


class TestGetFiles:
    """Tests for Filesystem.get_files() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_files_success(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockFilesystemServicer,
        sample_file_data: bytes,
        file_metadata: dict,
    ) -> None:
        """Test successful get_files operation.

        First uploads multiple files, then retrieves them using filters.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock filesystem servicer
            sample_file_data: Sample file data for testing
            file_metadata: File metadata for testing
        """
        # Upload multiple files to the mock servicer
        file_names = [f"{file_metadata['name']}_{i}" for i in range(3)]

        metadata_struct = struct_pb2.Struct()
        if file_metadata["metadata"]:
            metadata_struct.update(file_metadata["metadata"])

        upload_files = [
            filesystem_pb2.UploadFileData(
                context=file_metadata["context"],
                name=name,
                file_type=GrpcFilesystem._file_type_to_enum(file_metadata["file_type"]),
                content_type=file_metadata["content_type"],
                content=sample_file_data,
                metadata=metadata_struct,
                status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
                replace_if_exists=False,
            )
            for name in file_names
        ]

        upload_request = filesystem_pb2.UploadFilesRequest(files=upload_files)
        upload_response = mock_servicer.UploadFiles(upload_request, FakeContext())
        file_ids = [result.file.file_id for result in upload_response.results]

        # Create filter criteria
        filters = FileFilter()

        # Start the client call to get files
        future = client_execution_thread_pool.submit(
            client.get_files,
            filters,
            list_size=10,
            offset=0,
            order="created_at:desc",
            include_content=False,
        )

        # Get the service and method descriptor
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["GetFiles"]

        # Intercept the pending unary-unary call
        _, _request, rpc = test_channel.take_unary_unary(method_desc)

        # Create a request object for the mock servicer
        get_request = filesystem_pb2.GetFilesRequest(
            context=file_metadata["context"],
            filters=filesystem_pb2.FileFilter(
                context=file_metadata["context"],
                file_types=[GrpcFilesystem._file_type_to_enum(file_metadata["file_type"])],
                status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
            ),
            list_size=10,
            offset=0,
            order="created_at:desc",
            include_content=False,
        )

        # Use grpc_testing to send the response back to the client
        rpc.send_initial_metadata(())
        rpc.terminate(mock_servicer.GetFiles(get_request, FakeContext()), (), grpc.StatusCode.OK, "")

        # Verify the client call returns a list of FilesystemRecord
        result = future.result(timeout=5.0)
        assert isinstance(result, tuple)
        files, total_count = result
        assert len(files) == 3
        assert total_count == 3

        for file_data in files:
            assert isinstance(file_data, FilesystemRecord)
            assert file_data.context == file_metadata["context"]
            assert file_data.name in file_names
            assert file_data.file_type == "FILE_TYPE_" + file_metadata["file_type"]
            assert file_data.content_type == file_metadata["content_type"]
            assert file_data.metadata == file_metadata["metadata"]
            assert file_data.status == "FILE_STATUS_" + file_metadata["status"]
            assert file_data.storage_uri is not None
            assert file_data.file_url is not None
            assert file_data.size_bytes == len(sample_file_data)
            assert file_data.checksum is not None
            assert file_data.id in file_ids

        # Test empty context case
        empty_filters = FileFilter(
            file_types=[file_metadata["file_type"]],
            status="UPLOADING",
        )

        future = client_execution_thread_pool.submit(
            client.get_files,
            empty_filters,
            list_size=10,
            offset=0,
        )

        _, _, rpc = test_channel.take_unary_unary(method_desc)
        filesystem_pb2.GetFilesRequest(
            context="nonexistent_context",
            filters=filesystem_pb2.FileFilter(
                context="nonexistent_context",
                file_types=[GrpcFilesystem._file_type_to_enum(file_metadata["file_type"])],
                status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
            ),
            list_size=10,
            offset=0,
        )
        empty_response = filesystem_pb2.GetFilesResponse(files=[], total_count=0)
        rpc.send_initial_metadata(())
        rpc.terminate(empty_response, (), grpc.StatusCode.OK, "")

        empty_result = future.result(timeout=5.0)
        assert isinstance(empty_result, tuple)
        empty_files, empty_count = empty_result
        assert len(empty_files) == 0
        assert empty_count == 0


class TestUpdateFile:
    """Tests for Filesystem.update_file() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_update_file_success(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockFilesystemServicer,
        sample_file_data: bytes,
        file_metadata: dict,
    ) -> None:
        """Test successful update_file operation.

        First uploads a file, then updates it.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock filesystem servicer
            sample_file_data: Sample file data for testing
            file_metadata: File metadata for testing
        """
        # First upload a file to the mock servicer
        metadata_struct = struct_pb2.Struct()
        if file_metadata["metadata"]:
            metadata_struct.update(file_metadata["metadata"])

        upload_request = filesystem_pb2.UploadFilesRequest(
            files=[
                filesystem_pb2.UploadFileData(
                    context=file_metadata["context"],
                    name=file_metadata["name"],
                    file_type=GrpcFilesystem._file_type_to_enum(file_metadata["file_type"]),
                    content_type=file_metadata["content_type"],
                    content=sample_file_data,
                    metadata=metadata_struct,
                    status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
                    replace_if_exists=False,
                )
            ]
        )
        upload_response = mock_servicer.UploadFiles(upload_request, FakeContext())
        file_id = upload_response.results[0].file.file_id

        # Start the client call to update the file
        updated_content = b"Updated content"
        future = client_execution_thread_pool.submit(
            client.update_file,
            file_id,
            content=updated_content,
            file_type="DOCUMENT",
            content_type="text/plain",
            metadata={"new_key": "new_value"},
            new_name="updated_file.txt",
            status="ACTIVE",
        )

        # Get the service and method descriptor
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["UpdateFile"]

        # Intercept the pending unary-unary call
        _, _, rpc = test_channel.take_unary_unary(method_desc)

        # Create a request object for the mock servicer
        update_request = filesystem_pb2.UpdateFileRequest(
            context=file_metadata["context"],
            file_id=file_id,
            content=updated_content,
            file_type=GrpcFilesystem._file_type_to_enum("DOCUMENT"),
            content_type="text/plain",
            metadata=struct_pb2.Struct(fields={"new_key": struct_pb2.Value(string_value="new_value")}),
            new_name="updated_file.txt",
            status=GrpcFilesystem._file_status_to_enum("ACTIVE"),
        )

        # Use the mock servicer to handle the request
        response = mock_servicer.UpdateFile(update_request, FakeContext())

        # Use grpc_testing to send the response back to the client
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        # Verify the client call returns the expected FilesystemRecord
        result = future.result(timeout=5.0)
        assert isinstance(result, FilesystemRecord)
        assert result.id == file_id
        assert result.context == file_metadata["context"]
        assert result.name == "updated_file.txt"
        assert result.file_type == "FILE_TYPE_DOCUMENT"
        assert result.content_type == "text/plain"
        assert result.metadata == {"new_key": "new_value"}
        assert result.status == "FILE_STATUS_ACTIVE"
        assert result.storage_uri is not None
        assert result.file_url is not None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_update_file_not_found(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
    ) -> None:
        """Test that updating a non-existent file raises an error.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
        """
        future = client_execution_thread_pool.submit(
            client.update_file,
            "nonexistent_file_id",
            content=b"new content",
            file_type="DOCUMENT",
            content_type="text/plain",
        )
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["UpdateFile"]
        _, _, rpc = test_channel.take_unary_unary(method_desc)
        rpc.send_initial_metadata(())
        rpc.terminate(None, (), grpc.StatusCode.NOT_FOUND, "File not found")

        with pytest.raises(ServerError):
            future.result()


class TestDeleteFiles:
    """Tests for Filesystem.delete_files() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_delete_files_success(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockFilesystemServicer,
        sample_file_data: bytes,
        file_metadata: dict,
    ) -> None:
        """Test successful delete_files operation.

        First uploads multiple files, then deletes them using filters.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock filesystem servicer
            sample_file_data: Sample file data for testing
            file_metadata: File metadata for testing
        """
        # Upload multiple files to the mock servicer
        file_names = [f"{file_metadata['name']}_{i}" for i in range(3)]

        metadata_struct = struct_pb2.Struct()
        if file_metadata["metadata"]:
            metadata_struct.update(file_metadata["metadata"])

        upload_files = [
            filesystem_pb2.UploadFileData(
                context=file_metadata["context"],
                name=name,
                file_type=GrpcFilesystem._file_type_to_enum(file_metadata["file_type"]),
                content_type=file_metadata["content_type"],
                content=sample_file_data,
                metadata=metadata_struct,
                status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
                replace_if_exists=False,
            )
            for name in file_names
        ]

        upload_request = filesystem_pb2.UploadFilesRequest(files=upload_files)
        upload_response = mock_servicer.UploadFiles(upload_request, FakeContext())
        file_ids = [result.file.file_id for result in upload_response.results]

        # Create filter criteria
        filters = FileFilter(
            file_types=[file_metadata["file_type"]],
        )

        # Start the client call to delete files
        future = client_execution_thread_pool.submit(
            client.delete_files,
            filters,
            permanent=True,
            force=False,
        )

        # Get the service and method descriptor
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["DeleteFiles"]

        # Intercept the pending unary-unary call
        _, _, rpc = test_channel.take_unary_unary(method_desc)

        # Create a request object for the mock servicer
        delete_request = filesystem_pb2.DeleteFilesRequest(
            context=file_metadata["context"],
            filters=filesystem_pb2.FileFilter(
                context=file_metadata["context"],
                file_types=[GrpcFilesystem._file_type_to_enum(file_metadata["file_type"])],
                status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
            ),
            permanent=True,
            force=False,
        )

        # Use the mock servicer to handle the request
        response = mock_servicer.DeleteFiles(delete_request, FakeContext())

        # Use grpc_testing to send the response back to the client
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        # Verify the client call returns success
        result = future.result(timeout=5.0)
        assert isinstance(result, tuple)
        results, total_deleted, total_failed = result
        assert len(results) == 3
        assert total_deleted == 3
        assert total_failed == 0

        for file_id in file_ids:
            assert results[file_id] is True

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_delete_files_not_found(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
    ) -> None:
        """Test that deleting non-existent files returns empty results.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
        """
        filters = FileFilter(
            file_types=["DOCUMENT"],
            status="ACTIVE",
        )

        future = client_execution_thread_pool.submit(
            client.delete_files,
            filters,
            permanent=True,
            force=False,
        )
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["DeleteFiles"]
        _, _, rpc = test_channel.take_unary_unary(method_desc)

        # Mock servicer returns empty results for non-existent context
        response = filesystem_pb2.DeleteFilesResponse(
            results={},
            total_deleted=0,
            total_failed=0,
        )
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        results, total_deleted, total_failed = future.result()
        assert isinstance(results, dict)
        assert len(results) == 0
        assert total_deleted == 0
        assert total_failed == 0


class TestFilesystemEdgeCases:
    """Edge cases and error handling tests."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_server_error(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
        file_metadata: dict,
    ) -> None:
        """Test that the upload_files method raises ServerError for gRPC errors.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
            file_metadata: File metadata for testing.
        """
        # Create upload file data
        upload_file = UploadFileData(
            content=b"Sample content",
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            replace_if_exists=False,
        )

        # Start the client call
        future = client_execution_thread_pool.submit(client.upload_files, [upload_file])

        # Get the service and method descriptor
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["UploadFiles"]

        # Intercept the pending unary-unary call
        _, _, rpc = test_channel.take_unary_unary(method_desc)

        # Simulate a gRPC error response
        rpc.send_initial_metadata(())
        rpc.terminate(
            None,
            (),
            grpc.StatusCode.INTERNAL,
            "gRPC error occurred",
        )

        # Verify the client call raises a ServerError
        with pytest.raises(ServerError):
            future.result()

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_file_status_handling(
        self,
        client: GrpcFilesystem,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockFilesystemServicer,
        sample_file_data: bytes,
        file_metadata: dict,
    ) -> None:
        """Test that file status is handled correctly throughout the lifecycle.

        Args:
            client: GrpcFilesystem client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock filesystem servicer
            sample_file_data: Sample file data for testing
            file_metadata: File metadata for testing
        """
        # First upload a file
        upload_file = UploadFileData(
            content=sample_file_data,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            replace_if_exists=False,
        )

        # Upload the file
        future = client_execution_thread_pool.submit(client.upload_files, [upload_file])
        service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
        method_desc = service_desc.methods_by_name["UploadFiles"]
        _, _, rpc = test_channel.take_unary_unary(method_desc)

        metadata_struct = struct_pb2.Struct()
        metadata_struct.update(file_metadata["metadata"])

        upload_request = filesystem_pb2.UploadFilesRequest(
            files=[
                filesystem_pb2.UploadFileData(
                    context=file_metadata["context"],
                    name=file_metadata["name"],
                    file_type=GrpcFilesystem._file_type_to_enum(file_metadata["file_type"]),
                    content_type=file_metadata["content_type"],
                    content=sample_file_data,
                    metadata=metadata_struct,
                    status=GrpcFilesystem._file_status_to_enum(file_metadata["status"]),
                    replace_if_exists=False,
                )
            ]
        )
        response = mock_servicer.UploadFiles(upload_request, FakeContext())
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        upload_result = future.result()
        assert isinstance(upload_result, tuple)
        files, total_uploaded, total_failed = upload_result
        assert len(files) == 1
        assert total_uploaded == 1
        assert total_failed == 0
        assert files[0].status == "FILE_STATUS_" + file_metadata["status"]

        file_id = files[0].id

        # Update the file status
        future = client_execution_thread_pool.submit(
            client.update_file,
            file_id,
            status="ACTIVE",
        )

        method_desc = service_desc.methods_by_name["UpdateFile"]
        _, _, rpc = test_channel.take_unary_unary(method_desc)
        update_request = filesystem_pb2.UpdateFileRequest(
            context=file_metadata["context"],
            file_id=file_id,
            status=GrpcFilesystem._file_status_to_enum("ACTIVE"),
        )
        response = mock_servicer.UpdateFile(update_request, FakeContext())
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        update_result = future.result()
        assert isinstance(update_result, FilesystemRecord)
        assert update_result.status == "FILE_STATUS_ACTIVE"

        # Get the file and verify status
        future = client_execution_thread_pool.submit(client.get_file, file_id)
        method_desc = service_desc.methods_by_name["GetFile"]
        _, _, rpc = test_channel.take_unary_unary(method_desc)
        get_request = filesystem_pb2.GetFileRequest(
            context=file_metadata["context"],
            file_id=file_id,
        )
        response = mock_servicer.GetFile(get_request, FakeContext())
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        get_result = future.result()
        assert isinstance(get_result, FilesystemRecord)
        assert get_result.status == "FILE_STATUS_ACTIVE"

        # Delete the file (soft delete)
        filters = FileFilter(
            context="setup",
            file_types=[file_metadata["file_type"]],
            status="ACTIVE",
        )

        future = client_execution_thread_pool.submit(
            client.delete_files,
            filters,
            permanent=False,
            force=False,
        )

        # Build proto filter manually to avoid context ID conversion
        # The mock servicer expects raw context ("setup") not ID ("setup:1")
        filters_proto = filesystem_pb2.FileFilter(
            context=file_metadata["context"],
            file_types=[GrpcFilesystem._file_type_to_enum(file_metadata["file_type"])],
            status=GrpcFilesystem._file_status_to_enum("ACTIVE"),
        )

        method_desc = service_desc.methods_by_name["DeleteFiles"]
        _, _, rpc = test_channel.take_unary_unary(method_desc)
        delete_request = filesystem_pb2.DeleteFilesRequest(
            context=file_metadata["context"],
            filters=filters_proto,
            permanent=False,
            force=False,
        )
        response = mock_servicer.DeleteFiles(delete_request, FakeContext())
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        delete_result = future.result()
        assert isinstance(delete_result, tuple)
        results, total_deleted, total_failed = delete_result
        assert len(results) == 1
        assert total_deleted == 1
        assert total_failed == 0
        assert results[file_id] is True


# ============================================================================
# Regression Tests
# ============================================================================
# This section contains tests for previously identified bugs and edge cases
# that were fixed. Each test should document the issue/PR that it addresses.
#
# Format:
# @pytest.mark.grpc
# @pytest.mark.integration
# @pytest.mark.regression
# def test_regression_issue_123(...):
#     """Test for regression of issue #123.
#
#     Issue: [Brief description of the bug]
#     Fixed in: PR #456 / commit abc123
#
#     Verifies: [What this test checks to prevent regression]
#     """
#
# Add regression tests below as bugs are discovered and fixed.
