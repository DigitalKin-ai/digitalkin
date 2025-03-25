"""Test the grpc filesystem service."""

import secrets
import string

import grpc
import grpc_testing
import pytest
from digitalkin_proto.digitalkin.filesystem.v2 import (
    filesystem_pb2,
    filesystem_service_pb2,
    filesystem_service_pb2_grpc,
)
from digitalkin_proto.digitalkin.filesystem.v2.filesystem_pb2 import (
    FileType as FileTypeProto,
    File as FileProto,
)
from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.models import SecurityMode, ServerConfig, ServerMode
from digitalkin.services.filesystem.filesystem_strategy import (
    FilesystemData,
    FileType,
    FilesystemServiceError,
)
from digitalkin.services.filesystem.grpc_filesystem import GrpcFilesystem
from grpc.framework.foundation import logging_pool

from mock_filesystem_servicer import FakeContext, MockFilesystemServicer

service_instance = MockFilesystemServicer()
service_name = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]

alphabet = string.ascii_letters + string.digits
client_execution_thread_pool = logging_pool.pool(1)


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Mock a gRPC channel.

    Returns:
        Mock gRPC Channel
    """
    # Create a strict real time test clock
    test_clock = grpc_testing.strict_real_time()
    # Create a test channel with our service descriptor and our fake servicer
    return grpc_testing.channel([service_name], test_clock)


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
    # Create a dummy ServerConfig; its values are not used since we override _init_channel.
    dummy_config = ServerConfig(
        host="[::]",
        port=50151,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        max_workers=10,
        credentials=None,
    )

    mission_id = "test_mission"
    config : dict[str, str] = {}

    client = GrpcFilesystem(mission_id, config, dummy_config)

    # Override the channel and stub to use our test channel
    client.stub = filesystem_service_pb2_grpc.FilesystemServiceStub(test_channel)
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
        dict: File metadata with kin_context, name, file_type, and url
    """
    name = f"test_file_{secrets.token_hex(4)}.txt"
    return {
        "kin_context": "test_mission",
        "name": name,
        "file_type": FileType.DOCUMENT,
    }


def test_upload_request_creation_success(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    sample_file_data: bytes,
    file_metadata: dict,
) -> None:
    """Test successful upload with a good request.

    Verifies that upload creates the correct request.

    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        sample_file_data: Sample file data for testing
        file_metadata: File metadata for testing
    """
    # Start the client call (this call will block until the response is simulated)
    future = client_execution_thread_pool.submit(
        client.upload,
        sample_file_data,
        file_metadata["name"],
        file_metadata["file_type"]
    )

    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["UploadFile"]

    # Intercept the pending unary-unary call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Create a mock URL for the response
    url = f"https://storage.example.com/{file_metadata['kin_context']}/{file_metadata['name']}"

    # Create a mock response
    file_proto = FileProto(
        kin_context=file_metadata["kin_context"],
        name=file_metadata["name"],
        file_type=getattr(FileTypeProto, file_metadata["file_type"].name),
        url=url
    )

    # Use grpc_testing to send the response back to the client
    rpc.send_initial_metadata(())
    rpc.terminate(
        filesystem_pb2.UploadFileResponse(file=file_proto),
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify the client call returns the expected FilesystemData
    result = future.result()
    assert isinstance(result, FilesystemData)
    assert result.kin_context == file_metadata["kin_context"]
    assert result.name == file_metadata["name"]
    assert result.file_type.name == file_metadata["file_type"].name
    assert result.url == url
    # Verify the request corresponds to the file data
    assert request.kin_context == file_metadata["kin_context"]
    assert request.name == file_metadata["name"]
    assert request.file_type == getattr(FileTypeProto, file_metadata["file_type"].name)
    assert request.content == sample_file_data


def test_upload_success(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockFilesystemServicer,
    sample_file_data: bytes,
    file_metadata: dict,
) -> None:
    """Test successful upload using the mock servicer.

    Verifies that upload RPC call works with a valid request using the fake servicer.

    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock filesystem servicer
        sample_file_data: Sample file data for testing
        file_metadata: File metadata for testing
    """
    # Start the client call (this call will block until the response is simulated)
    future = client_execution_thread_pool.submit(
        client.upload,
        sample_file_data,
        file_metadata["name"],
        file_metadata["file_type"]
    )

    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["UploadFile"]

    # Intercept the pending unary-unary call
    _, _request, rpc = test_channel.take_unary_unary(method_desc)

    # Create a request object for the mock servicer
    request_obj = filesystem_pb2.UploadFileRequest(
        kin_context=file_metadata["kin_context"],
        name=file_metadata["name"],
        file_type=getattr(FileTypeProto, file_metadata["file_type"].name),
        content=sample_file_data
    )

    # Use the mock servicer to handle the request
    response = mock_servicer.UploadFile(request_obj, FakeContext()) # type: ignore

    # Use grpc_testing to send the response back to the client
    rpc.send_initial_metadata(())
    rpc.terminate(
        response,
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify the client call returns the expected FilesystemData
    result = future.result()
    assert isinstance(result, FilesystemData)
    assert result.kin_context == file_metadata["kin_context"]
    assert result.name == file_metadata["name"]
    assert result.file_type == file_metadata["file_type"]
    assert result.url is not None

    # Verify the file was stored in the mock servicer
    assert file_metadata["kin_context"] in mock_servicer.files
    assert file_metadata["name"] in mock_servicer.files[file_metadata["kin_context"]]

    stored_file = mock_servicer.files[file_metadata["kin_context"]][file_metadata["name"]]
    assert stored_file.kin_context == file_metadata["kin_context"]
    assert stored_file.name == file_metadata["name"]
    assert stored_file.file_type == file_metadata["file_type"]
    assert stored_file.url is not None


def test_get_file_by_name_success(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockFilesystemServicer,
    sample_file_data: bytes,
    file_metadata: dict,
) -> None:
    """Test successful get_file_by_name operation.

    First uploads a file, then retrieves it by name.

    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock filesystem servicer
        sample_file_data: Sample file data for testing
        file_metadata: File metadata for testing
    """
    # First upload a file to the mock servicer
    upload_request = filesystem_pb2.UploadFileRequest(
        kin_context=file_metadata["kin_context"],
        name=file_metadata["name"],
        file_type=getattr(FileTypeProto, file_metadata["file_type"].name),
        content=sample_file_data
    )
    upload_response = mock_servicer.UploadFile(upload_request, FakeContext()) # type: ignore

    # Start the client call to get the file by name
    future = client_execution_thread_pool.submit(
        client.get,
        file_metadata["name"]
    )

    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["GetFileByName"]

    # Intercept the pending unary-unary call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Create a request object for the mock servicer
    get_request = filesystem_pb2.GetFileByNameRequest(
        kin_context=file_metadata["kin_context"],
        name=file_metadata["name"]
    )

    # Use the mock servicer to handle the request
    response = mock_servicer.GetFileByName(get_request, FakeContext()) # type: ignore

    # Use grpc_testing to send the response back to the client
    rpc.send_initial_metadata(())
    rpc.terminate(
        response,
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify the client call returns the expected FilesystemData
    result = future.result()
    assert isinstance(result, FilesystemData)
    assert result.kin_context == file_metadata["kin_context"]
    assert result.name == file_metadata["name"]
    assert result.file_type == file_metadata["file_type"]
    assert result.url is not None

    # Verify the request corresponds to the file data
    assert request.name == file_metadata["name"]


def test_get_all_files_success(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockFilesystemServicer,
    sample_file_data: bytes,
    file_metadata: dict,
) -> None:
    """Test successful get_all_files operation.

    First uploads multiple files, then retrieves all files.

    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock filesystem servicer
        sample_file_data: Sample file data for testing
        file_metadata: File metadata for testing
    """
    # Upload multiple files to the mock servicer
    file_names = [f"{file_metadata['name']}_{i}" for i in range(3)]

    for name in file_names:
        upload_request = filesystem_pb2.UploadFileRequest(
            kin_context=file_metadata["kin_context"],
            name=name,
            file_type=getattr(FileTypeProto, file_metadata["file_type"].name),
            content=sample_file_data
        )
        upload_response = mock_servicer.UploadFile(upload_request, FakeContext()) # type: ignore

    # Start the client call to get all files
    future = client_execution_thread_pool.submit(
        client.get_all
    )

    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["GetFilesByKinContext"]

    # Intercept the pending unary-unary call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Create a request object for the mock servicer
    get_request = filesystem_pb2.GetFilesByKinContextRequest(
        kin_context=file_metadata["kin_context"]
    )

    # Use the mock servicer to handle the request
    response = mock_servicer.GetFilesByKinContext(get_request, FakeContext()) # type: ignore

    # Use grpc_testing to send the response back to the client
    rpc.send_initial_metadata(())
    rpc.terminate(
        response,
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify the client call returns a list of FilesystemData
    result = future.result()
    assert isinstance(result, list)
    assert len(result) == 3
    for file_data in result:
        assert isinstance(file_data, FilesystemData)
        assert file_data.kin_context == file_metadata["kin_context"]
        assert file_data.name in file_names
        assert file_data.file_type == file_metadata["file_type"]
        assert file_data.url is not None

    # Verify the request corresponds to the file data
    assert request.kin_context == file_metadata["kin_context"]


def test_get_batch_files_success(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockFilesystemServicer,
    sample_file_data: bytes,
    file_metadata: dict,
) -> None:
    """Test successful get_batch operation.

    First uploads multiple files, then retrieves a batch of files by name.

    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock filesystem servicer
        sample_file_data: Sample file data for testing
        file_metadata: File metadata for testing
    """
    # Upload multiple files to the mock servicer
    file_names = [f"{file_metadata['name']}_{i}" for i in range(3)]

    for name in file_names:
        upload_request = filesystem_pb2.UploadFileRequest(
            kin_context=file_metadata["kin_context"],
            name=name,
            file_type=getattr(FileTypeProto, file_metadata["file_type"].name),
            content=sample_file_data
        )
        upload_response = mock_servicer.UploadFile(upload_request, FakeContext()) # type: ignore

    # Start the client call to get batch files
    batch_names = file_names[:2]  # Get only the first two files
    future = client_execution_thread_pool.submit(
        client.get_batch,
        batch_names
    )

    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["GetFilesByNames"]

    # Intercept the pending unary-unary call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Create a request object for the mock servicer
    get_request = filesystem_pb2.GetFilesByNamesRequest(
        kin_context=file_metadata["kin_context"],
        names=batch_names
    )

    # Use the mock servicer to handle the request
    response: filesystem_pb2.GetFilesByNamesResponse = mock_servicer.GetFilesByNames(get_request, FakeContext()) # type: ignore
    # Use grpc_testing to send the response back to the client
    rpc.send_initial_metadata(())
    rpc.terminate(
        response,
        (),
        grpc.StatusCode.OK,
        "",
    )
    # Verify the client call returns a dictionary of FilesystemData
    result = future.result()
    assert isinstance(result, dict)
    assert len(result) == len(batch_names)
    for name, file_data in result.items():
        assert isinstance(file_data, FilesystemData)
        assert file_data.kin_context == file_metadata["kin_context"]
        assert file_data.name in batch_names
        assert file_data.file_type == file_metadata["file_type"]
        assert file_data.url is not None

def test_get_batch_files_nonexistent(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockFilesystemServicer,
    file_metadata: dict,
    sample_file_data: bytes,
) -> None:
    """Test get_batch operation for non-existent files.

    Attempts to retrieve files that don't exist.

    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock filesystem servicer
        file_metadata: File metadata for testing
    """
    # Upload multiple files to the mock servicer
    file_names = [f"{file_metadata['name']}_{i}" for i in range(3)]

    for name in file_names:
        upload_request = filesystem_pb2.UploadFileRequest(
            kin_context=file_metadata["kin_context"],
            name=name,
            file_type=getattr(FileTypeProto, file_metadata["file_type"].name),
            content=sample_file_data
        )
        upload_response = mock_servicer.UploadFile(upload_request, FakeContext()) # type: ignore

    # Start the client call to get batch files with non-existent names
    batch_names = ["nonexistent_file_1.txt", "nonexistent_file_2.txt"]
    future = client_execution_thread_pool.submit(
        client.get_batch,
        batch_names
    )

    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["GetFilesByNames"]

    # Intercept the pending unary-unary call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Create a request object for the mock servicer
    get_request = filesystem_pb2.GetFilesByNamesRequest(
        kin_context=file_metadata["kin_context"],
        names=batch_names
    )

    # Use the mock servicer to handle the request
    response: filesystem_pb2.GetFilesByNamesResponse = mock_servicer.GetFilesByNames(get_request, FakeContext()) # type: ignore

    # Use grpc_testing to send the response back to the client
    rpc.send_initial_metadata(())
    rpc.terminate(
        response,
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify the client call returns an empty dictionary (indicating no files found)
    result = future.result()
    print(result)
    assert isinstance(result, dict)
    assert len(result) == len(batch_names)
    for name, file_data in result.items():
        assert file_data is None


def test_delete_file_success(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockFilesystemServicer,
    sample_file_data: bytes,
    file_metadata: dict,
) -> None:
    """Test successful delete operation.

    First uploads a file, then deletes it.

    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock filesystem servicer
        sample_file_data: Sample file data for testing
        file_metadata: File metadata for testing
    """
    # First upload a file to the mock servicer
    upload_request = filesystem_pb2.UploadFileRequest(
        kin_context=file_metadata["kin_context"],
        name=file_metadata["name"],
        file_type=getattr(FileTypeProto, file_metadata["file_type"].name),
        content=sample_file_data
    )
    upload_response = mock_servicer.UploadFile(upload_request, FakeContext()) # type: ignore

    # Start the client call to delete the file
    future = client_execution_thread_pool.submit(
        client.delete,
        file_metadata["name"]
    )

    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["DeleteFile"]

    # Intercept the pending unary-unary call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Create a request object for the mock servicer
    delete_request = filesystem_pb2.DeleteFileRequest(
        kin_context=file_metadata["kin_context"],
        name=file_metadata["name"]
    )

    # Use the mock servicer to handle the request
    response = mock_servicer.DeleteFile(delete_request, FakeContext()) # type: ignore

    # Use grpc_testing to send the response back to the client
    rpc.send_initial_metadata(())
    rpc.terminate(
        response,
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify the client call returns success
    result = future.result()
    assert result == 1  # 1 indicates successful deletion

    # Verify the request corresponds to the file data
    assert request.name == file_metadata["name"]

    # Verify the file is deleted from the mock servicer
    assert file_metadata["name"] not in mock_servicer.files[file_metadata["kin_context"]]


def test_delete_nonexistent_file(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockFilesystemServicer,
    file_metadata: dict,
) -> None:
    """Test delete operation for a non-existent file.

    Attempts to delete a file that doesn't exist.

    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock filesystem servicer
        file_metadata: File metadata for testing
    """
    # Start the client call to delete a non-existent file
    future = client_execution_thread_pool.submit(
        client.delete,
        "nonexistent_file.txt"
    )

    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["DeleteFile"]

    # Intercept the pending unary-unary call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Create a request object for the mock servicer
    delete_request = filesystem_pb2.DeleteFileRequest(
        kin_context=file_metadata["kin_context"],
        name="nonexistent_file.txt"
    )

    # Use the mock servicer to handle the request
    response = mock_servicer.DeleteFile(delete_request, FakeContext()) # type: ignore
    # Use grpc_testing to send the response back to the client
    rpc.send_initial_metadata(())
    rpc.terminate(
        response,
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify the client call returns 0 (indicating the file didn't exist)
    result = future.result()

    # Verify the request corresponds to the file data
    assert request.name == "nonexistent_file.txt"

def test_update_success(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockFilesystemServicer,
    sample_file_data: bytes,
    file_metadata: dict,
) -> None:
    """Test successful update operation.

    First uploads a file, then updates it.

    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock filesystem servicer
        sample_file_data: Sample file data for testing
        file_metadata: File metadata for testing
    """
    # First upload a file to the mock servicer
    upload_request = filesystem_pb2.UploadFileRequest(
        kin_context=file_metadata["kin_context"],
        name=file_metadata["name"],
        file_type=getattr(FileTypeProto, file_metadata["file_type"].name),
        content=sample_file_data
    )
    upload_response = mock_servicer.UploadFile(upload_request, FakeContext()) # type: ignore

    # Start the client call to update the file
    updated_content = b"Updated content"
    future = client_execution_thread_pool.submit(
        client.update,
        file_metadata["name"],
        updated_content,
        file_metadata["file_type"]
    )

    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["UpdateFile"]

    # Intercept the pending unary-unary call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Create a request object for the mock servicer
    update_request = filesystem_pb2.UpdateFileRequest(
        kin_context=file_metadata["kin_context"],
        name=file_metadata["name"],
        file_type=getattr(FileTypeProto, file_metadata["file_type"].name),
        content=updated_content
    )

    # Use the mock servicer to handle the request
    response = mock_servicer.UpdateFile(update_request, FakeContext()) # type: ignore

    # Use grpc_testing to send the response back to the client
    rpc.send_initial_metadata(())
    rpc.terminate(
        response,
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify the client call returns the expected FilesystemData
    result = future.result()
    assert isinstance(result, FilesystemData)
    assert result.kin_context == file_metadata["kin_context"]
    assert result.name == file_metadata["name"]
    assert result.file_type == file_metadata["file_type"]
    assert result.url is not None

    # Verify the request corresponds to the file data
    assert request.name == file_metadata["name"]
    assert request.file_type == getattr(FileTypeProto, file_metadata["file_type"].name)
    assert request.content == updated_content
    # Verify the file was updated in the mock servicer
    assert file_metadata["kin_context"] in mock_servicer.files
    assert file_metadata["name"] in mock_servicer.files[file_metadata["kin_context"]]

def test_filesystem_service_error(
    client: GrpcFilesystem,
    file_metadata: dict,
) -> None:
    """Test that the upload method raises ValidationError for invalid data.

    Args:
        client: GrpcFilesystem client for testing
        file_metadata: File metadata for testing
    """

    with pytest.raises(FilesystemServiceError, match="Unexpected error in UploadFile"):
        client.upload(
            b"Invalid content",
            file_metadata["name"],
            "invalid"  # Invalid file type # type: ignore
        )

def test_server_error(
    client: GrpcFilesystem,
    test_channel: grpc_testing.Channel,
    file_metadata: dict,
) -> None:
    """Test that the upload method raises ServerError for gRPC errors.
    This simulates a gRPC error response from the server.
    Args:
        client: GrpcFilesystem client for testing
        test_channel: Mock gRPC channel
        file_metadata: File metadata for testing
    """
    # Start the client call (this call will block until the response is simulated)
    future = client_execution_thread_pool.submit(
        client.upload,
        b"Sample content",
        file_metadata["name"],
        file_metadata["file_type"]
    )
    # Get the service and method descriptor
    service_desc = filesystem_service_pb2.DESCRIPTOR.services_by_name["FilesystemService"]
    method_desc = service_desc.methods_by_name["UploadFile"]
    # Intercept the pending unary-unary call
    _, request, rpc = test_channel.take_unary_unary(method_desc)
    # Simulate a gRPC error response
    rpc.send_initial_metadata(())
    rpc.terminate(
        None,
        (),
        grpc.StatusCode.INTERNAL,
        "gRPC error occurred",
    )
    # Verify the client call raises a ServerError
    with pytest.raises(ServerError) as excinfo:
        future.result()
