"""Test file for Filesystem Servicer from the client side."""

import logging
import secrets
import string

import grpc
from digitalkin_proto.digitalkin.filesystem.v2 import (
    filesystem_pb2,
    filesystem_service_pb2_grpc,
)
from digitalkin_proto.digitalkin.filesystem.v2.filesystem_pb2 import (
    FileType as FileTypeProto,
    File as FileProto,
    FileResult,
)
from digitalkin.services.filesystem.filesystem_strategy import (
    FilesystemData,
    FileType,
)
from pydantic import ValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --- Fake Context for Servicer ---
class FakeContext:
    def __init__(self) -> None:
        self._code = grpc.StatusCode.OK
        self._details = ""

    def set_code(self, code) -> None:
        self._code = code

    def set_details(self, details) -> None:
        self._details = details


class MockFilesystemServicer(filesystem_service_pb2_grpc.FilesystemServiceServicer):
    """Implementation of the MockFilesystemServicer."""

    alphabet = string.ascii_letters + string.digits

    def __init__(self) -> None:
        """Initialize the filesystem servicer with an empty files dictionary."""
        super().__init__()
        self.files: dict[str, dict[str, FilesystemData]] = {}  # kin_context -> {name: file_data}

    def _generate_url(self, kin_context: str, name: str) -> str:
        """Generate a fake URL for a file.

        Args:
            kin_context: The context of the file
            name: The name of the file

        Returns:
            str: A fake URL for the file
        """
        random_id = "".join(secrets.choice(self.alphabet) for _ in range(8))
        return f"https://storage.example.com/{kin_context}/{random_id}/{name}"

    def UploadFile(
        self, request: filesystem_pb2.UploadFileRequest, context: grpc.ServicerContext
    ) -> filesystem_pb2.UploadFileResponse:
        """Upload a file to the mock filesystem.

        Args:
            request: The UploadFileRequest containing the file to upload
            context: The gRPC context

        Returns:
            filesystem_pb2.UploadFileResponse: The response containing the uploaded file
        """
        try:
            kin_context = request.kin_context
            name = request.name

            # Initialize the kin_context dict if it doesn't exist
            if kin_context not in self.files:
                self.files[kin_context] = {}

            # Check if file already exists
            if name in self.files[kin_context]:
                msg = f"File {name} already exists in context {kin_context}"
                logger.warning(msg)
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(msg)
                return filesystem_pb2.UploadFileResponse()

            try:
                # Convert proto file type to our enum
                file_type = FileType[FileTypeProto.Name(request.file_type)]
            except ValueError as e:
                msg = f"Invalid file type: {str(e)}"
                logger.warning(msg)
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(msg)
                return filesystem_pb2.UploadFileResponse()

            # Create the file data
            url = self._generate_url(kin_context, name)

            file_data = FilesystemData(
                kin_context=kin_context,
                name=name,
                file_type=file_type,
                url=url
            )

            # Store the file
            self.files[kin_context][name] = file_data
            logger.info(f"Uploaded file {name} to context {kin_context}")

            # Convert to proto and return
            file_proto = FileProto(
                kin_context=file_data.kin_context,
                name=file_data.name,
                file_type=getattr(FileTypeProto, file_data.file_type.name),
                url=file_data.url
            )

            return filesystem_pb2.UploadFileResponse(file=file_proto)

        except ValidationError as e:
            msg = f"Validation error: {str(e)}"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return filesystem_pb2.UploadFileResponse()
        except Exception as e:
            msg = f"Unexpected error in UploadFile: {str(e)}"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(msg)
            return filesystem_pb2.UploadFileResponse()

    def GetFileByName(
        self, request: filesystem_pb2.GetFileByNameRequest, context: grpc.ServicerContext
    ) -> filesystem_pb2.GetFileByNameResponse:
        """Get a file by name from the mock filesystem.

        Args:
            request: The GetFileByNameRequest containing the name of the file to get
            context: The gRPC context

        Returns:
            filesystem_pb2.GetFileByNameResponse: The response containing the file
        """
        try:
            kin_context = request.kin_context
            name = request.name

            # Check if kin_context exists
            if kin_context not in self.files:
                msg = f"Context {kin_context} does not exist"
                logger.warning(msg)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(msg)
                return filesystem_pb2.GetFileByNameResponse()

            # Check if file exists
            if name not in self.files[kin_context]:
                msg = f"File {name} does not exist in context {kin_context}"
                logger.warning(msg)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(msg)
                return filesystem_pb2.GetFileByNameResponse()

            # Return the file
            file_data = self.files[kin_context][name]
            file_proto = FileProto(
                kin_context=file_data.kin_context,
                name=file_data.name,
                file_type=getattr(FileTypeProto, file_data.file_type.name),
                url=file_data.url
            )

            return filesystem_pb2.GetFileByNameResponse(file=file_proto)
        except Exception as e:
            msg = f"Unexpected error in GetFileByName: {str(e)}"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(msg)
            return filesystem_pb2.GetFileByNameResponse()

    def GetFilesByKinContext(
        self, request: filesystem_pb2.GetFilesByKinContextRequest, context: grpc.ServicerContext
    ) -> filesystem_pb2.GetFilesByKinContextResponse:
        """Get all files for a specific kin context.

        Args:
            request: The GetFilesByKinContextRequest containing the kin context
            context: The gRPC context

        Returns:
            filesystem_pb2.GetFilesByKinContextResponse: The response containing all files
        """
        try:
            kin_context = request.kin_context

            # Check if kin_context exists
            if kin_context not in self.files:
                # Return empty list rather than error, as this is a common case
                logger.info(f"Context {kin_context} does not exist or is empty")
                return filesystem_pb2.GetFilesByKinContextResponse(files=[])

            # Return all files in the context
            file_protos = []
            for file_data in self.files[kin_context].values():
                file_proto = FileProto(
                    kin_context=file_data.kin_context,
                    name=file_data.name,
                    file_type=getattr(FileTypeProto, file_data.file_type.name),
                    url=file_data.url
                )
                file_protos.append(file_proto)

            return filesystem_pb2.GetFilesByKinContextResponse(files=file_protos)
        except Exception as e:
            msg = f"Unexpected error in GetFilesByKinContext: {str(e)}"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(msg)
            return filesystem_pb2.GetFilesByKinContextResponse(files=[])

    def GetFilesByNames(
        self, request: filesystem_pb2.GetFilesByNamesRequest, context: grpc.ServicerContext
    ) -> filesystem_pb2.GetFilesByNamesResponse:
        """Get multiple files by name.

        Args:
            request: The GetFilesByNamesRequest containing the names of the files to get
            context: The gRPC context

        Returns:
            filesystem_pb2.GetFilesByNamesResponse: The response containing the files
        """
        try:
            kin_context = request.kin_context
            names = request.names

            # Check if kin_context exists
            if kin_context not in self.files:
                msg = f"Context {kin_context} does not exist"
                logger.warning(msg)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(msg)
                raise grpc.RpcError(msg)

            # Get all files that exist
            file_protos = {}
            for name in names:
                if name in self.files[kin_context]:
                    file_data = self.files[kin_context][name]
                    file_proto = FileProto(
                        kin_context=file_data.kin_context,
                        name=file_data.name,
                        file_type=getattr(FileTypeProto, file_data.file_type.name),
                        url=file_data.url
                    )
                    file_protos[name] = FileResult(file=file_proto)
                else:
                    logger.info(f"File {name} does not exist in context {kin_context}")
                    file_protos[name] = FileResult(error="File Not Found")

            return filesystem_pb2.GetFilesByNamesResponse(files=file_protos)
        except Exception as e:
            msg = f"Unexpected error in GetFilesByNames: {str(e)}"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(msg)
            return filesystem_pb2.GetFilesByNamesResponse(files={})

    def UpdateFile(
        self, request: filesystem_pb2.UpdateFileRequest, context: grpc.ServicerContext
    ) -> filesystem_pb2.UpdateFileResponse:
        """Update a file in the mock filesystem.

        Args:
            request: The UpdateFileRequest containing the file to update
            context: The gRPC context
        Returns:
            filesystem_pb2.UpdateFileResponse: The response containing the updated file
        """
        try:
            kin_context = request.kin_context
            name = request.name

            # Check if kin_context exists
            if kin_context not in self.files:
                msg = f"Context {kin_context} does not exist"
                logger.warning(msg)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(msg)
                return filesystem_pb2.UpdateFileResponse()

            # Check if file exists
            if name not in self.files[kin_context]:
                msg = f"File {name} does not exist in context {kin_context}"
                logger.warning(msg)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(msg)
                return filesystem_pb2.UpdateFileResponse()

            # Update the file data
            file_data = self.files[kin_context][name]
            file_data.file_type = FileType[FileTypeProto.Name(request.file_type)]
            file_data.url = self._generate_url(kin_context, name)

            # Convert to proto and return
            file_proto = FileProto(
                kin_context=file_data.kin_context,
                name=file_data.name,
                file_type=getattr(FileTypeProto, file_data.file_type.name),
                url=file_data.url
            )

            return filesystem_pb2.UpdateFileResponse(file=file_proto)
        except ValidationError as e:
            msg = f"Validation error: {str(e)}"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return filesystem_pb2.UpdateFileResponse()
        except Exception as e:
            msg = f"Unexpected error in UpdateFile: {str(e)}"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(msg)
            return filesystem_pb2.UpdateFileResponse()

    def DeleteFile(
        self, request: filesystem_pb2.DeleteFileRequest, context: grpc.ServicerContext
    ) -> filesystem_pb2.DeleteFileResponse:
        """Delete a file from the mock filesystem.

        Args:
            request: The DeleteFileRequest containing the name of the file to delete
            context: The gRPC context

        Returns:
            filesystem_pb2.DeleteFileResponse: The response indicating success or failure
        """
        try:
            kin_context = request.kin_context
            name = request.name

            # Check if kin_context exists
            if kin_context not in self.files:
                msg = f"Context {kin_context} does not exist"
                logger.warning(msg)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(msg)
                return filesystem_pb2.DeleteFileResponse()


            # Check if file exists
            if name not in self.files[kin_context]:
                msg = f"File {name} does not exist in context {kin_context}"
                logger.warning(msg)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(msg)
                return filesystem_pb2.DeleteFileResponse()

            # Delete the file
            del self.files[kin_context][name]
            logger.info(f"Deleted file {name} from context {kin_context}")

            return filesystem_pb2.DeleteFileResponse()
        except Exception as e:
            msg = f"Unexpected error in DeleteFile: {str(e)}"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(msg)
            return filesystem_pb2.DeleteFileResponse()
