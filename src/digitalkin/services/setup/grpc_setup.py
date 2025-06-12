"""Digital Kin Setup Service gRPC Client."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import grpc
from digitalkin_proto.digitalkin.setup.v2 import (
    setup_pb2,
    setup_service_pb2_grpc,
)
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from pydantic import ValidationError

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.models import ClientConfig
from digitalkin.logger import logger
from digitalkin.services.setup.setup_strategy import SetupData, SetupServiceError, SetupStrategy, SetupVersionData


class GrpcSetup(SetupStrategy, GrpcClientWrapper):
    """This class implements the gRPC setup service."""

    def __post_init__(self, config: ClientConfig) -> None:
        """Init the channel from a config file.

        Need to be call if the user register a gRPC channel.
        """
        channel = self._init_channel(config)
        self.stub = setup_service_pb2_grpc.SetupServiceStub(channel)
        logger.debug("Channel client 'setup' initialized succesfully")

    @contextmanager
    def _handle_grpc_errors(self, operation: str) -> Generator[Any, Any, Any]:  # noqa: PLR6301
        """Context manager for consistent gRPC error handling.

        Yields:
            Allow error handling in context.

        Args:
            operation: Description of the operation being performed.

        Raises:
            ValueError: Error wiht the model validation.
            ServerError: from gRPC Client.
            SetupServiceError: setup service internal.
        """
        try:
            yield
        except ValidationError as e:
            msg = f"Invalid data for {operation}"
            logger.exception(msg)
            raise ValueError(msg) from e
        except grpc.RpcError as e:
            msg = f"gRPC {operation} failed: {e}"
            logger.exception(msg)
            raise ServerError(msg) from e
        except Exception as e:
            msg = f"Unexpected error in {operation}"
            logger.exception(msg)
            raise SetupServiceError(msg) from e

    def create_setup(self, setup_dict: dict[str, Any]) -> str:
        """Create a new setup with comprehensive validation.

        Args:
            setup_dict: Dictionary containing setup details.

        Returns:
            bool: Success status of setup creation.

        Raises:
            ValidationError: If setup data is invalid.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        with self._handle_grpc_errors("Setup Creation"):
            valid_data = SetupData.model_validate(setup_dict)

            request = setup_pb2.CreateSetupRequest(
                name=valid_data.name,
                organisation_id=valid_data.organisation_id,
                owner_id=valid_data.owner_id,
                module_id=valid_data.module_id,
                current_setup_version=setup_pb2.SetupVersion(**valid_data.current_setup_version.model_dump()),
            )
            response = self.exec_grpc_query("CreateSetup", request)
            logger.debug("Setup '%s' query sent successfully", valid_data.name)
            return response

    def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with 'name' and optional 'version'.

        Returns:
            dict[str, Any]: Setup details including optional setup version.

        Raises:
            ValidationError: If the setup name is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        with self._handle_grpc_errors("Get Setup"):
            if "setup_id" not in setup_dict:
                msg = "Setup name is required"
                raise ValidationError(msg)
            request = setup_pb2.GetSetupRequest(
                setup_id=setup_dict["setup_id"],
                version=setup_dict.get("version", ""),
            )
            response = self.exec_grpc_query("GetSetup", request)
            response_data = json_format.MessageToDict(response, preserving_proto_field_name=True)
            return SetupData(**response_data["setup"])

    def update_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Update an existing setup.

        Args:
            setup_dict: Dictionary with setup update details.

        Returns:
            bool: Success status of the update operation.

        Raises:
            ValidationError: If setup data is invalid.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        current_setup_version = None

        with self._handle_grpc_errors("Setup Update"):
            valid_data = SetupData.model_validate(setup_dict)

            if valid_data.current_setup_version is not None:
                current_setup_version = setup_pb2.SetupVersion(**valid_data.current_setup_version.model_dump())

            request = setup_pb2.UpdateSetupRequest(
                setup_id=valid_data.id,
                name=valid_data.name,
                owner_id=valid_data.owner_id or "",
                current_setup_version=current_setup_version,
            )
            response = self.exec_grpc_query("UpdateSetup", request)
            logger.debug("Setup '%s' query sent successfully", valid_data.name)
            return getattr(response, "success", False)

    def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the setup 'setup_id'.

        Returns:
            bool: Success status of deletion.

        Raises:
            ValidationError: If the setup setup_id is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        with self._handle_grpc_errors("Setup Deletion"):
            setup_id = setup_dict.get("setup_id")
            if not setup_id:
                msg = "Setup name is required for deletion"
                raise ValidationError(msg)
            request = setup_pb2.DeleteSetupRequest(setup_id=setup_id)
            response = self.exec_grpc_query("DeleteSetup", request)
            logger.debug("Setup '%s' query sent successfully", setup_id)
            return getattr(response, "success", False)

    def create_setup_version(self, setup_version_dict: dict[str, Any]) -> str:
        """Create a new setup version.

        Args:
            setup_version_dict: Dictionary with setup version details.

        Returns:
            str: version of setup version creation.

        Raises:
            ValidationError: If setup version data is invalid.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        with self._handle_grpc_errors("Setup Version Creation"):
            valid_data = SetupVersionData.model_validate(setup_version_dict)
            content_struct = Struct()
            content_struct.update(valid_data.content)
            request = setup_pb2.CreateSetupVersionRequest(
                setup_id=valid_data.setup_id,
                version=valid_data.version,
                content=content_struct,
            )
            logger.debug(
                "Setup Version '%s' for setup '%s' query sent successfully",
                valid_data.version,
                valid_data.setup_id,
            )
            return self.exec_grpc_query("CreateSetupVersion", request)

    def get_setup_version(self, setup_version_dict: dict[str, Any]) -> SetupVersionData:
        """Retrieve a setup version by its unique identifier.

        Args:
            setup_version_dict: Dictionary with the setup version 'setup_version_id'.

        Returns:
            dict[str, Any]: Setup version details.

        Raises:
            ValidationError: If the setup version id is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        with self._handle_grpc_errors("Get Setup Version"):
            setup_version_id = setup_version_dict.get("setup_version_id")
            if not setup_version_id:
                msg = "Setup version id is required"
                raise ValidationError(msg)
            request = setup_pb2.GetSetupVersionRequest(setup_version_id=setup_version_id)
            response = self.exec_grpc_query("GetSetupVersion", request)
            return SetupVersionData(**json_format.MessageToDict(response.setup_version))

    def search_setup_versions(self, setup_version_dict: dict[str, Any]) -> list[SetupVersionData]:
        """Search for setup versions based on filters.

        Args:
            setup_version_dict: Dictionary with optional 'name' and 'version' filters.

        Returns:
            list[dict[str, Any]]: A list of matching setup version details.

        Raises:
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
            ValidationError: If both name and version are not provided.
        """
        with self._handle_grpc_errors("Search Setup Versions"):
            if "name" not in setup_version_dict and "version" not in setup_version_dict:
                msg = "Either name or version must be provided"
                raise ValidationError(msg)
            request = setup_pb2.SearchSetupVersionsRequest(
                setup_id=setup_version_dict.get("setup_id", ""),
                version=setup_version_dict.get("version", ""),
            )
            response = self.exec_grpc_query("SearchSetupVersions", request)
            return [SetupVersionData(**json_format.MessageToDict(sv)) for sv in response.setup_versions]

    def update_setup_version(self, setup_version_dict: dict[str, Any]) -> bool:
        """Update an existing setup version.

        Args:
            setup_version_dict: Dictionary with setup version update details.

        Returns:
            bool: Success status of the update operation.

        Raises:
            ValidationError: If setup version data is invalid.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        with self._handle_grpc_errors("Setup Version Update"):
            valid_data = SetupVersionData.model_validate(setup_version_dict)
            content_struct = Struct()
            content_struct.update(valid_data.content)
            request = setup_pb2.UpdateSetupVersionRequest(
                setup_version_id=valid_data.id,
                version=valid_data.version,
                content=content_struct,
            )
            response = self.exec_grpc_query("UpdateSetupVersion", request)
            logger.debug(
                "Setup Version '%s' for setup '%s' query sent successfully",
                valid_data.id,
                valid_data.setup_id,
            )
            return getattr(response, "success", False)

    def delete_setup_version(self, setup_version_dict: dict[str, Any]) -> bool:
        """Delete a setup version by its unique identifier.

        Args:
            setup_version_dict: Dictionary with the setup version 'name'.

        Returns:
            bool: Success status of version deletion.

        Raises:
            ValidationError: If the setup version name is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        with self._handle_grpc_errors("Setup Version Deletion"):
            setup_version_id = setup_version_dict.get("setup_version_id")
            if not setup_version_id:
                msg = "Setup version id is required for deletion"
                raise ValidationError(msg)
            request = setup_pb2.DeleteSetupVersionRequest(setup_version_id=setup_version_id)
            response = self.exec_grpc_query("DeleteSetupVersion", request)
            logger.debug("Setup Version '%s' query sent successfully", setup_version_id)
            return getattr(response, "success", False)
