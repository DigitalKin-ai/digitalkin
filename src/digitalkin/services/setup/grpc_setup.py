"""Digital Kin Setup Service gRPC Client."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import grpc
from agentic_mesh_protocol.setup.v1 import (
    setup_pb2,
    setup_service_pb2_grpc,
)
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from pydantic import ValidationError

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.setup.setup_strategy import SetupData, SetupServiceError, SetupStrategy, SetupVersionData
from digitalkin.utils.proto_utils import proto_to_dict


class GrpcSetup(SetupStrategy, GrpcClientWrapper):
    """gRPC client implementation for the Setup service.

    Communicates with the remote SetupService gRPC server to manage
    setup configurations and versions.
    """

    service_name: str = "SetupService"

    def __post_init__(self, config: ClientConfig) -> None:
        """Init the channel from a config file.

        Need to be call if the user register a gRPC channel.
        """
        channel = self._init_channel(config)
        self.stub = setup_service_pb2_grpc.SetupServiceStub(channel)
        logger.debug("Channel client 'setup' initialized successfully")

    @asynccontextmanager
    async def handle_grpc_errors(  # noqa: PLR6301
        self, operation: str
    ) -> AsyncGenerator[Any, Any]:  # Mixin: self available for subclass overrides
        """Context manager for consistent gRPC error handling with detailed logging.

        Args:
            operation: Description of the operation being performed (e.g., "Get Setup", "Create Setup Version").

        Yields:
            Allow error handling in context.

        Raises:
            ValueError: Pydantic model validation failed - input data is malformed.
            ServerError: gRPC communication failed - remote service returned error or is unreachable.
            SetupServiceError: Unexpected error during setup operation - includes connection/timeout issues.
        """
        try:
            yield
        except ValidationError as e:
            msg = f"Validation failed for {operation}: {e}"
            logger.error(
                "ValidationError in %s: %s",
                operation,
                e,
                extra={"operation": operation, "error_type": "ValidationError", "service_name": "SetupService"},
            )
            raise ValueError(msg) from e
        except grpc.RpcError as e:
            status_code = e.code().name if e.code() else "UNKNOWN"
            details = e.details() or str(e)
            msg = f"gRPC {operation} [{status_code}]: {details}"
            logger.error(
                "gRPC %s [%s]: %s",
                operation,
                status_code,
                details,
                extra={"operation": operation, "error_type": "grpc.RpcError", "grpc_code": status_code},
            )
            raise ServerError(msg) from e
        except (TimeoutError, ConnectionError, OSError) as e:
            error_type = type(e).__name__
            msg = f"{error_type} in {operation}: {e}"
            logger.error(
                "%s in %s: %s",
                error_type,
                operation,
                e,
                extra={"operation": operation, "error_type": error_type, "service_name": "SetupService"},
            )
            raise SetupServiceError(msg) from e
        except Exception as e:
            error_type = type(e).__name__
            msg = f"Unexpected {error_type} in {operation}: {e}"
            logger.error(
                "Unexpected %s in %s: %s",
                error_type,
                operation,
                e,
                extra={"operation": operation, "error_type": error_type, "service_name": "SetupService"},
                exc_info=True,
            )
            raise SetupServiceError(msg) from e

    async def create_setup(self, setup_dict: dict[str, Any]) -> str:
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
        async with self.handle_grpc_errors("Setup Creation"):
            valid_data = SetupData.model_validate(setup_dict)

            request = setup_pb2.CreateSetupRequest(
                name=valid_data.name,
                organisation_id=valid_data.organisation_id,
                owner_id=valid_data.owner_id,
                module_id=valid_data.module_id,
                current_setup_version=setup_pb2.SetupVersion(**valid_data.current_setup_version.model_dump()),
            )
            response = await self.exec_grpc_query("CreateSetup", request)
            logger.debug("Setup '%s' query sent successfully", valid_data.name)
            return response

    async def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
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
        async with self.handle_grpc_errors("Get Setup"):
            if "setup_id" not in setup_dict:
                msg = "Setup name is required"
                raise ValidationError(msg)

            request = setup_pb2.GetSetupRequest(
                setup_id=setup_dict["setup_id"],
                version=setup_dict.get("version", ""),
            )
            response = await self.exec_grpc_query("GetSetup", request)
            response_data = proto_to_dict(response)
            return SetupData(**response_data["setup"])

    async def update_setup(self, setup_dict: dict[str, Any]) -> bool:
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

        async with self.handle_grpc_errors("Setup Update"):
            valid_data = SetupData.model_validate(setup_dict)

            if valid_data.current_setup_version is not None:
                current_setup_version = setup_pb2.SetupVersion(**valid_data.current_setup_version.model_dump())

            request = setup_pb2.UpdateSetupRequest(
                setup_id=valid_data.id,
                name=valid_data.name,
                owner_id=valid_data.owner_id or "",
                current_setup_version=current_setup_version,
            )
            response = await self.exec_grpc_query("UpdateSetup", request)
            logger.debug("Setup '%s' query sent successfully", valid_data.name)
            return response.success

    async def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
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
        async with self.handle_grpc_errors("Setup Deletion"):
            setup_id = setup_dict.get("setup_id")
            if not setup_id:
                msg = "Setup name is required for deletion"
                raise ValidationError(msg)
            request = setup_pb2.DeleteSetupRequest(setup_id=setup_id)
            response = await self.exec_grpc_query("DeleteSetup", request)
            logger.debug("Setup '%s' query sent successfully", setup_id)
            return response.success

    async def create_setup_version(self, setup_version_dict: dict[str, Any]) -> str:
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
        async with self.handle_grpc_errors("Setup Version Creation"):
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
            return await self.exec_grpc_query("CreateSetupVersion", request)

    async def get_setup_version(self, setup_version_dict: dict[str, Any]) -> SetupVersionData:
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
        async with self.handle_grpc_errors("Get Setup Version"):
            setup_version_id = setup_version_dict.get("setup_version_id")
            if not setup_version_id:
                msg = "Setup version id is required"
                raise ValidationError(msg)
            request = setup_pb2.GetSetupVersionRequest(setup_version_id=setup_version_id)
            response = await self.exec_grpc_query("GetSetupVersion", request)
            return SetupVersionData(**proto_to_dict(response.setup_version))

    async def search_setup_versions(self, setup_version_dict: dict[str, Any]) -> list[SetupVersionData]:
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
        async with self.handle_grpc_errors("Search Setup Versions"):
            if "name" not in setup_version_dict and "version" not in setup_version_dict:
                msg = "Either name or version must be provided"
                raise ValidationError(msg)
            request = setup_pb2.SearchSetupVersionsRequest(
                setup_id=setup_version_dict.get("setup_id", ""),
                version=setup_version_dict.get("version", ""),
            )
            response = await self.exec_grpc_query("SearchSetupVersions", request)
            return [SetupVersionData(**proto_to_dict(sv)) for sv in response.setup_versions]

    async def update_setup_version(self, setup_version_dict: dict[str, Any]) -> bool:
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
        async with self.handle_grpc_errors("Setup Version Update"):
            valid_data = SetupVersionData.model_validate(setup_version_dict)
            content_struct = Struct()
            content_struct.update(valid_data.content)
            request = setup_pb2.UpdateSetupVersionRequest(
                setup_version_id=valid_data.id,
                version=valid_data.version,
                content=content_struct,
            )
            response = await self.exec_grpc_query("UpdateSetupVersion", request)
            logger.debug(
                "Setup Version '%s' for setup '%s' query sent successfully",
                valid_data.id,
                valid_data.setup_id,
            )
            return response.success

    async def delete_setup_version(self, setup_version_dict: dict[str, Any]) -> bool:
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
        async with self.handle_grpc_errors("Setup Version Deletion"):
            setup_version_id = setup_version_dict.get("setup_version_id")
            if not setup_version_id:
                msg = "Setup version id is required for deletion"
                raise ValidationError(msg)
            request = setup_pb2.DeleteSetupVersionRequest(setup_version_id=setup_version_id)
            response = await self.exec_grpc_query("DeleteSetupVersion", request)
            logger.debug("Setup Version '%s' query sent successfully", setup_version_id)
            return response.success

    async def list_setups(self, list_dict: dict[str, Any]) -> dict[str, Any]:
        """List setups with optional filtering and pagination.

        Args:
            list_dict: Dictionary with optional filters:
                - organisation_id: Filter by organisation
                - owner_id: Filter by owner
                - limit: Maximum number of results
                - offset: Number of results to skip

        Returns:
            dict[str, Any]: Dictionary with 'setups' list and 'total_count'.

        Raises:
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        async with self.handle_grpc_errors("List Setups"):
            request = setup_pb2.ListSetupsRequest(
                organisation_id=list_dict.get("organisation_id", ""),
                owner_id=list_dict.get("owner_id", ""),
                limit=list_dict.get("limit", 0),
                offset=list_dict.get("offset", 0),
            )
            response = await self.exec_grpc_query("ListSetups", request)
            return {
                "setups": [proto_to_dict(setup) for setup in response.setups],
                "total_count": response.total_count,
            }
