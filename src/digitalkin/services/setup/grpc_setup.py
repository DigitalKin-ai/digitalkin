"""Digital Kin Setup Service gRPC Client."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import grpc
from agentic_mesh_protocol.setup.v1 import (
    setup_pb2,
    setup_service_pb2_grpc,
)
from google.protobuf.struct_pb2 import Struct
from pydantic import ValidationError

from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.services.setup.setup_strategy import SetupData, SetupStrategy
from digitalkin.utils.proto_utils import ProtoUtils


class GrpcSetup(SetupStrategy, GrpcClientWrapper):
    """gRPC client implementation for the Setup service.

    Communicates with the remote SetupService gRPC server to manage
    setup configurations. Owner/organisation/module of a created setup
    are resolved server-side from the request context metadata.
    """

    service_name: str = "SetupService"

    def __post_init__(self, config: ClientConfig) -> None:
        """Init the channel from a config file.

        Need to be call if the user register a gRPC channel.
        """
        self._init_channel(config)
        self.stub = self._get_or_create_stub(setup_service_pb2_grpc.SetupServiceStub)
        logger.debug("Channel client 'setup' initialized successfully")

    async def close(self) -> None:
        """Release this instance's pooled gRPC channel ref."""
        await self.close_channel()

    @asynccontextmanager
    async def handle_grpc_errors(  # noqa: PLR6301
        self, operation: str
    ) -> AsyncGenerator[Any, Any]:  # Mixin: self available for subclass overrides
        """Context manager for consistent gRPC error handling with detailed logging.

        Args:
            operation: Description of the operation being performed (e.g., "Get Setup", "Change Visibility").

        Yields:
            Allow error handling in context.

        Raises:
            PermissionDeniedError: Service rejected the call with PERMISSION_DENIED.
            ValueError: Pydantic model validation failed - response data is malformed.
            ServerError: gRPC communication failed - remote service returned error or is unreachable.
            SetupServiceError: Unexpected error during setup operation - includes connection/timeout issues.
        """
        try:
            yield
        except PermissionDeniedError:
            raise
        except ServerError:
            # Already normalised by exec_grpc_query (status code + details) — pass through.
            raise
        except ValidationError as e:
            msg = f"Validation failed for {operation}: {e}"
            logger.error(
                "ValidationError in %s: %s",
                operation,
                e,
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
                exc_info=True,
            )
            raise SetupServiceError(msg) from e

    @staticmethod
    def _to_setup_data(setup_msg: setup_pb2.Setup, version_msg: setup_pb2.SetupVersion) -> SetupData:
        """Assemble a ``SetupData`` from a response's setup + sibling setup_version.

        The setup's embedded ``current_setup_version`` wins when populated;
        otherwise the response-level ``setup_version`` fills it.

        Args:
            setup_msg: The response ``Setup`` message.
            version_msg: The response-level ``SetupVersion`` message.

        Returns:
            The validated ``SetupData``.

        Raises:
            SetupServiceError: If neither carries a setup version.
        """
        if setup_msg.HasField("current_setup_version"):
            version_msg = setup_msg.current_setup_version
        elif not version_msg.id:
            msg = f"setup '{setup_msg.id}' returned without a setup version"
            raise SetupServiceError(msg)
        data = ProtoUtils.proto_to_dict(setup_msg, with_defaults=True)
        data["current_setup_version"] = ProtoUtils.proto_to_dict(version_msg, with_defaults=True)
        return SetupData(**data)

    async def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with 'setup_id' and optional 'version'.

        Returns:
            The setup with its current version populated.

        Raises:
            ValueError: If the setup_id is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        if not setup_dict.get("setup_id"):
            msg = "setup_id is required"
            raise ValueError(msg)
        async with self.handle_grpc_errors("Get Setup"):
            # Proto3 optional: a None kwarg leaves the field unset (no empty-string presence).
            request = setup_pb2.GetSetupRequest(
                setup_id=setup_dict["setup_id"],
                version=setup_dict.get("version") or None,
            )
            response = await self.exec_grpc_query("GetSetup", request)
            return self._to_setup_data(response.setup, response.setup_version)

    async def create_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Create a new setup; owner/organisation/module derive from the request context.

        Args:
            setup_dict: Dictionary with 'name' and 'content'.

        Returns:
            The created setup with its initial version.

        Raises:
            ValueError: If name or content is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the server reports failure or an unexpected error occurs.
        """
        if not setup_dict.get("name") or not isinstance(setup_dict.get("content"), dict):
            msg = "name and content (object) are required"
            raise ValueError(msg)
        async with self.handle_grpc_errors("Setup Creation"):
            content_struct = Struct()
            content_struct.update(setup_dict["content"])
            request = setup_pb2.CreateSetupRequest(name=setup_dict["name"], content=content_struct)
            response = await self.exec_grpc_query("CreateSetup", request)
            if not response.success:
                msg = f"setup creation refused for '{setup_dict['name']}'"
                raise SetupServiceError(msg)
            logger.debug("Setup '%s' created successfully", setup_dict["name"])
            return self._to_setup_data(response.setup, response.setup_version)

    async def update_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Update a setup's name and current version content.

        Args:
            setup_dict: Dictionary with 'setup_id', 'name' and 'content'.

        Returns:
            The updated setup with its current version.

        Raises:
            ValueError: If setup_id, name or content is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the server reports failure or an unexpected error occurs.
        """
        if (
            not setup_dict.get("setup_id")
            or not setup_dict.get("name")
            or not isinstance(setup_dict.get("content"), dict)
        ):
            msg = "setup_id, name and content (object) are required"
            raise ValueError(msg)
        async with self.handle_grpc_errors("Setup Update"):
            content_struct = Struct()
            content_struct.update(setup_dict["content"])
            request = setup_pb2.UpdateSetupRequest(
                setup_id=setup_dict["setup_id"],
                name=setup_dict["name"],
                content=content_struct,
            )
            response = await self.exec_grpc_query("UpdateSetup", request)
            if not response.success:
                msg = f"setup update refused for '{setup_dict['setup_id']}'"
                raise SetupServiceError(msg)
            logger.debug("Setup '%s' updated successfully", setup_dict["setup_id"])
            return self._to_setup_data(response.setup, response.setup_version)

    async def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_id'.

        Returns:
            bool: Success status of deletion.

        Raises:
            ValueError: If the setup_id is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        setup_id = setup_dict.get("setup_id")
        if not setup_id:
            msg = "setup_id is required for deletion"
            raise ValueError(msg)
        async with self.handle_grpc_errors("Setup Deletion"):
            request = setup_pb2.DeleteSetupRequest(setup_id=setup_id)
            response = await self.exec_grpc_query("DeleteSetup", request)
            logger.debug("Setup '%s' deletion query sent successfully", setup_id)
            return response.success

    async def change_visibility(self, setup_dict: dict[str, Any]) -> SetupData:
        """Change a setup's visibility scope.

        Args:
            setup_dict: Dictionary with 'setup_id' and 'visibility'
                (``public`` | ``private`` | ``internal``).

        Returns:
            The setup with its updated visibility.

        Raises:
            ValueError: If setup_id is missing or visibility is not a valid scope.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the server reports failure or an unexpected error occurs.
        """
        setup_id = setup_dict.get("setup_id")
        if not setup_id:
            msg = "setup_id is required"
            raise ValueError(msg)
        scope = str(setup_dict.get("visibility", "")).lower()
        if scope not in {"public", "private", "internal"}:  # fail closed: never send UNSPECIFIED or unknown
            msg = f"invalid visibility '{setup_dict.get('visibility')}'; use 'public', 'private' or 'internal'"
            raise ValueError(msg)
        async with self.handle_grpc_errors("Change Visibility"):
            # Proto ctors accept the enum member name; the guard above keeps it fail-closed.
            request = setup_pb2.ChangeVisibilityRequest(setup_id=setup_id, visibility=f"VISIBILITY_{scope.upper()}")
            response = await self.exec_grpc_query("ChangeVisibility", request)
            if not response.success:
                msg = f"visibility change refused for '{setup_id}'"
                raise SetupServiceError(msg)
            logger.debug("Setup '%s' visibility changed to %s", setup_id, scope)
            return self._to_setup_data(response.setup, response.setup_version)
