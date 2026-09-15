"""Digital Kin Setup Service gRPC Client."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import grpc
from agentic_mesh_protocol.pagination.v1 import pagination_pb2
from agentic_mesh_protocol.setup.v1 import (
    setup_dto_pb2,
    setup_messages_pb2,
    setup_service_pb2_grpc,
    setup_version_dto_pb2,
    setup_version_service_pb2_grpc,
)
from pydantic import ValidationError

from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.registry import RegistrySetupStatus
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.services.setup.setup_strategy import (
    SetupData,
    SetupPage,
    SetupStrategy,
    SetupVersionData,
    SetupVersionPage,
)
from digitalkin.utils.json_structure import JsonStructure
from digitalkin.utils.proto_utils import ProtoUtils
from digitalkin.utils.setup_content_validator import SetupContentValidator


class SetupServicesStub(
    setup_service_pb2_grpc.SetupServiceStub,
    setup_version_service_pb2_grpc.SetupVersionServiceStub,
):
    """SetupService and SetupVersionService RPCs bound to one channel.

    ``exec_grpc_query`` resolves every RPC on ``self.stub``; one stub carrying both
    services keeps the version RPCs on the same retry and circuit-breaker path.
    """

    def __init__(self, channel: grpc.aio.Channel) -> None:
        """Bind both services' RPCs to the channel.

        Args:
            channel: The setup backend channel.
        """
        setup_service_pb2_grpc.SetupServiceStub.__init__(self, channel)
        setup_version_service_pb2_grpc.SetupVersionServiceStub.__init__(self, channel)


class GrpcSetup(SetupStrategy, GrpcClientWrapper):
    """gRPC client implementation for the Setup service.

    Communicates with the remote SetupService and SetupVersionService to manage
    setup configurations. Owner/organization/module of a created setup are resolved
    server-side from the request context metadata.
    """

    service_name: str = "SetupService"

    def __post_init__(self, config: ClientConfig) -> None:
        """Init the channel from a config file.

        Need to be call if the user register a gRPC channel.
        """
        self._init_channel(config)
        self.stub = self._get_or_create_stub(SetupServicesStub)
        logger.debug("Channel client 'setup' initialized successfully")

    async def close(self) -> None:
        """Release this instance's pooled gRPC channel ref."""
        await self.close_channel()

    @asynccontextmanager
    async def handle_grpc_errors(  # ruff: ignore[no-self-use]
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
            SetupServiceError: The result carried an OperationError, or an unexpected error occurred
                during the setup operation - includes connection/timeout issues.
        """
        try:
            yield
        except PermissionDeniedError:
            raise
        except ServerError:
            # Already normalised by exec_grpc_query (status code + details) — pass through.
            raise
        except SetupServiceError as e:
            logger.error("%s refused: %s", operation, e)
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
    def _to_setup_data(setup_msg: setup_messages_pb2.Setup) -> SetupData:
        """Validate a result's ``Setup``, its embedded ``current_setup_version`` included.

        Args:
            setup_msg: The ``Setup`` held by a ``SetupResult``.

        Returns:
            The validated ``SetupData``.

        Raises:
            SetupServiceError: The setup carries no version, or a version without content.
        """
        version = setup_msg.current_setup_version
        if not version.id:
            msg = f"setup '{setup_msg.id}' returned without a setup version"
            raise SetupServiceError(msg)
        # An unset content Struct is dropped by proto_to_dict rather than rendered as {}, so
        # SetupData would fail with a bare "Field required" naming neither setup nor version.
        if not version.HasField("content"):
            msg = f"setup '{setup_msg.id}' version '{version.id}' arrived with no content"
            raise SetupServiceError(msg)
        return SetupData(**ProtoUtils.proto_to_dict(setup_msg, with_defaults=True))

    async def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with 'setup_id', optional 'version' and optional
                'structure_key'. The wire takes one key path; the server projects the
                version content down to it.

        Returns:
            The setup with its current version populated.

        Raises:
            ValueError: If the setup_id is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the result holds an OperationError (NOT_FOUND included for
                a structure_key the content does not have) or an unexpected error occurs.
        """
        if not setup_dict.get("setup_id"):
            msg = "setup_id is required"
            raise ValueError(msg)
        async with self.handle_grpc_errors("Get Setup"):
            # Proto3 optional: a None kwarg leaves the field unset; "" would fail min_len.
            request = setup_dto_pb2.GetSetupRequest(
                setup_id=setup_dict["setup_id"],
                version=setup_dict.get("version") or None,
                structure_key=setup_dict.get("structure_key") or None,
            )
            response = await self.exec_grpc_query("GetSetup", request)
            GrpcErrorHandlerMixin.raise_on_error(response.result, SetupServiceError)
            return self._to_setup_data(response.result.setup)

    async def list_setups(self, setup_dict: dict[str, Any]) -> SetupPage:
        """List setups, optionally filtered.

        Args:
            setup_dict: Dictionary with optional 'organization_id', 'owner_id' and
                'module_id' filters, optional 'statuses' (``RegistrySetupStatus`` members or
                their names) and optional 'limit' (clamped to 1..100, default 20) / 'offset'.

        Returns:
            The requested page and the total count of matching setups. A result holding an
            OperationError, or a setup that has no version yet, is dropped and logged.

        Raises:
            ValueError: If a status is UNSPECIFIED or unknown.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        statuses = [RegistrySetupStatus(status) for status in setup_dict.get("statuses") or ()]
        if RegistrySetupStatus.UNSPECIFIED in statuses:  # fail closed: a dropped status would widen the filter
            msg = f"invalid statuses {setup_dict['statuses']!r}; each must name a defined setup status"
            raise ValueError(msg)
        async with self.handle_grpc_errors("List Setups"):
            # Proto3 optional filters: None leaves them unset; "" would fail the id prefix rule.
            # Statuses travel by name — SetupStatus was renumbered, so values never map by number.
            request = setup_dto_pb2.ListSetupsRequest(
                organization_id=setup_dict.get("organization_id") or None,
                owner_id=setup_dict.get("owner_id") or None,
                module_id=setup_dict.get("module_id") or None,
                statuses=[status.name for status in statuses],
                pagination=pagination_pb2.PaginationRequest(
                    limit=min(max(int(setup_dict.get("limit") or 20), 1), 100),
                    offset=int(setup_dict.get("offset") or 0),
                ),
            )
            response = await self.exec_grpc_query("ListSetups", request)
            setups = []
            for result in GrpcErrorHandlerMixin.successful_results("ListSetups", response.results):
                if not result.setup.current_setup_version.id:
                    logger.warning(
                        "ListSetups dropped setup %s (%s): no setup version yet", result.identifier, result.setup.name
                    )
                    continue
                setups.append(self._to_setup_data(result.setup))
            return SetupPage(setups=setups, total_count=response.bulk.pagination.total_count)

    async def create_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Create a new setup; owner/organization/module derive from the request context.

        Args:
            setup_dict: Dictionary with 'name', 'content', optional 'documentation' and
                optional 'structure' — the ``{key path: description}`` map the agent
                wrote for ``content``, stored as written with only each description's
                length bounded.

        Returns:
            The created setup with its initial version and structure.

        Raises:
            ValueError: If name or content is missing, or output_format_spec or documentation
                is oversized.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the result holds an OperationError or an unexpected error occurs.
        """
        if not setup_dict.get("name") or not isinstance(setup_dict.get("content"), dict):
            msg = "name and content (object) are required"
            raise ValueError(msg)
        # Outside handle_grpc_errors: an input guard must stay a ValueError, not become a
        # SetupServiceError via the catch-all.
        SetupContentValidator.reject_oversized_output_format_spec(setup_dict["content"])
        SetupContentValidator.reject_oversized_documentation(setup_dict.get("documentation") or "")
        async with self.handle_grpc_errors("Setup Creation"):
            request = setup_dto_pb2.CreateSetupRequest(
                name=setup_dict["name"],
                revision=setup_messages_pb2.SetupRevision(
                    content=setup_dict["content"],
                    structure=JsonStructure.clip(setup_dict.get("structure") or {}),
                    documentation=setup_dict.get("documentation") or "",
                ),
            )
            response = await self.exec_grpc_query("CreateSetup", request)
            GrpcErrorHandlerMixin.raise_on_error(response.result, SetupServiceError)
            logger.debug("Setup '%s' created successfully", setup_dict["name"])
            return self._to_setup_data(response.result.setup)

    async def update_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Update a setup's name and current version content.

        Args:
            setup_dict: Dictionary with 'setup_id', 'name', 'content', optional
                'set_as_current' (defaults to True), optional 'documentation' and optional
                'structure'. The map belongs to the content it describes, so a revision
                carries only the map its own call supplied; omitting it leaves the new
                revision without one.

        Returns:
            The updated setup with its current version and structure.

        Raises:
            ValueError: If setup_id, name or content is missing, or output_format_spec or
                documentation is oversized.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the result holds an OperationError or an unexpected error occurs.
        """
        if (
            not setup_dict.get("setup_id")
            or not setup_dict.get("name")
            or not isinstance(setup_dict.get("content"), dict)
        ):
            msg = "setup_id, name and content (object) are required"
            raise ValueError(msg)
        SetupContentValidator.reject_oversized_output_format_spec(setup_dict["content"])
        SetupContentValidator.reject_oversized_documentation(setup_dict.get("documentation") or "")
        async with self.handle_grpc_errors("Setup Update"):
            # The revision cuts a new version rather than editing in place; without
            # set_as_current the setup would keep serving the old content.
            request = setup_dto_pb2.UpdateSetupRequest(
                setup_id=setup_dict["setup_id"],
                name=setup_dict["name"],
                revision=setup_messages_pb2.SetupRevision(
                    content=setup_dict["content"],
                    structure=JsonStructure.clip(setup_dict.get("structure") or {}),
                    documentation=setup_dict.get("documentation") or "",
                ),
                set_as_current=bool(setup_dict.get("set_as_current", True)),
            )
            response = await self.exec_grpc_query("UpdateSetup", request)
            GrpcErrorHandlerMixin.raise_on_error(response.result, SetupServiceError)
            logger.debug("Setup '%s' updated successfully", setup_dict["setup_id"])
            return self._to_setup_data(response.result.setup)

    async def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_id'.

        Returns:
            bool: True once deleted, False when the result holds an OperationError.

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
            request = setup_dto_pb2.DeleteSetupRequest(setup_id=setup_id)
            result = (await self.exec_grpc_query("DeleteSetup", request)).result
            if result.WhichOneof("outcome") == "error":
                logger.warning("Setup '%s' deletion refused: %s %s", setup_id, result.error.code, result.error.message)
                return False
            logger.debug("Setup '%s' deleted", setup_id)
            return True

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
            SetupServiceError: If the result holds an OperationError or an unexpected error occurs.
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
            request = setup_dto_pb2.ChangeVisibilityRequest(setup_id=setup_id, visibility=scope.upper())
            response = await self.exec_grpc_query("ChangeVisibility", request)
            GrpcErrorHandlerMixin.raise_on_error(response.result, SetupServiceError)
            logger.debug("Setup '%s' visibility changed to %s", setup_id, scope)
            return self._to_setup_data(response.result.setup)

    async def create_setup_version(self, setup_dict: dict[str, Any]) -> SetupVersionData:
        """Cut a new version of a setup.

        Args:
            setup_dict: Dictionary with 'setup_id', 'version' (the label), 'content',
                optional 'structure', optional 'documentation' and optional
                'set_as_current' (defaults to False: the version is staged).

        Returns:
            The created version.

        Raises:
            ValueError: If setup_id, version or content is missing, or output_format_spec or
                documentation is oversized.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the result holds an OperationError or an unexpected error occurs.
        """
        if (
            not setup_dict.get("setup_id")
            or not setup_dict.get("version")
            or not isinstance(setup_dict.get("content"), dict)
        ):
            msg = "setup_id, version and content (object) are required"
            raise ValueError(msg)
        SetupContentValidator.reject_oversized_output_format_spec(setup_dict["content"])
        SetupContentValidator.reject_oversized_documentation(setup_dict.get("documentation") or "")
        async with self.handle_grpc_errors("Setup Version Creation"):
            request = setup_version_dto_pb2.CreateSetupVersionRequest(
                setup_id=setup_dict["setup_id"],
                version=setup_dict["version"],
                revision=setup_messages_pb2.SetupRevision(
                    content=setup_dict["content"],
                    structure=JsonStructure.clip(setup_dict.get("structure") or {}),
                    documentation=setup_dict.get("documentation") or "",
                ),
                set_as_current=bool(setup_dict.get("set_as_current")),
            )
            response = await self.exec_grpc_query("CreateSetupVersion", request)
            GrpcErrorHandlerMixin.raise_on_error(response.result, SetupServiceError)
            logger.debug("Setup '%s' version '%s' created", setup_dict["setup_id"], setup_dict["version"])
            return SetupVersionData(**ProtoUtils.proto_to_dict(response.result.version, with_defaults=True))

    async def get_setup_version(self, setup_dict: dict[str, Any]) -> SetupVersionData:
        """Retrieve a setup version by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_version_id'.

        Returns:
            The requested version.

        Raises:
            ValueError: If the setup_version_id is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the result holds an OperationError or an unexpected error occurs.
        """
        setup_version_id = setup_dict.get("setup_version_id")
        if not setup_version_id:
            msg = "setup_version_id is required"
            raise ValueError(msg)
        async with self.handle_grpc_errors("Get Setup Version"):
            request = setup_version_dto_pb2.GetSetupVersionRequest(setup_version_id=setup_version_id)
            response = await self.exec_grpc_query("GetSetupVersion", request)
            GrpcErrorHandlerMixin.raise_on_error(response.result, SetupServiceError)
            return SetupVersionData(**ProtoUtils.proto_to_dict(response.result.version, with_defaults=True))

    async def update_setup_version(self, setup_dict: dict[str, Any]) -> SetupVersionData:
        """Edit a setup version in place; only the supplied fields are sent.

        Args:
            setup_dict: Dictionary with 'setup_version_id' and at least one of 'version',
                'content', 'documentation' (``""`` clears it) or 'structure'. A missing or
                ``None`` field stays unset on the wire, so the server leaves it unchanged.

        Returns:
            The updated version.

        Raises:
            ValueError: If setup_version_id is missing, no field is supplied, content is not an
                object, or output_format_spec or documentation is oversized.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the result holds an OperationError or an unexpected error occurs.
        """
        setup_version_id = setup_dict.get("setup_version_id")
        version = setup_dict.get("version") or None
        content = setup_dict.get("content")
        documentation = setup_dict.get("documentation")
        structure = setup_dict.get("structure")
        if not setup_version_id:
            msg = "setup_version_id is required"
            raise ValueError(msg)
        # The request's CEL rule refuses an update carrying no field.
        if version is None and content is None and documentation is None and structure is None:
            msg = "at least one of version, content, documentation or structure is required"
            raise ValueError(msg)
        if content is not None and not isinstance(content, dict):
            msg = "content must be an object"
            raise ValueError(msg)
        SetupContentValidator.reject_oversized_output_format_spec(content or {})
        SetupContentValidator.reject_oversized_documentation(documentation or "")
        async with self.handle_grpc_errors("Setup Version Update"):
            # A None kwarg leaves the field unset; "" stays a real documentation value (clears it).
            request = setup_version_dto_pb2.UpdateSetupVersionRequest(
                setup_version_id=setup_version_id,
                version=version,
                content=content,
                documentation=documentation,
                structure=None if structure is None else JsonStructure.clip(structure),
            )
            response = await self.exec_grpc_query("UpdateSetupVersion", request)
            GrpcErrorHandlerMixin.raise_on_error(response.result, SetupServiceError)
            logger.debug("Setup version '%s' updated", setup_version_id)
            return SetupVersionData(**ProtoUtils.proto_to_dict(response.result.version, with_defaults=True))

    async def delete_setup_version(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup version by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_version_id'.

        Returns:
            bool: True once deleted, False when the result holds an OperationError.

        Raises:
            ValueError: If the setup_version_id is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        setup_version_id = setup_dict.get("setup_version_id")
        if not setup_version_id:
            msg = "setup_version_id is required for deletion"
            raise ValueError(msg)
        async with self.handle_grpc_errors("Setup Version Deletion"):
            request = setup_version_dto_pb2.DeleteSetupVersionRequest(setup_version_id=setup_version_id)
            result = (await self.exec_grpc_query("DeleteSetupVersion", request)).result
            if result.WhichOneof("outcome") == "error":
                logger.warning(
                    "Setup version '%s' deletion refused: %s %s",
                    setup_version_id,
                    result.error.code,
                    result.error.message,
                )
                return False
            logger.debug("Setup version '%s' deleted", setup_version_id)
            return True

    async def list_setup_versions(self, setup_dict: dict[str, Any]) -> SetupVersionPage:
        """List a setup's versions, most recent first.

        Args:
            setup_dict: Dictionary with 'setup_id' and optional 'limit' / 'offset'.

        Returns:
            The requested page, its total count and the currently active version id. A result
            holding an OperationError is dropped from the page and logged.

        Raises:
            ValueError: If the setup_id is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: For any unexpected internal error.
        """
        setup_id = setup_dict.get("setup_id")
        if not setup_id:
            msg = "setup_id is required"
            raise ValueError(msg)
        async with self.handle_grpc_errors("List Setup Versions"):
            # The proto floors limit at 1, so an unset/zero limit would be rejected outright.
            request = setup_version_dto_pb2.ListSetupVersionsRequest(
                setup_id=setup_id,
                pagination=pagination_pb2.PaginationRequest(
                    limit=int(setup_dict.get("limit") or 20),
                    offset=int(setup_dict.get("offset") or 0),
                ),
            )
            response = await self.exec_grpc_query("ListSetupVersions", request)
            return SetupVersionPage(
                setup_versions=[
                    SetupVersionData(**ProtoUtils.proto_to_dict(result.version, with_defaults=True))
                    for result in GrpcErrorHandlerMixin.successful_results("ListSetupVersions", response.results)
                ],
                total_count=response.bulk.pagination.total_count,
                current_setup_version_id=response.current_setup_version_id,
            )

    async def set_current_setup_version(self, setup_dict: dict[str, Any]) -> SetupData:
        """Activate an existing version of a setup, making it the current one.

        Args:
            setup_dict: Dictionary with 'setup_id' and 'setup_version_id'.

        Returns:
            The setup with its newly activated version.

        Raises:
            ValueError: If setup_id or setup_version_id is missing.
            ServerError: If gRPC operation fails.
            SetupServiceError: If the result holds an OperationError or an unexpected error occurs.
        """
        setup_id = setup_dict.get("setup_id")
        setup_version_id = setup_dict.get("setup_version_id")
        if not setup_id or not setup_version_id:
            msg = "setup_id and setup_version_id are required"
            raise ValueError(msg)
        async with self.handle_grpc_errors("Set Current Setup Version"):
            request = setup_version_dto_pb2.SetCurrentSetupVersionRequest(
                setup_id=setup_id,
                setup_version_id=setup_version_id,
            )
            response = await self.exec_grpc_query("SetCurrentSetupVersion", request)
            GrpcErrorHandlerMixin.raise_on_error(response.result, SetupServiceError)
            logger.debug("Setup '%s' now on version %s", setup_id, setup_version_id)
            return self._to_setup_data(response.result.setup)
