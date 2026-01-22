"""Digital Kin Setup Service gRPC Client."""

from typing import Any

from agentic_mesh_protocol.pagination.v1.pagination_pb2 import PaginationRequest
from agentic_mesh_protocol.setup.v1 import (
    setup_dto_pb2,
    setup_messages_pb2,
    setup_service_pb2_grpc,
)
from google.protobuf import json_format
from pydantic import ValidationError

from digitalkin.exception.setup import SetupServiceError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.setup import SetupData
from digitalkin.services.setup.setup_strategy import SetupStrategy


class GrpcSetup(SetupStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """This class implements the gRPC setup service."""

    def __init__(
            self,
            mission_id: str | None = None,
            setup_id: str | None = None,
            setup_version_id: str | None = None,
            client_config: ClientConfig = None,
            config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the gRPC setup strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version this strategy is associated with
            client_config: Configuration for the gRPC client connection
            config: Configuration for the filesystem strategy
        """
        super().__init__(mission_id, setup_id, setup_version_id, config)
        self.service_name = "SetupService"
        channel = self._init_channel(client_config)
        self.stub = setup_service_pb2_grpc.SetupServiceStub(channel)
        logger.debug("Channel client 'Setup' initialized successfully")

    # ═════════════════════════════════ Private Methods ══════════════════════════════════ #

    def __post_init__(self, config: ClientConfig) -> None:
        """Init the channel from a config file.

        Need to be call if the user register a gRPC channel.
        """
        channel = self._init_channel(config)
        self.stub = setup_service_pb2_grpc.SetupServiceStub(channel)
        logger.debug("Channel client 'setup' initialized successfully")

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    async def create(self, setup_dict: dict[str, Any]) -> str:
        async with self.handle_grpc_errors("CreateSetup", SetupServiceError):
            valid_data = SetupData.model_validate(setup_dict)

            request = setup_dto_pb2.CreateSetupRequest(
                name=valid_data.name,
                organization_id=valid_data.organization_id,
                owner_id=valid_data.owner_id,
                module_id=valid_data.module_id,
                current_setup_version=setup_messages_pb2.SetupVersion(**valid_data.current_setup_version.model_dump()),
            )
            response = await self.exec_grpc_query("CreateSetup", request)
            logger.debug("Setup '%s' query sent successfully", valid_data.name)
            return response

    async def get(self, setup_dict: dict[str, Any]) -> SetupData:
        async with self.handle_grpc_errors("GetSetup", SetupServiceError):
            if "setup_id" not in setup_dict:
                msg = "Setup name is required"
                raise ValidationError(msg)
            request = setup_dto_pb2.GetSetupRequest(
                setup_id=setup_dict["setup_id"],
                version=setup_dict.get("version", ""),
            )
            response = await self.exec_grpc_query("GetSetup", request)
            response_data = json_format.MessageToDict(response, preserving_proto_field_name=True)
            return SetupData(**response_data["result"]["setup"])

    async def update(self, setup_dict: dict[str, Any]) -> bool:
        current_setup_version = None

        async with self.handle_grpc_errors("SetupUpdate", SetupServiceError):
            valid_data = SetupData.model_validate(setup_dict)

            if valid_data.current_setup_version is not None:
                current_setup_version = setup_messages_pb2.SetupVersion(**valid_data.current_setup_version.model_dump())

            request = setup_dto_pb2.UpdateSetupRequest(
                setup_id=valid_data.id,
                name=valid_data.name,
                owner_id=valid_data.owner_id or "",
                current_setup_version=current_setup_version,
            )
            response = await self.exec_grpc_query("UpdateSetup", request)
            logger.debug("Setup '%s' query sent successfully", valid_data.name)
            return response.result.success

    async def delete(self, setup_dict: dict[str, Any]) -> bool:
        async with self.handle_grpc_errors("SetupDeletion", SetupServiceError):
            setup_id = setup_dict.get("setup_id")
            if not setup_id:
                msg = "Setup name is required for deletion"
                raise ValidationError(msg)
            request = setup_dto_pb2.DeleteSetupRequest(setup_id=setup_id)
            response = await self.exec_grpc_query("DeleteSetup", request)
            logger.debug("Setup '%s' query sent successfully", setup_id)
            return response.result.success

    async def list(self, list_dict: dict[str, Any]) -> dict[str, Any]:
        async with self.handle_grpc_errors("ListSetups", SetupServiceError):
            request = setup_dto_pb2.ListSetupsRequest(
                organization_id=list_dict.get("organization_id", ""),
                owner_id=list_dict.get("owner_id", ""),
                pagination=PaginationRequest(limit=list_dict.get("limit", 0), offset=list_dict.get("offset", 0)),
            )
            response = await self.exec_grpc_query("ListSetups", request)
            return {
                "setups": [
                    json_format.MessageToDict(setup_result.setup, preserving_proto_field_name=True)
                    for setup_result in response.result
                ],
                "total_count": response.bulk.total_process,
            }
