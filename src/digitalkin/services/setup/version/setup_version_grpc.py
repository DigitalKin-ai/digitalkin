"""Digital Kin Setup Service gRPC Client."""

from typing import Any

from agentic_mesh_protocol.setup.v1 import setup_version_dto_pb2, setup_version_service_pb2_grpc
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from pydantic import ValidationError

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.setup.setup_models import SetupVersionData
from digitalkin.services.setup.version.setup_version_strategy import SetupVersionStrategy


class GrpcSetupVersion(SetupVersionStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """This class implements the gRPC setup service."""

    def __init__(
            self,
            mission_id: str,
            setup_id: str,
            setup_version_id: str,
            client_config: ClientConfig,
            config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the gRPC setup version strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version this strategy is associated with
            client_config: Configuration for the gRPC client connection
            config: Configuration for the filesystem strategy
        """
        super().__init__(mission_id, setup_id, setup_version_id, config)
        self.service_name = "SetupVersionService"
        channel = self._init_channel(client_config)
        self.stub = setup_version_service_pb2_grpc.SetupVersionServiceStub(channel)
        logger.debug("Channel client 'SetupVersion' initialized successfully")

    # ═════════════════════════════════ Private Methods ══════════════════════════════════ #

    def __post_init__(self, config: ClientConfig) -> None:
        """Init the channel from a config file.

        Need to be call if the user register a gRPC channel.
        """
        channel = self._init_channel(config)
        self.stub = setup_version_service_pb2_grpc.SetupVersionServiceStub(channel)
        logger.debug("Channel client 'setup' initialized successfully")

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #
    def create(self, setup_version_dict: dict[str, Any]) -> str:
        with self.handle_grpc_errors("Setup Version Creation"):
            valid_data = SetupVersionData.model_validate(setup_version_dict)
            content_struct = Struct()
            content_struct.update(valid_data.content)
            request = setup_version_dto_pb2.CreateSetupVersionRequest(
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

    def get(self, setup_version_dict: dict[str, Any]) -> SetupVersionData:
        with self.handle_grpc_errors("Get Setup Version"):
            setup_version_id = setup_version_dict.get("setup_version_id")
            if not setup_version_id:
                msg = "Setup version id is required"
                raise ValidationError(msg)
            request = setup_version_dto_pb2.GetSetupVersionRequest(setup_version_id=setup_version_id)
            response = self.exec_grpc_query("GetSetupVersion", request)
            return SetupVersionData(
                **json_format.MessageToDict(response.result.version, preserving_proto_field_name=True)
            )

    def search(self, setup_version_dict: dict[str, Any]) -> list[SetupVersionData]:
        with self.handle_grpc_errors("Search Setup Versions"):
            if "name" not in setup_version_dict and "version" not in setup_version_dict:
                msg = "Either name or version must be provided"
                raise ValidationError(msg)
            request = setup_version_dto_pb2.SearchSetupVersionsRequest(
                setup_id=setup_version_dict.get("setup_id", ""),
                version=setup_version_dict.get("version", ""),
            )
            response = self.exec_grpc_query("SearchSetupVersions", request)
            return [
                SetupVersionData(**json_format.MessageToDict(sv_result.version, preserving_proto_field_name=True))
                for sv_result in response.result
            ]

    def update(self, setup_version_dict: dict[str, Any]) -> bool:
        with self.handle_grpc_errors("Setup Version Update"):
            valid_data = SetupVersionData.model_validate(setup_version_dict)
            content_struct = Struct()
            content_struct.update(valid_data.content)
            request = setup_version_dto_pb2.UpdateSetupVersionRequest(
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
            return response.result.success

    def delete(self, setup_version_dict: dict[str, Any]) -> bool:
        with self.handle_grpc_errors("Setup Version Deletion"):
            setup_version_id = setup_version_dict.get("setup_version_id")
            if not setup_version_id:
                msg = "Setup version id is required for deletion"
                raise ValidationError(msg)
            request = setup_version_dto_pb2.DeleteSetupVersionRequest(setup_version_id=setup_version_id)
            response = self.exec_grpc_query("DeleteSetupVersion", request)
            logger.debug("Setup Version '%s' query sent successfully", setup_version_id)
            return response.result.success
