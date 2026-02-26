"""Digital Kin UserProfile Service gRPC Client."""

from typing import Any

from agentic_mesh_protocol.user_profile.v1 import (
    user_profile_pb2,
    user_profile_service_pb2_grpc,
)
from google.protobuf import json_format

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.base_strategy import RequestContext
from digitalkin.services.user_profile.user_profile_strategy import UserProfileServiceError, UserProfileStrategy


class GrpcUserProfile(UserProfileStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC client implementation for the UserProfile service."""

    service_name: str = "UserProfileService"

    def __init__(
        self,
        client_config: ClientConfig,
    ) -> None:
        """Initialize the user profile service.

        Args:
            client_config: Client configuration for gRPC connection
        """
        super().__init__()
        channel = self._init_channel(client_config)
        self.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(channel)
        logger.debug("Channel client 'UserProfile' initialized successfully")

    async def get_user_profile(self, ctx: RequestContext) -> dict[str, Any]:
        """Get user profile by mission_id (which maps to user_id).

        Args:
            ctx: Request context carrying mission/setup IDs.

        Returns:
            dict[str, Any]: User profile data

        Raises:
            UserProfileServiceError: If the user profile cannot be retrieved
            ServerError: If gRPC operation fails
        """
        async with self.handle_grpc_errors("GetUserProfile", UserProfileServiceError):
            # mission_id typically contains user context
            request = user_profile_pb2.GetUserProfileRequest(mission_id=ctx.mission_id)
            response = await self.exec_grpc_query("GetUserProfile", request)

            if not response.success:
                msg = f"Failed to get user profile for mission_id: {ctx.mission_id}"
                logger.error(msg)
                raise UserProfileServiceError(msg)

            # Convert proto to dict
            user_profile_dict = json_format.MessageToDict(
                response.user_profile,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )

            logger.debug(f"Retrieved user profile for mission_id: {ctx.mission_id}")
            return user_profile_dict
