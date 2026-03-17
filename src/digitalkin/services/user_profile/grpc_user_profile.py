"""Digital Kin UserProfile Service gRPC Client."""

from typing import Any

from agentic_mesh_protocol.user_profile.v1 import (
    user_profile_pb2,
    user_profile_service_pb2_grpc,
)

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.user_profile.user_profile_strategy import UserProfileServiceError, UserProfileStrategy
from digitalkin.utils.proto_utils import proto_to_dict


class GrpcUserProfile(UserProfileStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC client implementation for the UserProfile service."""

    service_name: str = "UserProfileService"

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        client_config: ClientConfig,
    ) -> None:
        """Initialize the user profile service.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version
            client_config: Client configuration for gRPC connection
        """
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id)
        channel = self._init_channel(client_config)
        self.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(channel)
        logger.debug("Channel client 'UserProfile' initialized successfully")

    async def get_user_profile(self) -> dict[str, Any] | None:
        """Get user profile by mission_id (which maps to user_id).

        Returns:
            User profile data, or None if not found.

        Raises:
            UserProfileServiceError: If the gRPC operation fails.
        """
        async with self.handle_grpc_errors("GetUserProfile", UserProfileServiceError):
            request = user_profile_pb2.GetUserProfileRequest(mission_id=self.mission_id)
            response = await self.exec_grpc_query("GetUserProfile", request)

            if not response.success:
                logger.warning("No user profile found for mission_id: %s", self.mission_id)
                return None

            user_profile_dict = proto_to_dict(response.user_profile, with_defaults=True)

            logger.debug("Retrieved user profile for mission_id: %s", self.mission_id)
            return user_profile_dict
