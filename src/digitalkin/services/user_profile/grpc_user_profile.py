"""Digital Kin UserProfile Service gRPC Client."""

from typing import Any, cast

from agentic_mesh_protocol.user_profile.v1 import (
    user_profile_pb2,
    user_profile_service_pb2_grpc,
)

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.user_profile.exceptions import UserProfileServiceError
from digitalkin.services.user_profile.user_profile_strategy import UserProfileStrategy
from digitalkin.utils.proto_utils import ProtoUtils


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
        self._init_channel(client_config)
        self.stub = self._get_or_create_stub(user_profile_service_pb2_grpc.UserProfileServiceStub)
        logger.debug("Channel client 'UserProfile' initialized successfully")

    async def close(self) -> None:
        """Release this instance's pooled gRPC channel ref."""
        await self.close_channel()

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

            user_profile_dict = ProtoUtils.proto_to_dict(response.user_profile, with_defaults=True)

            logger.debug("Retrieved user profile for mission_id: %s", self.mission_id)
            return user_profile_dict

    async def check_resource_access(self, resource_type: int, resource_id: str) -> bool:
        """Check whether the caller may access a resource (e.g. a setup).

        Args:
            resource_type: The ResourceType enum value (e.g. RESOURCE_TYPE_SETUP).
            resource_id: The resource identifier (e.g. the setup_id).

        Returns:
            True if access is granted, False otherwise.

        Raises:
            UserProfileServiceError: If the gRPC operation fails.
        """
        async with self.handle_grpc_errors("CheckResourceAccess", UserProfileServiceError):
            request = user_profile_pb2.CheckResourceAccessRequest(
                resource_type=cast("user_profile_pb2.ResourceType", resource_type),
                resource_id=resource_id,
            )
            response = await self.exec_grpc_query("CheckResourceAccess", request)
            return response.allowed
