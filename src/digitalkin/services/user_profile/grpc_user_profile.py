"""Digital Kin UserProfile Service gRPC Client."""

from typing import Any

import grpc
from digitalkin_proto.agentic_mesh_protocol.user_profile.v1 import (
    user_profile_pb2,
    user_profile_service_pb2_grpc,
)
from google.protobuf import json_format

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.identity.identity_strategy import IdentityStrategy


class UserProfileServiceError(Exception):
    """Base exception for UserProfile service errors."""


class GrpcUserProfile(IdentityStrategy, GrpcClientWrapper):
    """This class implements the gRPC user profile service."""

    def __init__(self, user_id: str, config: ClientConfig) -> None:
        """Initialize the user profile service.

        Args:
            user_id: The user ID to fetch profile for
            config: Client configuration for gRPC connection
        """
        self.user_id = user_id
        channel = self._init_channel(config)
        self.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(channel)
        logger.debug("Channel client 'user_profile' initialized successfully")

    async def get_identity(self) -> str:
        """Get the identity (user_id).

        Returns:
            str: The user ID
        """
        return self.user_id

    def get_user_profile(self) -> dict[str, Any]:
        """Get user profile by user_id.

        Returns:
            dict[str, Any]: User profile data

        Raises:
            UserProfileServiceError: If the user profile cannot be retrieved
            ServerError: If gRPC operation fails
        """
        try:
            request = user_profile_pb2.GetUserProfileRequest(user_id=self.user_id)
            response = self.exec_grpc_query("GetUserProfile", request)

            if not response.success:
                msg = f"Failed to get user profile for user_id: {self.user_id}"
                logger.error(msg)
                raise UserProfileServiceError(msg)

            # Convert proto to dict
            user_profile_dict = json_format.MessageToDict(
                response.user_profile,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )

            logger.debug(f"Retrieved user profile for user_id: {self.user_id}")
            return user_profile_dict

        except grpc.RpcError as e:
            msg = f"gRPC GetUserProfile failed: {e}"
            logger.exception(msg)
            raise ServerError(msg) from e
        except Exception as e:
            msg = f"Unexpected error in GetUserProfile: {e}"
            logger.exception(msg)
            raise UserProfileServiceError(msg) from e
