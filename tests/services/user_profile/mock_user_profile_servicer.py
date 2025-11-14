"""Mock UserProfile Servicer for testing the GrpcUserProfile service."""

from typing import Any

import grpc
from digitalkin_proto.agentic_mesh_protocol.user_profile.v1 import (
    user_profile_pb2,
    user_profile_service_pb2_grpc,
)

from digitalkin.logger import logger


# --- Fake Context for Servicer ---
class FakeContext:
    """Fake gRPC context for testing."""

    def __init__(self) -> None:
        """Initialize with OK status."""
        self._code = grpc.StatusCode.OK
        self._details = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        """Set the gRPC status code.

        Args:
            code: The status code to set
        """
        self._code = code

    def set_details(self, details: str) -> None:
        """Set the error details.

        Args:
            details: The error message
        """
        self._details = details


class MockUserProfileServicer(user_profile_service_pb2_grpc.UserProfileServiceServicer):
    """Mock implementation of the UserProfile Service Servicer for testing."""

    def __init__(self) -> None:
        """Initialize the mock servicer with empty user profile storage."""
        super().__init__()
        # user_id -> user_profile_data
        self.user_profiles: dict[str, dict[str, Any]] = {}

    def GetUserProfile(
        self, request: user_profile_pb2.GetUserProfileRequest, context: grpc.ServicerContext
    ) -> user_profile_pb2.GetUserProfileResponse:
        """Get a user profile by user_id.

        Args:
            request: GetUserProfileRequest containing user_id
            context: gRPC context

        Returns:
            GetUserProfileResponse: Response containing user profile or empty if not found
        """
        try:
            if not request.user_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("User ID is required")
                return user_profile_pb2.GetUserProfileResponse(success=False)

            # Try to find the user profile
            user_profile_data = self.user_profiles.get(request.user_id)

            if not user_profile_data:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"User profile for user_id {request.user_id} not found")
                return user_profile_pb2.GetUserProfileResponse(success=False)

            # Create user profile proto
            subscription = user_profile_pb2.Subscription(**user_profile_data.get("subscription", {}))
            credits = user_profile_pb2.Credits(**user_profile_data.get("credits", {}))
            metadata = user_profile_pb2.Metadata(**user_profile_data.get("metadata", {}))

            user_profile = user_profile_pb2.UserProfile(
                user_id=user_profile_data["user_id"],
                organisation_id=user_profile_data["organisation_id"],
                email=user_profile_data["email"],
                first_name=user_profile_data.get("first_name", ""),
                last_name=user_profile_data.get("last_name", ""),
                locale=user_profile_data.get("locale", ""),
                subscription=subscription,
                credits=credits,
                metadata=metadata,
            )

            logger.info(f"Retrieved user profile for user_id: {request.user_id}")
            return user_profile_pb2.GetUserProfileResponse(success=True, user_profile=user_profile)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in GetUserProfile: {e}", exc_info=True)
            return user_profile_pb2.GetUserProfileResponse(success=False)

    def add_user_profile(self, user_profile_data: dict[str, Any]) -> None:
        """Helper method to add a user profile to the mock storage.

        Args:
            user_profile_data: Dictionary containing user profile data
        """
        user_id = user_profile_data["user_id"]
        self.user_profiles[user_id] = user_profile_data
        logger.debug(f"Added user profile for user_id: {user_id}")
