"""Mock UserProfile Servicer for testing the GrpcUserProfile service."""

import grpc
from agentic_mesh_protocol.pagination.v1 import bulk_pb2
from agentic_mesh_protocol.user_profile.v1 import (
    user_profile_dto_pb2,
    user_profile_service_pb2_grpc,
    user_profile_messages_pb2
)

from digitalkin.logger import logger


class MockUserProfileServicer(user_profile_service_pb2_grpc.UserProfileServiceServicer):
    """Mock implementation of the UserProfile Service Servicer for testing."""

    def __init__(self) -> None:
        """Initialize the mock servicer with empty user profile storage."""
        super().__init__()
        # mission_id -> user_profile proto response
        self.user_profiles: dict[str, user_profile_dto_pb2.GetUserProfileResponse] = {}

    def add_user_profile(self, mission_id: str, response: user_profile_dto_pb2.GetUserProfileResponse) -> None:
        """Add a user profile response to the mock storage.

        Args:
            mission_id: The mission ID to associate with the profile
            response: The GetUserProfileResponse proto to return
        """
        self.user_profiles[mission_id] = response
        logger.debug(f"Added user profile for mission_id: {mission_id}")

    def GetUserProfile(
            self, request: user_profile_dto_pb2.GetUserProfileRequest, context: grpc.ServicerContext
    ) -> user_profile_dto_pb2.GetUserProfileResponse:
        """Get a user profile by mission_id.

        Args:
            request: GetUserProfileRequest containing mission_id
            context: gRPC context

        Returns:
            GetUserProfileResponse: Response containing user profile or empty if not found
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                result = user_profile_messages_pb2.UserProfileResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)),
                                                                     success=False)
                return user_profile_dto_pb2.GetUserProfileResponse(result=result)

            # Try to find the user profile
            response = self.user_profiles.get(request.mission_id)

            if not response.result.success:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"User profile for mission_id {request.mission_id} not found")
                result = user_profile_messages_pb2.UserProfileResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND)),
                                                                     success=False)
                return user_profile_dto_pb2.GetUserProfileResponse(result=result)

            logger.info(f"Retrieved user profile for mission_id: {request.mission_id}")
            result = user_profile_messages_pb2.UserProfileResult(profile=response.result.profile, success=True)
            return user_profile_dto_pb2.GetUserProfileResponse(result=result)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in GetUserProfile: {e}", exc_info=True)
            result = user_profile_messages_pb2.UserProfileResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL)),
                                                                 success=False)
            return user_profile_dto_pb2.GetUserProfileResponse(result=result)
