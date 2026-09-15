"""Mock UserProfile Servicer for testing the GrpcUserProfile service."""

import grpc
from agentic_mesh_protocol.pagination.v1 import bulk_pb2
from agentic_mesh_protocol.user_profile.v1 import (
    user_profile_dto_pb2,
    user_profile_messages_pb2,
    user_profile_service_pb2_grpc,
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
            The stored response, or one whose result holds a NOT_FOUND OperationError.
        """
        if not request.mission_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Mission ID is required")
            return user_profile_dto_pb2.GetUserProfileResponse()

        response = self.user_profiles.get(request.mission_id)
        if response is None:
            return user_profile_dto_pb2.GetUserProfileResponse(
                result=user_profile_messages_pb2.UserProfileResult(
                    identifier=request.mission_id,
                    error=bulk_pb2.OperationError(
                        code="NOT_FOUND", message=f"User profile for mission_id {request.mission_id} not found"
                    ),
                )
            )

        logger.info(f"Retrieved user profile for mission_id: {request.mission_id}")
        return response
