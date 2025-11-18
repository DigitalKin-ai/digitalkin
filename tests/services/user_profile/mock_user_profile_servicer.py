"""Mock UserProfile Servicer for testing the GrpcUserProfile service."""

from datetime import datetime
from typing import Any

import grpc
from digitalkin_proto.agentic_mesh_protocol.user_profile.v1 import (
    user_profile_pb2,
    user_profile_service_pb2_grpc,
)
from google.protobuf import timestamp_pb2

from digitalkin.logger import logger
from tests.fixtures.grpc_fixtures import FakeContext


class MockUserProfileServicer(user_profile_service_pb2_grpc.UserProfileServiceServicer):
    """Mock implementation of the UserProfile Service Servicer for testing."""

    def __init__(self) -> None:
        """Initialize the mock servicer with empty user profile storage."""
        super().__init__()
        # user_id -> user_profile_data
        self.user_profiles: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _convert_to_timestamp(timestamp_str: str | None) -> timestamp_pb2.Timestamp | None:
        """Convert ISO format timestamp string to protobuf Timestamp.

        Args:
            timestamp_str: ISO format timestamp string (e.g., "2024-01-01T00:00:00Z")

        Returns:
            Protobuf Timestamp object or None if input is None/empty
        """
        if not timestamp_str:
            return None

        # Parse ISO format timestamp
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(dt)
        return ts

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
            # Handle subscription with timestamp conversion
            subscription_data = user_profile_data.get("subscription", {})
            subscription_kwargs = {}
            if "tier" in subscription_data:
                subscription_kwargs["tier"] = subscription_data["tier"]
            if "status" in subscription_data:
                subscription_kwargs["status"] = subscription_data["status"]
            if "start" in subscription_data:
                start_ts = self._convert_to_timestamp(subscription_data["start"])
                if start_ts:
                    subscription_kwargs["start"] = start_ts
            if "end" in subscription_data:
                end_ts = self._convert_to_timestamp(subscription_data["end"])
                if end_ts:
                    subscription_kwargs["end"] = end_ts
            subscription = user_profile_pb2.Subscription(**subscription_kwargs)

            # Handle credits with timestamp conversion
            credits_data = user_profile_data.get("credits", {})
            credits_kwargs = {}
            if "allocated" in credits_data:
                credits_kwargs["allocated"] = credits_data["allocated"]
            if "used" in credits_data:
                credits_kwargs["used"] = credits_data["used"]
            if "timestamp" in credits_data:
                timestamp_ts = self._convert_to_timestamp(credits_data["timestamp"])
                if timestamp_ts:
                    credits_kwargs["timestamp"] = timestamp_ts
            credits = user_profile_pb2.Credits(**credits_kwargs)

            # Handle metadata - it's a google.protobuf.Struct, pass dict directly
            metadata_dict = user_profile_data.get("metadata", {})

            user_profile = user_profile_pb2.UserProfile(
                user_id=user_profile_data["user_id"],
                organisation_id=user_profile_data["organisation_id"],
                email=user_profile_data["email"],
                first_name=user_profile_data.get("first_name", ""),
                last_name=user_profile_data.get("last_name", ""),
                locale=user_profile_data.get("locale", ""),
                subscription=subscription,
                credits=credits,
                metadata=metadata_dict,
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
