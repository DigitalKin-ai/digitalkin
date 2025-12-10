"""Comprehensive tests for GrpcUserProfile service.

This test suite validates the GrpcUserProfile service implementation, including:
- Getting user profiles by mission_id
- Handling missing user profiles
- Validating user profile data structure
- Error handling and edge cases
"""

import logging
from concurrent import futures

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.user_profile.v1 import (
    user_profile_pb2,
    user_profile_service_pb2,
    user_profile_service_pb2_grpc,
)
from tests.fixtures.grpc_fixtures import FakeContext
from tests.services.user_profile.mock_user_profile_servicer import MockUserProfileServicer

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.user_profile.grpc_user_profile import GrpcUserProfile

# Set timeout for all tests in this file (20 seconds)
pytestmark = pytest.mark.timeout(20)

# --- Test Constants ---
MISSION_ID = "missions:test_mission_123"
USER_ID = "users:test_user_123"
ORGANISATION_ID = "organisations:test_org_456"

# Module-level variables required by grpc_test_server fixture
service_instance = MockUserProfileServicer()
service_name = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"]

test_logger = logging.getLogger(__name__)


# --- Fixtures ---


@pytest.fixture
def thread_pool():
    """Create thread pool and ensure cleanup.

    Returns:
        ThreadPoolExecutor instance
    """
    test_logger.info("Creating thread pool...")
    pool = futures.ThreadPoolExecutor(max_workers=10)
    yield pool
    test_logger.info("Shutting down thread pool...")
    pool.shutdown(wait=True, cancel_futures=True)
    test_logger.info("Thread pool shut down")


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Create a test gRPC channel.

    Returns:
        A testing channel for intercepting gRPC calls
    """
    test_logger.info("Creating test channel...")
    test_clock = grpc_testing.strict_real_time()
    channel = grpc_testing.channel([service_name], test_clock)
    test_logger.info("Test channel created")
    return channel


@pytest.fixture
def mock_servicer() -> MockUserProfileServicer:
    """Create a mock user profile servicer.

    Returns:
        Mock servicer instance
    """
    test_logger.info("Creating mock servicer...")
    servicer = MockUserProfileServicer()
    test_logger.info("Mock servicer created")
    return servicer


@pytest.fixture
def dummy_client_config() -> ClientConfig:
    """Create a dummy ClientConfig for testing.

    Returns:
        ClientConfig instance with test values
    """
    from digitalkin.models.grpc_servers.models import SecurityMode, ServerMode

    return ClientConfig(
        host="[::]",
        port=50051,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )


@pytest.fixture
def client(
    test_channel: grpc_testing.Channel,
    dummy_client_config: ClientConfig,
) -> GrpcUserProfile:
    """Create a GrpcUserProfile client with test channel.

    Args:
        test_channel: Test gRPC channel
        dummy_client_config: Dummy client configuration

    Returns:
        GrpcUserProfile client configured for testing
    """
    test_logger.info("Creating client...")
    # Initialize with mission_id, setup_id, setup_version_id, client_config
    # Using USER_ID as mission_id for backward compatibility with tests
    client = GrpcUserProfile(
        mission_id=MISSION_ID,
        setup_id="setups:test_setup",
        setup_version_id="setup_versions:test_version",
        client_config=dummy_client_config,
    )
    # Override the channel and stub to use our test channel
    client.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)
    test_logger.info("Client created")
    return client


@pytest.fixture
def sample_user_profile_response() -> user_profile_pb2.GetUserProfileResponse:
    """Create a sample user profile response proto for testing.

    Returns:
        GetUserProfileResponse proto
    """
    user_profile = user_profile_pb2.UserProfile(
        user_id=USER_ID,
        organisation_id=ORGANISATION_ID,
        email="test.user@example.com",
        first_name="Test",
        last_name="User",
        locale="en_US",
        subscription=user_profile_pb2.Subscription(
            tier="premium",
            status="active",
        ),
        credits=[
            user_profile_pb2.CreditLot(
                source="subscription",
                total=1000,
                remaining=750.0,
            )
        ],
        metadata={"security_key": "test_security_key_123"},
    )
    return user_profile_pb2.GetUserProfileResponse(success=True, user_profile=user_profile)


# ============================================================================
# get_user_profile() Tests
# ============================================================================


class TestGetUserProfileSuccess:
    """Tests for successful user profile retrieval with various data structures."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_user_profile_success(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        sample_user_profile_response: user_profile_pb2.GetUserProfileResponse,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully retrieving a user profile."""
        mock_servicer.add_user_profile(MISSION_ID, sample_user_profile_response)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.mission_id == MISSION_ID

        context = FakeContext()
        response = mock_servicer.GetUserProfile(request, context)
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)

        assert result is not None
        assert result["user_id"] == USER_ID
        assert result["organisation_id"] == ORGANISATION_ID
        assert result["email"] == "test.user@example.com"
        assert result["first_name"] == "Test"
        assert result["last_name"] == "User"
        assert result["locale"] == "en_US"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_user_profile_with_subscription(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        sample_user_profile_response: user_profile_pb2.GetUserProfileResponse,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with subscription data."""
        mock_servicer.add_user_profile(MISSION_ID, sample_user_profile_response)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.GetUserProfile(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)

        assert "subscription" in result
        subscription = result["subscription"]
        assert subscription["tier"] == "premium"
        assert subscription["status"] == "active"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_user_profile_with_credits(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        sample_user_profile_response: user_profile_pb2.GetUserProfileResponse,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with credits data."""
        mock_servicer.add_user_profile(MISSION_ID, sample_user_profile_response)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.GetUserProfile(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)

        assert "credits" in result
        credits = result["credits"]
        assert len(credits) > 0
        assert credits[0]["total"] == "1000"
        assert credits[0]["remaining"] == 750.0

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_user_profile_with_metadata(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        sample_user_profile_response: user_profile_pb2.GetUserProfileResponse,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with metadata."""
        mock_servicer.add_user_profile(MISSION_ID, sample_user_profile_response)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.GetUserProfile(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)

        assert "metadata" in result
        metadata = result["metadata"]
        assert "security_key" in metadata
        assert metadata["security_key"] == "test_security_key_123"


class TestGetUserProfileValidation:
    """Tests for user profile validation and error handling."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_get_user_profile_not_found(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test getting a non-existent user profile raises error."""
        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.GetUserProfile(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), context._code, context._details)

        with pytest.raises(Exception):
            future.result(timeout=5.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_user_profile_with_minimal_data(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with minimal required fields."""
        minimal_profile = user_profile_pb2.UserProfile(
            user_id=USER_ID,
            organisation_id=ORGANISATION_ID,
            email="minimal@example.com",
        )
        minimal_response = user_profile_pb2.GetUserProfileResponse(success=True, user_profile=minimal_profile)
        mock_servicer.add_user_profile(MISSION_ID, minimal_response)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.GetUserProfile(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)

        assert result["user_id"] == USER_ID
        assert result["email"] == "minimal@example.com"


class TestGetUserProfileEdgeCases:
    """Tests for edge cases and special scenarios in user profile handling."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_user_profile_with_special_characters_in_email(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with special characters in email."""
        profile = user_profile_pb2.UserProfile(
            user_id=USER_ID,
            organisation_id=ORGANISATION_ID,
            email="test.user+tag@example.co.uk",
        )
        response = user_profile_pb2.GetUserProfileResponse(success=True, user_profile=profile)
        mock_servicer.add_user_profile(MISSION_ID, response)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        resp = mock_servicer.GetUserProfile(request, context)
        rpc.terminate(resp, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result["email"] == "test.user+tag@example.co.uk"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_user_profile_with_unicode_names(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with Unicode characters in names."""
        profile = user_profile_pb2.UserProfile(
            user_id=USER_ID,
            organisation_id=ORGANISATION_ID,
            email="test@example.com",
            first_name="José",
            last_name="François-müller",
        )
        response = user_profile_pb2.GetUserProfileResponse(success=True, user_profile=profile)
        mock_servicer.add_user_profile(MISSION_ID, response)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        resp = mock_servicer.GetUserProfile(request, context)
        rpc.terminate(resp, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result["first_name"] == "José"
        assert result["last_name"] == "François-müller"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_user_profile_with_different_locales(
        self,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        dummy_client_config: ClientConfig,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving user profiles with various locale settings."""
        locales = ["en_US", "fr_FR", "ja_JP", "pt_BR", "zh_CN"]

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        for i, locale in enumerate(locales):
            mission_id = f"missions:mission_{i}"
            profile = user_profile_pb2.UserProfile(
                user_id=f"users:user_{i}",
                organisation_id=ORGANISATION_ID,
                email=f"user{i}@example.com",
                locale=locale,
            )
            response = user_profile_pb2.GetUserProfileResponse(success=True, user_profile=profile)
            mock_servicer.add_user_profile(mission_id, response)

            test_client = GrpcUserProfile(
                mission_id=mission_id,
                setup_id="setups:test_setup",
                setup_version_id="setup_versions:test_version",
                client_config=dummy_client_config,
            )
            test_client.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)

            future = thread_pool.submit(test_client.get_user_profile)
            _, request, rpc = test_channel.take_unary_unary(method_desc)

            context = FakeContext()
            resp = mock_servicer.GetUserProfile(request, context)
            rpc.terminate(resp, (), grpc.StatusCode.OK, "")

            result = future.result(timeout=5.0)
            assert result["locale"] == locale

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_get_user_profile_with_zero_credits(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with zero credits."""
        profile = user_profile_pb2.UserProfile(
            user_id=USER_ID,
            organisation_id=ORGANISATION_ID,
            email="test@example.com",
            credits=[
                user_profile_pb2.CreditLot(
                    source="subscription",
                    total=0,
                    remaining=0.0,
                )
            ],
        )
        response = user_profile_pb2.GetUserProfileResponse(success=True, user_profile=profile)
        mock_servicer.add_user_profile(MISSION_ID, response)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        resp = mock_servicer.GetUserProfile(request, context)
        rpc.terminate(resp, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        credits = result["credits"]
        assert len(credits) > 0
        assert credits[0]["total"] == "0"
        assert credits[0]["remaining"] == 0.0

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_user_profile_with_expired_subscription(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with expired subscription."""
        profile = user_profile_pb2.UserProfile(
            user_id=USER_ID,
            organisation_id=ORGANISATION_ID,
            email="test@example.com",
            subscription=user_profile_pb2.Subscription(
                tier="premium",
                status="expired",
            ),
        )
        response = user_profile_pb2.GetUserProfileResponse(success=True, user_profile=profile)
        mock_servicer.add_user_profile(MISSION_ID, response)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        resp = mock_servicer.GetUserProfile(request, context)
        rpc.terminate(resp, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        subscription = result["subscription"]
        assert subscription["status"] == "expired"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_multiple_user_profiles_independence(
        self,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        dummy_client_config: ClientConfig,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test that multiple user profiles are independent."""
        mission1_id = "missions:mission_1"
        mission2_id = "missions:mission_2"

        profile1 = user_profile_pb2.UserProfile(
            user_id="users:user_1",
            organisation_id=ORGANISATION_ID,
            email="user1@example.com",
            first_name="User",
            last_name="One",
            credits=[user_profile_pb2.CreditLot(source="subscription", total=100, remaining=100.0)],
        )
        response1 = user_profile_pb2.GetUserProfileResponse(success=True, user_profile=profile1)
        mock_servicer.add_user_profile(mission1_id, response1)

        profile2 = user_profile_pb2.UserProfile(
            user_id="users:user_2",
            organisation_id=ORGANISATION_ID,
            email="user2@example.com",
            first_name="User",
            last_name="Two",
            credits=[user_profile_pb2.CreditLot(source="subscription", total=200, remaining=200.0)],
        )
        response2 = user_profile_pb2.GetUserProfileResponse(success=True, user_profile=profile2)
        mock_servicer.add_user_profile(mission2_id, response2)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        # Get user 1 profile
        client1 = GrpcUserProfile(
            mission_id=mission1_id,
            setup_id="setups:test_setup",
            setup_version_id="setup_versions:test_version",
            client_config=dummy_client_config,
        )
        client1.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)

        future1 = thread_pool.submit(client1.get_user_profile)
        _, request1, rpc1 = test_channel.take_unary_unary(method_desc)
        context1 = FakeContext()
        resp1 = mock_servicer.GetUserProfile(request1, context1)
        rpc1.terminate(resp1, (), grpc.StatusCode.OK, "")
        result1 = future1.result(timeout=5.0)

        # Get user 2 profile
        client2 = GrpcUserProfile(
            mission_id=mission2_id,
            setup_id="setups:test_setup",
            setup_version_id="setup_versions:test_version",
            client_config=dummy_client_config,
        )
        client2.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)

        future2 = thread_pool.submit(client2.get_user_profile)
        _, request2, rpc2 = test_channel.take_unary_unary(method_desc)
        context2 = FakeContext()
        resp2 = mock_servicer.GetUserProfile(request2, context2)
        rpc2.terminate(resp2, (), grpc.StatusCode.OK, "")
        result2 = future2.result(timeout=5.0)

        # Verify independence
        assert result1["user_id"] == "users:user_1"
        assert result2["user_id"] == "users:user_2"
        assert result1["email"] == "user1@example.com"
        assert result2["email"] == "user2@example.com"
        assert result1["credits"][0]["total"] == "100"
        assert result2["credits"][0]["total"] == "200"
