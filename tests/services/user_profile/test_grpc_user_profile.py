"""Comprehensive tests for GrpcUserProfile service.

This test suite validates the GrpcUserProfile service implementation, including:
- Getting user profiles by user_id
- Handling missing user profiles
- Validating user profile data structure
- Error handling and edge cases
"""

import logging
from concurrent import futures

import grpc
import grpc_testing
import pytest
from digitalkin_proto.agentic_mesh_protocol.user_profile.v1 import (
    user_profile_service_pb2,
    user_profile_service_pb2_grpc,
)
from tests.fixtures.grpc_fixtures import FakeContext
from tests.services.user_profile.mock_user_profile_servicer import MockUserProfileServicer

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.user_profile.grpc_user_profile import GrpcUserProfile

# --- Test Constants ---
USER_ID = "users:test_user_123"
ORGANISATION_ID = "organisations:test_org_456"

# Module-level variables required by grpc_test_server fixture
service_instance = MockUserProfileServicer()
service_name = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"]

test_logger = logging.getLogger(__name__)


# --- Fixtures ---


@pytest.fixture(scope="module")
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
        mission_id=USER_ID,
        setup_id="setups:test_setup",
        setup_version_id="setup_versions:test_version",
        client_config=dummy_client_config,
    )
    # Override the channel and stub to use our test channel
    client.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)
    test_logger.info("Client created")
    return client


@pytest.fixture
def sample_user_profile_data() -> dict:
    """Create sample user profile data for testing.

    Returns:
        Dictionary containing user profile data
    """
    return {
        "user_id": USER_ID,
        "organisation_id": ORGANISATION_ID,
        "email": "test.user@example.com",
        "first_name": "Test",
        "last_name": "User",
        "locale": "en_US",
        "subscription": {
            "tier": "premium",
            "status": "active",
            "start": "2024-01-01T00:00:00Z",
            "end": "2025-01-01T00:00:00Z",
        },
        "credits": [
            {
                "source": "subscription",
                "total": 1000,
                "remaining": 750,
                "timestamp": "2024-12-15T10:30:00Z",
            },
        ],
        "metadata": {
            "security_key": "test_security_key_123",
        },
    }


# ============================================================================
# get_user_profile() Tests
# ============================================================================


class TestGetUserProfileSuccess:
    """Tests for successful user profile retrieval with various data structures.

    This test class covers the happy path scenarios where user profiles are
    successfully retrieved with different combinations of data fields including
    basic profile data, subscription information, credits, and metadata.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_user_profile_success(
        self,
        client: GrpcUserProfile,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockUserProfileServicer,
        sample_user_profile_data: dict,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully retrieving a user profile.

        Verifies:
        - User profile is retrieved with correct data
        - All fields are present and accurate
        """
        # Add user profile to mock servicer
        mock_servicer.add_user_profile(sample_user_profile_data)

        # Get the method descriptor
        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        # Execute client call in thread pool
        future = thread_pool.submit(client.get_user_profile)

        # Intercept the call
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        # Verify request
        assert request.mission_id == USER_ID

        # Mock servicer processes the request
        context = FakeContext()
        response = mock_servicer.GetUserProfile(request, context)

        # Send initial metadata and terminate the RPC
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        # Get result
        result = future.result(timeout=5.0)

        # Verify result
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
        sample_user_profile_data: dict,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with subscription data.

        Verifies:
        - Subscription information is correctly included
        - All subscription fields are present
        """
        mock_servicer.add_user_profile(sample_user_profile_data)

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

        # Verify subscription data
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
        sample_user_profile_data: dict,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with credits data.

        Verifies:
        - Credits information is correctly included
        - All credit fields are present and accurate
        """
        mock_servicer.add_user_profile(sample_user_profile_data)

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

        # Verify credits data - now a repeated CreditLot field
        assert "credits" in result
        credits = result["credits"]
        assert len(credits) == 1
        # Protobuf int64 fields are converted to strings in JSON, double fields stay as float
        assert credits[0]["source"] == "subscription"
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
        sample_user_profile_data: dict,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test retrieving a user profile with metadata.

        Verifies:
        - Metadata information is correctly included
        - All metadata fields are present
        """
        mock_servicer.add_user_profile(sample_user_profile_data)

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

        # Verify metadata
        assert "metadata" in result
        metadata = result["metadata"]
        assert "security_key" in metadata
        assert metadata["security_key"] == "test_security_key_123"


class TestGetUserProfileValidation:
    """Tests for user profile validation and error handling.

    This test class covers error scenarios and validation cases including
    handling of non-existent profiles and minimal data requirements.
    """

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
        """Test getting a non-existent user profile raises error.

        Verifies:
        - Attempting to get a non-existent user profile raises ServerError
        - Error message is appropriate
        """
        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        future = thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.GetUserProfile(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), context._code, context._details)

        # Verify error is raised - ServerError is raised when gRPC call fails
        with pytest.raises(ServerError):
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
        """Test retrieving a user profile with minimal required fields.

        Verifies:
        - User profile works with only required fields
        - Optional fields default appropriately
        """
        minimal_data = {
            "user_id": USER_ID,
            "organisation_id": ORGANISATION_ID,
            "email": "minimal@example.com",
            "subscription": {},
            "credits": [],
            "metadata": {},
        }

        mock_servicer.add_user_profile(minimal_data)

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

        # Verify minimal fields are present
        assert result["user_id"] == USER_ID
        assert result["email"] == "minimal@example.com"
        assert "subscription" in result
        assert "credits" in result
        assert "metadata" in result


class TestGetUserProfileEdgeCases:
    """Tests for edge cases and special scenarios in user profile handling.

    This test class covers edge cases including special characters, Unicode,
    different locales, zero credits, expired subscriptions, and concurrent operations.
    """

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
        """Test retrieving a user profile with special characters in email.

        Verifies:
        - Special characters in email are handled correctly
        """
        data = {
            "user_id": USER_ID,
            "organisation_id": ORGANISATION_ID,
            "email": "test.user+tag@example.co.uk",
            "subscription": {},
            "credits": [],
            "metadata": {},
        }

        mock_servicer.add_user_profile(data)

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
        """Test retrieving a user profile with Unicode characters in names.

        Verifies:
        - Unicode characters are handled correctly
        """
        data = {
            "user_id": USER_ID,
            "organisation_id": ORGANISATION_ID,
            "email": "test@example.com",
            "first_name": "José",
            "last_name": "François-müller",
            "subscription": {},
            "credits": [],
            "metadata": {},
        }

        mock_servicer.add_user_profile(data)

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
        """Test retrieving user profiles with various locale settings.

        Verifies:
        - Different locale formats are handled correctly
        """
        locales = ["en_US", "fr_FR", "ja_JP", "pt_BR", "zh_CN"]

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        for i, locale in enumerate(locales):
            user_id = f"users:user_{i}"
            data = {
                "user_id": user_id,
                "organisation_id": ORGANISATION_ID,
                "email": f"user{i}@example.com",
                "locale": locale,
                "subscription": {},
                "credits": [],
                "metadata": {},
            }

            mock_servicer.add_user_profile(data)

            client = GrpcUserProfile(
                mission_id=user_id,
                setup_id="setups:test_setup",
                setup_version_id="setup_versions:test_version",
                client_config=dummy_client_config,
            )
            client.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)

            future = thread_pool.submit(client.get_user_profile)
            _, request, rpc = test_channel.take_unary_unary(method_desc)

            context = FakeContext()
            response = mock_servicer.GetUserProfile(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")

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
        """Test retrieving a user profile with zero credits.

        Verifies:
        - Zero credits are handled correctly
        """
        data = {
            "user_id": USER_ID,
            "organisation_id": ORGANISATION_ID,
            "email": "test@example.com",
            "subscription": {},
            "credits": [
                {
                    "source": "subscription",
                    "total": 0,
                    "remaining": 0,
                    "timestamp": "2024-12-15T10:30:00Z",
                },
            ],
            "metadata": {},
        }

        mock_servicer.add_user_profile(data)

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
        credits = result["credits"]
        assert len(credits) == 1
        # Protobuf int64 fields are converted to strings in JSON, double fields stay as float
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
        """Test retrieving a user profile with expired subscription.

        Verifies:
        - Expired subscription status is handled correctly
        """
        data = {
            "user_id": USER_ID,
            "organisation_id": ORGANISATION_ID,
            "email": "test@example.com",
            "subscription": {
                "tier": "premium",
                "status": "expired",
                "start": "2023-01-01T00:00:00Z",
                "end": "2024-01-01T00:00:00Z",
            },
            "credits": [],
            "metadata": {},
        }

        mock_servicer.add_user_profile(data)

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
        """Test that multiple user profiles are independent.

        Verifies:
        - Different users have separate profiles
        - Profiles don't interfere with each other
        """
        user1_id = "users:user_1"
        user2_id = "users:user_2"

        data1 = {
            "user_id": user1_id,
            "organisation_id": ORGANISATION_ID,
            "email": "user1@example.com",
            "first_name": "User",
            "last_name": "One",
            "subscription": {},
            "credits": [{"source": "subscription", "total": 100, "remaining": 100}],
            "metadata": {},
        }

        data2 = {
            "user_id": user2_id,
            "organisation_id": ORGANISATION_ID,
            "email": "user2@example.com",
            "first_name": "User",
            "last_name": "Two",
            "subscription": {},
            "credits": [{"source": "subscription", "total": 200, "remaining": 200}],
            "metadata": {},
        }

        mock_servicer.add_user_profile(data1)
        mock_servicer.add_user_profile(data2)

        method_desc = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
            "GetUserProfile"
        ]

        # Get user 1 profile
        client1 = GrpcUserProfile(
            mission_id=user1_id,
            setup_id="setups:test_setup",
            setup_version_id="setup_versions:test_version",
            client_config=dummy_client_config,
        )
        client1.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)

        future1 = thread_pool.submit(client1.get_user_profile)
        _, request1, rpc1 = test_channel.take_unary_unary(method_desc)
        context1 = FakeContext()
        response1 = mock_servicer.GetUserProfile(request1, context1)
        rpc1.send_initial_metadata(())
        rpc1.terminate(response1, (), grpc.StatusCode.OK, "")
        result1 = future1.result(timeout=5.0)

        # Get user 2 profile
        client2 = GrpcUserProfile(
            mission_id=user2_id,
            setup_id="setups:test_setup",
            setup_version_id="setup_versions:test_version",
            client_config=dummy_client_config,
        )
        client2.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)

        future2 = thread_pool.submit(client2.get_user_profile)
        _, request2, rpc2 = test_channel.take_unary_unary(method_desc)
        context2 = FakeContext()
        response2 = mock_servicer.GetUserProfile(request2, context2)
        rpc2.send_initial_metadata(())
        rpc2.terminate(response2, (), grpc.StatusCode.OK, "")
        result2 = future2.result(timeout=5.0)

        # Verify independence
        assert result1["user_id"] == user1_id
        assert result2["user_id"] == user2_id
        assert result1["email"] == "user1@example.com"
        assert result2["email"] == "user2@example.com"
        # Protobuf int64 fields are converted to strings in JSON
        assert result1["credits"][0]["total"] == "100"
        assert result2["credits"][0]["total"] == "200"


# ============================================================================
# Regression Tests
# ============================================================================
# This section contains tests for previously identified bugs and edge cases
# that were fixed. Each test should document the issue/PR that it addresses.
#
# Format:
# @pytest.mark.grpc
# @pytest.mark.integration
# @pytest.mark.regression
# def test_regression_issue_123(...):
#     """Test for regression of issue #123.
#
#     Issue: [Brief description of the bug]
#     Fixed in: PR #456 / commit abc123
#
#     Verifies: [What this test checks to prevent regression]
#     """
#
# Add regression tests below as bugs are discovered and fixed.
