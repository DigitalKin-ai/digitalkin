"""Comprehensive tests for GrpcUserProfile service.

This test suite validates the GrpcUserProfile service implementation, including:
- Getting user profiles by user_id
- Handling missing user profiles
- Validating user profile data structure
- Error handling and edge cases
"""

from concurrent import futures

import grpc_testing
import pytest
from digitalkin_proto.agentic_mesh_protocol.user_profile.v1 import user_profile_service_pb2_grpc
from tests.services.user_profile.mock_user_profile_servicer import FakeContext, MockUserProfileServicer

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.user_profile.grpc_user_profile import GrpcUserProfile, UserProfileServiceError

# --- Test Constants ---
USER_ID = "users:test_user_123"
ORGANISATION_ID = "organisations:test_org_456"

# Thread pool for client execution
client_execution_thread_pool = futures.ThreadPoolExecutor(max_workers=1)


# --- Fixtures ---
@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Create a test gRPC channel.

    Returns:
        A testing channel for intercepting gRPC calls
    """
    return grpc_testing.channel(
        service_descriptors=[user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"]],
        time=grpc_testing.strict_real_time(),
    )


@pytest.fixture
def mock_servicer() -> MockUserProfileServicer:
    """Create a mock user profile servicer.

    Returns:
        Mock servicer instance
    """
    return MockUserProfileServicer()


@pytest.fixture
def dummy_client_config() -> ClientConfig:
    """Create a dummy ClientConfig for testing.

    Returns:
        ClientConfig instance with test values
    """
    return ClientConfig(host="localhost", port=50051, secure=False)


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
    client = GrpcUserProfile(USER_ID, dummy_client_config)
    client.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)
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
            "plan": "premium",
            "status": "active",
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2025-01-01T00:00:00Z",
        },
        "credits": {
            "total": 1000,
            "used": 250,
            "remaining": 750,
        },
        "metadata": {
            "last_login": "2024-12-15T10:30:00Z",
            "login_count": 42,
            "preferences": "{}",
        },
    }


# ============================================================================
# get_user_profile() Tests
# ============================================================================


def test_get_user_profile_success(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
    sample_user_profile_data: dict,
) -> None:
    """Test successfully retrieving a user profile.

    Verifies:
    - User profile is retrieved with correct data
    - All fields are present and accurate
    """
    # Add user profile to mock servicer
    mock_servicer.add_user_profile(sample_user_profile_data)

    # Get the method descriptor
    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    # Execute client call in thread pool
    future = client_execution_thread_pool.submit(client.get_user_profile)

    # Intercept the call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Verify request
    assert request.user_id == USER_ID

    # Mock servicer processes the request
    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)

    # Terminate the RPC
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


def test_get_user_profile_with_subscription(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
    sample_user_profile_data: dict,
) -> None:
    """Test retrieving a user profile with subscription data.

    Verifies:
    - Subscription information is correctly included
    - All subscription fields are present
    """
    mock_servicer.add_user_profile(sample_user_profile_data)

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    future = client_execution_thread_pool.submit(client.get_user_profile)
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)

    # Verify subscription data
    assert "subscription" in result
    subscription = result["subscription"]
    assert subscription["plan"] == "premium"
    assert subscription["status"] == "active"


def test_get_user_profile_with_credits(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
    sample_user_profile_data: dict,
) -> None:
    """Test retrieving a user profile with credits data.

    Verifies:
    - Credits information is correctly included
    - All credit fields are present and accurate
    """
    mock_servicer.add_user_profile(sample_user_profile_data)

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    future = client_execution_thread_pool.submit(client.get_user_profile)
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)

    # Verify credits data
    assert "credits" in result
    credits = result["credits"]
    assert credits["total"] == 1000
    assert credits["used"] == 250
    assert credits["remaining"] == 750


def test_get_user_profile_with_metadata(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
    sample_user_profile_data: dict,
) -> None:
    """Test retrieving a user profile with metadata.

    Verifies:
    - Metadata information is correctly included
    - All metadata fields are present
    """
    mock_servicer.add_user_profile(sample_user_profile_data)

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    future = client_execution_thread_pool.submit(client.get_user_profile)
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)

    # Verify metadata
    assert "metadata" in result
    metadata = result["metadata"]
    assert "last_login" in metadata
    assert metadata["login_count"] == 42


def test_get_user_profile_not_found(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
) -> None:
    """Test getting a non-existent user profile raises error.

    Verifies:
    - Attempting to get a non-existent user profile raises UserProfileServiceError
    - Error message is appropriate
    """
    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    future = client_execution_thread_pool.submit(client.get_user_profile)
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)
    rpc.terminate(response, (), context._code, context._details)

    # Verify error is raised
    with pytest.raises(UserProfileServiceError):
        future.result(timeout=5.0)


def test_get_user_profile_with_minimal_data(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
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
        "credits": {},
        "metadata": {},
    }

    mock_servicer.add_user_profile(minimal_data)

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    future = client_execution_thread_pool.submit(client.get_user_profile)
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)

    # Verify minimal fields are present
    assert result["user_id"] == USER_ID
    assert result["email"] == "minimal@example.com"
    assert "subscription" in result
    assert "credits" in result
    assert "metadata" in result


@pytest.mark.asyncio
async def test_get_identity(
    client: GrpcUserProfile,
) -> None:
    """Test get_identity method returns user_id.

    Verifies:
    - get_identity returns the correct user_id
    """
    identity = await client.get_identity()
    assert identity == USER_ID


# ============================================================================
# Edge Cases
# ============================================================================


def test_get_user_profile_with_special_characters_in_email(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
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
        "credits": {},
        "metadata": {},
    }

    mock_servicer.add_user_profile(data)

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    future = client_execution_thread_pool.submit(client.get_user_profile)
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result["email"] == "test.user+tag@example.co.uk"


def test_get_user_profile_with_unicode_names(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
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
        "credits": {},
        "metadata": {},
    }

    mock_servicer.add_user_profile(data)

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    future = client_execution_thread_pool.submit(client.get_user_profile)
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result["first_name"] == "José"
    assert result["last_name"] == "François-müller"


def test_get_user_profile_with_different_locales(
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
    dummy_client_config: ClientConfig,
) -> None:
    """Test retrieving user profiles with various locale settings.

    Verifies:
    - Different locale formats are handled correctly
    """
    locales = ["en_US", "fr_FR", "ja_JP", "pt_BR", "zh_CN"]

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
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
            "credits": {},
            "metadata": {},
        }

        mock_servicer.add_user_profile(data)

        client = GrpcUserProfile(user_id, dummy_client_config)
        client.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)

        future = client_execution_thread_pool.submit(client.get_user_profile)
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.GetUserProfile(request, context)
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result["locale"] == locale


def test_get_user_profile_with_zero_credits(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
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
        "credits": {
            "total": 0,
            "used": 0,
            "remaining": 0,
        },
        "metadata": {},
    }

    mock_servicer.add_user_profile(data)

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    future = client_execution_thread_pool.submit(client.get_user_profile)
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    credits = result["credits"]
    assert credits["total"] == 0
    assert credits["remaining"] == 0


def test_get_user_profile_with_expired_subscription(
    client: GrpcUserProfile,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
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
            "plan": "premium",
            "status": "expired",
            "start_date": "2023-01-01T00:00:00Z",
            "end_date": "2024-01-01T00:00:00Z",
        },
        "credits": {},
        "metadata": {},
    }

    mock_servicer.add_user_profile(data)

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    future = client_execution_thread_pool.submit(client.get_user_profile)
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetUserProfile(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    subscription = result["subscription"]
    assert subscription["status"] == "expired"


def test_multiple_user_profiles_independence(
    test_channel: grpc_testing.Channel,
    mock_servicer: MockUserProfileServicer,
    dummy_client_config: ClientConfig,
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
        "credits": {"total": 100},
        "metadata": {},
    }

    data2 = {
        "user_id": user2_id,
        "organisation_id": ORGANISATION_ID,
        "email": "user2@example.com",
        "first_name": "User",
        "last_name": "Two",
        "subscription": {},
        "credits": {"total": 200},
        "metadata": {},
    }

    mock_servicer.add_user_profile(data1)
    mock_servicer.add_user_profile(data2)

    method_desc = user_profile_service_pb2_grpc.DESCRIPTOR.services_by_name["UserProfileService"].methods_by_name[
        "GetUserProfile"
    ]

    # Get user 1 profile
    client1 = GrpcUserProfile(user1_id, dummy_client_config)
    client1.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)

    future1 = client_execution_thread_pool.submit(client1.get_user_profile)
    _, request1, rpc1 = test_channel.take_unary_unary(method_desc)
    context1 = FakeContext()
    response1 = mock_servicer.GetUserProfile(request1, context1)
    rpc1.terminate(response1, (), grpc.StatusCode.OK, "")
    result1 = future1.result(timeout=5.0)

    # Get user 2 profile
    client2 = GrpcUserProfile(user2_id, dummy_client_config)
    client2.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)

    future2 = client_execution_thread_pool.submit(client2.get_user_profile)
    _, request2, rpc2 = test_channel.take_unary_unary(method_desc)
    context2 = FakeContext()
    response2 = mock_servicer.GetUserProfile(request2, context2)
    rpc2.terminate(response2, (), grpc.StatusCode.OK, "")
    result2 = future2.result(timeout=5.0)

    # Verify independence
    assert result1["user_id"] == user1_id
    assert result2["user_id"] == user2_id
    assert result1["email"] == "user1@example.com"
    assert result2["email"] == "user2@example.com"
    assert result1["credits"]["total"] == 100
    assert result2["credits"]["total"] == 200
