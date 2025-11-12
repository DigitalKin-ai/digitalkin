"""Global pytest fixtures for DigitalKin tests.

These fixtures provide reusable test dependencies across all test files.
They are automatically discovered when pytest_plugins is configured in conftest.py.

Usage in tests:
    def test_something(mock_surreal_connection):
        # Use the fixture
        await mock_surreal_connection.create("tasks", {"task_id": "test"})
"""

import pytest
import pytest_asyncio
from unittest.mock import Mock

from digitalkin.modules._base_module import BaseModule
from tests.mocks import (
    ConfigurableMockModule,
    SimpleMockModule,
    create_mock_surreal_connection,
    create_mock_task_session,
)


@pytest_asyncio.fixture
async def mock_surreal_connection() -> Mock:
    """Standard mock SurrealDB connection.

    Provides a Mock object with spec=SurrealDBConnection and pre-configured
    AsyncMock methods for all common operations.

    Returns:
        Mock SurrealDB connection with sensible defaults

    Example:
        async def test_create(mock_surreal_connection):
            result = await mock_surreal_connection.create("tasks", {})
            assert result["id"] == "mock_record_id"
    """
    return create_mock_surreal_connection()


@pytest_asyncio.fixture
async def mock_task_session() -> Mock:
    """Standard mock TaskSession.

    Provides a Mock object with spec=TaskSession and pre-configured
    attributes and AsyncMock methods.

    Returns:
        Mock TaskSession with sensible defaults

    Example:
        async def test_session(mock_task_session):
            assert mock_task_session.status == TaskStatus.PENDING
            await mock_task_session.listen_signals()
    """
    return create_mock_task_session()


@pytest.fixture
def simple_mock_module() -> type[BaseModule]:
    """SimpleMockModule class for basic testing.

    Returns the SimpleMockModule class itself (not an instance).
    Tests can instantiate it with their own parameters.

    Returns:
        SimpleMockModule class

    Example:
        def test_module(simple_mock_module):
            module = simple_mock_module(
                job_id="test_job",
                mission_id="missions:test",
                setup_id="setup",
                setup_version_id="v1",
            )
            assert module.name == "SimpleMockModule"
    """
    return SimpleMockModule


@pytest.fixture
def configurable_mock_module() -> type[BaseModule]:
    """ConfigurableMockModule class for advanced testing.

    Returns the ConfigurableMockModule class itself (not an instance).
    Tests can instantiate it with custom configuration.

    Returns:
        ConfigurableMockModule class

    Example:
        def test_module_error(configurable_mock_module):
            module = configurable_mock_module(
                job_id="test_job",
                mission_id="missions:test",
                setup_id="setup",
                setup_version_id="v1",
                initialize_error=RuntimeError("Init failed"),
            )
            # Test error handling
    """
    return ConfigurableMockModule


@pytest.fixture
def module_init_params() -> dict[str, str]:
    """Standard module initialization parameters.

    Provides consistent parameter values for module instantiation
    across all tests.

    Returns:
        Dict with job_id, mission_id, setup_id, setup_version_id

    Example:
        def test_module(simple_mock_module, module_init_params):
            module = simple_mock_module(**module_init_params)
            assert module.job_id == "test_job_123"
    """
    return {
        "job_id": "test_job_123",
        "mission_id": "missions:test_mission",
        "setup_id": "setup:test_setup",
        "setup_version_id": "v1.0.0",
    }
