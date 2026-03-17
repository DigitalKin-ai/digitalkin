"""Global pytest fixtures for DigitalKin tests.

These fixtures provide reusable test dependencies across all test files.
They are automatically discovered when pytest_plugins is configured in conftest.py.
"""

from unittest.mock import Mock

import pytest
import pytest_asyncio

from digitalkin.modules._base_module import BaseModule
from tests.mocks import (
    ConfigurableMockModule,
    SimpleMockModule,
    create_mock_task_session,
)


@pytest_asyncio.fixture
async def mock_task_session() -> Mock:
    """Standard mock TaskSession.

    Returns:
        Mock TaskSession with sensible defaults.
    """
    return create_mock_task_session()


@pytest.fixture
def simple_mock_module() -> type[BaseModule]:
    """SimpleMockModule class for basic testing.

    Returns:
        SimpleMockModule class.
    """
    return SimpleMockModule


@pytest.fixture
def configurable_mock_module() -> type[BaseModule]:
    """ConfigurableMockModule class for advanced testing.

    Returns:
        ConfigurableMockModule class.
    """
    return ConfigurableMockModule


@pytest.fixture
def module_init_params() -> dict[str, str]:
    """Standard module initialization parameters.

    Returns:
        Dict with job_id, mission_id, setup_id, setup_version_id.
    """
    return {
        "job_id": "test_job_123",
        "mission_id": "missions:test_mission",
        "setup_id": "setup:test_setup",
        "setup_version_id": "v1.0.0",
    }
