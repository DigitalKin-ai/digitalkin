"""TaskSession mocks for testing.

Provides factory function for creating mock TaskSession objects.

Usage:
    # Basic mock
    session = create_mock_task_session()

    # Custom attributes
    session = create_mock_task_session(
        mission_id="missions:custom",
        status=TaskStatus.RUNNING,
    )

    # Custom async methods
    session = create_mock_task_session(
        listen_signals=AsyncMock(side_effect=CustomError())
    )
"""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock

from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import TaskStatus


def create_mock_task_session(**overrides: Any) -> Mock:
    """Factory for creating mock TaskSession objects.

    Creates a Mock object with spec=TaskSession and pre-configured
    attributes and AsyncMock methods.

    Args:
        **overrides: Override specific attributes or methods.
            Example: mission_id="missions:custom", status=TaskStatus.RUNNING

    Returns:
        Mock TaskSession with sensible defaults

    Example:
        # Basic usage
        session = create_mock_task_session()
        assert session.status == TaskStatus.PENDING

        # Custom status
        session = create_mock_task_session(status=TaskStatus.RUNNING)
        assert session.status == TaskStatus.RUNNING

        # Custom async behavior
        async def custom_listen():
            await asyncio.sleep(1)
            raise KeyboardInterrupt()

        session = create_mock_task_session(
            listen_signals=AsyncMock(side_effect=custom_listen)
        )
    """
    session = Mock(spec=TaskSession)

    # Basic attributes
    session.mission_id = "missions:mock"
    session.status = TaskStatus.PENDING
    session.setup_id = "setup:test"
    session.setup_version_id = "setup_version:test"
    session.started_at = None
    session.completed_at = None
    session.error = None

    # Database connection mock
    session.db = Mock()
    session.db.close = AsyncMock()

    # Async methods - default to CancelledError for supervisor pattern tests
    session.listen_signals = AsyncMock(side_effect=asyncio.CancelledError())
    session.generate_heartbeats = AsyncMock(side_effect=asyncio.CancelledError())

    # State management methods
    session.update_status = Mock()
    session.set_error = Mock()

    # Apply any overrides
    for key, value in overrides.items():
        setattr(session, key, value)

    return session
