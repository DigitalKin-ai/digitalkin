"""TaskSession mocks for testing.

Provides factory function for creating mock TaskSession objects.
"""

from typing import Any
from unittest.mock import AsyncMock, Mock

from digitalkin.core.task_manager.task_session import TaskSession


def create_mock_task_session(**overrides: Any) -> Mock:
    """Factory for creating mock TaskSession objects.

    Creates a Mock object with ``spec=TaskSession`` and pre-configured
    attributes and AsyncMock methods.

    Args:
        **overrides: Override specific attributes or methods.

    Returns:
        Mock ``TaskSession`` with sensible defaults.
    """
    session = Mock(spec=TaskSession)

    # Basic attributes
    session.mission_id = "missions:mock"
    session.status = "pending"
    session.setup_id = "setup:test"
    session.setup_version_id = "setup_version:test"
    session.started_at = None
    session.completed_at = None
    session.error = None

    # Side-channel fields read by TaskExecutor / _handle_*.
    session.pending_signal_action = ""
    session.last_signal_published_ns = 0

    # Signal service (sender-only).
    session.signal_service = Mock()
    session.signal_service.send_signal = AsyncMock()
    session.signal_service.close = AsyncMock()

    # State management methods
    session.update_status = Mock()
    session.set_error = Mock()

    # Apply any overrides
    for key, value in overrides.items():
        setattr(session, key, value)

    return session
