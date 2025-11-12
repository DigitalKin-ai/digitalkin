"""Centralized mock implementations for DigitalKin tests.

This package provides reusable mock objects for testing:
- modules.py: MockModule implementations (ConfigurableMockModule, SimpleMockModule)
- models.py: Pydantic test models (Input, Output, Setup, Secret)
- database.py: SurrealDB connection mocks
- sessions.py: TaskSession mocks

Import these in your tests instead of creating duplicate mocks.
"""

from tests.mocks.database import StatefulMockSurrealConnection, create_mock_surreal_connection
from tests.mocks.models import MockInputModel, MockOutputModel, MockSecretModel, MockSetupModel
from tests.mocks.modules import ConfigurableMockModule, SimpleMockModule
from tests.mocks.sessions import create_mock_task_session

__all__ = [
    # Modules
    "ConfigurableMockModule",
    "SimpleMockModule",
    # Models
    "MockInputModel",
    "MockOutputModel",
    "MockSetupModel",
    "MockSecretModel",
    # Database
    "create_mock_surreal_connection",
    "StatefulMockSurrealConnection",
    # Sessions
    "create_mock_task_session",
]
