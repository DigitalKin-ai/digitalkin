"""Standardized Pydantic models for testing.

These models provide consistent test data structures across all test files.
Use these instead of defining inline models in each test file.
"""

from pydantic import BaseModel, Field

from digitalkin.models.module.module_types import DataModel, DataTrigger


class MockInputTrigger(DataTrigger):
    """Mock input trigger for testing.

    Uses protocol="mock" to route to mock trigger handlers.
    """

    protocol: str = "mock"
    data: str = ""
    metadata: dict[str, str] = Field(default_factory=dict)


class MockInputModel(DataModel[MockInputTrigger]):
    """Mock input model for testing.

    Wraps MockInputTrigger in DataModel for module input.
    """


class MockOutputModel(BaseModel):
    """Mock output model for testing.

    Represents typical module output with result and status.
    """

    result: str = "test_result"
    status: str = "success"
    data: dict[str, str] = Field(default_factory=dict)


class MockSetupModel(BaseModel):
    """Mock setup model for testing.

    Represents module configuration with both runtime and config-time fields.
    """

    config: str = "default_config"
    timeout: int = Field(default=30, json_schema_extra={"config": True})
    enabled: bool = Field(default=True, json_schema_extra={"config": True})
    internal_state: str = Field(default="", json_schema_extra={"hidden": True})


class MockSecretModel(BaseModel):
    """Mock secret model for testing.

    Represents sensitive configuration like API keys.
    """

    api_key: str = "mock_api_key"
    secret_token: str = "mock_secret_token"
