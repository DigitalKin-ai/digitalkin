"""Unit tests for ModuleServicer gRPC service.

This module contains comprehensive tests for the ModuleServicer class, which handles
module lifecycle, monitoring, and schema introspection operations.
"""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import grpc
import pytest
from agentic_mesh_protocol.module.v1 import (
    information_pb2,
    lifecycle_pb2,
)
from agentic_mesh_protocol.setup.v1 import setup_pb2
from google.protobuf import json_format, struct_pb2

from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.grpc_servers.module_servicer import ModuleServicer
from digitalkin.modules._base_module import BaseModule
from tests.fixtures.grpc_fixtures import FakeContext


# Mock Module Class for testing
class MockModule(BaseModule):
    """Mock module class for testing purposes."""

    name = "test_module"
    description = "Test module for unit tests"

    @classmethod
    def discover(cls) -> None:
        """Mock discover method."""

    @classmethod
    def create_input_model(cls, input_data: dict[str, Any]) -> dict[str, Any]:
        """Mock input model creation."""
        return input_data

    @classmethod
    def create_output_model(cls, output_data: dict[str, Any]) -> dict[str, Any]:
        """Mock output model creation."""
        return output_data

    @classmethod
    async def create_setup_model(cls, setup_data: dict[str, Any], *, config_fields: bool = False) -> dict[str, Any]:  # noqa: ARG003
        """Mock setup model creation."""
        return setup_data

    @classmethod
    def create_config_setup_model(cls, config_setup_data: dict[str, Any]) -> dict[str, Any]:
        """Mock config setup model creation."""
        return config_setup_data

    @classmethod
    async def get_input_format(cls, *, llm_format: bool = False) -> str:  # noqa: ARG003
        """Mock input format schema."""
        return '{"type": "object", "properties": {"message": {"type": "string"}}}'

    @classmethod
    async def get_output_format(cls, *, llm_format: bool = False) -> str:  # noqa: ARG003
        """Mock output format schema."""
        return '{"type": "object", "properties": {"result": {"type": "string"}}}'

    @classmethod
    async def get_setup_format(cls, *, llm_format: bool = False) -> str:  # noqa: ARG003
        """Mock setup format schema."""
        return '{"type": "object", "properties": {"config": {"type": "string"}}}'

    @classmethod
    async def get_secret_format(cls, *, llm_format: bool = False) -> str:  # noqa: ARG003
        """Mock secret format schema."""
        return '{"type": "object", "properties": {"api_key": {"type": "string"}}}'

    @classmethod
    async def get_config_setup_format(cls, *, llm_format: bool = False) -> str:  # noqa: ARG003
        """Mock config setup format schema."""
        return '{"type": "object", "properties": {"setup_config": {"type": "string"}}}'


@pytest.fixture
def mock_job_manager():
    """Create a mock job manager for testing."""
    manager = AsyncMock(spec=BaseJobManager)
    manager.tasks = {}
    manager.create_config_setup_instance_job = AsyncMock(return_value="test-config-job-id")
    manager.generate_config_setup_module_response = AsyncMock(return_value={"updated": "config"})
    return manager


@pytest.fixture
def mock_setup_strategy():
    """Create a mock setup strategy."""
    setup_mock = Mock()
    setup_data = Mock()
    setup_data.current_setup_version.content = {"test": "setup"}
    setup_data.current_setup_version.setup_id = "setup-123"
    setup_data.current_setup_version.id = "version-123"
    setup_mock.get_setup = AsyncMock(return_value=setup_data)
    return setup_mock


@pytest.fixture
def module_servicer(mock_job_manager, mock_setup_strategy):
    """Create a ModuleServicer instance with mocked dependencies."""
    # Create instance without calling __init__
    servicer = ModuleServicer.__new__(ModuleServicer)
    servicer.module_class = MockModule
    servicer.job_manager = mock_job_manager
    servicer.setup = mock_setup_strategy
    servicer._setup_cache = {}
    servicer._setup_cache_max = 100
    servicer._setup_inflight: dict[str, asyncio.Future] = {}
    servicer._completion_timeout = 300.0
    servicer._registry_cache = None
    servicer._tool_cache_by_setup = {}
    servicer._communication_cache = None

    return servicer


@pytest.fixture
def fake_context():
    """Create a fake gRPC context for testing."""
    return FakeContext()


class TestGetModuleInput:
    """Tests for GetModuleInput endpoint."""

    @pytest.mark.asyncio
    async def test_get_module_input_success(self, module_servicer, fake_context):
        """Test successful retrieval of module input schema."""
        request = information_pb2.GetModuleInputRequest(llm_format=False)

        response = await module_servicer.GetModuleInput(request, fake_context)

        assert response.success is True
        assert response.input_schema is not None

    @pytest.mark.asyncio
    async def test_get_module_input_llm_format(self, module_servicer, fake_context):
        """Test retrieval of module input schema in LLM format."""
        request = information_pb2.GetModuleInputRequest(llm_format=True)

        response = await module_servicer.GetModuleInput(request, fake_context)

        assert response.success is True
        assert response.input_schema is not None

    @pytest.mark.asyncio
    async def test_get_module_input_not_implemented(self, module_servicer, fake_context):
        """Test get module input when format is not implemented."""
        with patch.object(MockModule, "get_input_format", side_effect=NotImplementedError("Not implemented")):
            request = information_pb2.GetModuleInputRequest(llm_format=False)

            await module_servicer.GetModuleInput(request, fake_context)

            assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED
            assert "Not implemented" in fake_context.get_details()


class TestGetModuleOutput:
    """Tests for GetModuleOutput endpoint."""

    @pytest.mark.asyncio
    async def test_get_module_output_success(self, module_servicer, fake_context):
        """Test successful retrieval of module output schema."""
        request = information_pb2.GetModuleOutputRequest(llm_format=False)

        response = await module_servicer.GetModuleOutput(request, fake_context)

        assert response.success is True
        assert response.output_schema is not None

    @pytest.mark.asyncio
    async def test_get_module_output_llm_format(self, module_servicer, fake_context):
        """Test retrieval of module output schema in LLM format."""
        request = information_pb2.GetModuleOutputRequest(llm_format=True)

        response = await module_servicer.GetModuleOutput(request, fake_context)

        assert response.success is True
        assert response.output_schema is not None

    @pytest.mark.asyncio
    async def test_get_module_output_not_implemented(self, module_servicer, fake_context):
        """Test get module output when format is not implemented."""
        with patch.object(MockModule, "get_output_format", side_effect=NotImplementedError("Not implemented")):
            request = information_pb2.GetModuleOutputRequest(llm_format=False)

            await module_servicer.GetModuleOutput(request, fake_context)

            assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED


class TestGetModuleSetup:
    """Tests for GetModuleSetup endpoint."""

    @pytest.mark.asyncio
    async def test_get_module_setup_success(self, module_servicer, fake_context):
        """Test successful retrieval of module setup schema."""
        request = information_pb2.GetModuleSetupRequest(llm_format=False)

        response = await module_servicer.GetModuleSetup(request, fake_context)

        assert response.success is True
        assert response.setup_schema is not None

    @pytest.mark.asyncio
    async def test_get_module_setup_llm_format(self, module_servicer, fake_context):
        """Test retrieval of module setup schema in LLM format."""
        request = information_pb2.GetModuleSetupRequest(llm_format=True)

        response = await module_servicer.GetModuleSetup(request, fake_context)

        assert response.success is True
        assert response.setup_schema is not None

    @pytest.mark.asyncio
    async def test_get_module_setup_not_implemented(self, module_servicer, fake_context):
        """Test get module setup when format is not implemented."""
        with patch.object(MockModule, "get_setup_format", side_effect=NotImplementedError("Not implemented")):
            request = information_pb2.GetModuleSetupRequest(llm_format=False)

            await module_servicer.GetModuleSetup(request, fake_context)

            assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED


class TestGetModuleSecret:
    """Tests for GetModuleSecret endpoint."""

    @pytest.mark.asyncio
    async def test_get_module_secret_success(self, module_servicer, fake_context):
        """Test successful retrieval of module secret schema."""
        request = information_pb2.GetModuleSecretRequest(llm_format=False)

        response = await module_servicer.GetModuleSecret(request, fake_context)

        assert response.success is True
        assert response.secret_schema is not None

    @pytest.mark.asyncio
    async def test_get_module_secret_llm_format(self, module_servicer, fake_context):
        """Test retrieval of module secret schema in LLM format."""
        request = information_pb2.GetModuleSecretRequest(llm_format=True)

        response = await module_servicer.GetModuleSecret(request, fake_context)

        assert response.success is True
        assert response.secret_schema is not None

    @pytest.mark.asyncio
    async def test_get_module_secret_not_implemented(self, module_servicer, fake_context):
        """Test get module secret when format is not implemented."""
        with patch.object(MockModule, "get_secret_format", side_effect=NotImplementedError("Not implemented")):
            request = information_pb2.GetModuleSecretRequest(llm_format=False)

            await module_servicer.GetModuleSecret(request, fake_context)

            assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED


class TestGetConfigSetupModule:
    """Tests for GetConfigSetupModule endpoint."""

    @pytest.mark.asyncio
    async def test_get_config_setup_module_success(self, module_servicer, fake_context):
        """Test successful retrieval of config setup schema."""
        request = information_pb2.GetConfigSetupModuleRequest(llm_format=False)

        response = await module_servicer.GetConfigSetupModule(request, fake_context)

        assert response.success is True
        assert response.config_setup_schema is not None

    @pytest.mark.asyncio
    async def test_get_config_setup_module_llm_format(self, module_servicer, fake_context):
        """Test retrieval of config setup schema in LLM format."""
        request = information_pb2.GetConfigSetupModuleRequest(llm_format=True)

        response = await module_servicer.GetConfigSetupModule(request, fake_context)

        assert response.success is True
        assert response.config_setup_schema is not None

    @pytest.mark.asyncio
    async def test_get_config_setup_module_not_implemented(self, module_servicer, fake_context):
        """Test get config setup when format is not implemented."""
        with patch.object(MockModule, "get_config_setup_format", side_effect=NotImplementedError("Not implemented")):
            request = information_pb2.GetConfigSetupModuleRequest(llm_format=False)

            await module_servicer.GetConfigSetupModule(request, fake_context)

            assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED


class TestConfigSetupModule:
    """Tests for ConfigSetupModule endpoint."""

    @pytest.mark.asyncio
    async def test_config_setup_module_success(self, module_servicer, fake_context, mock_job_manager):
        """Test successful module setup configuration."""
        # Create setup version using the correct import
        setup_version = setup_pb2.SetupVersion(
            id="version-123",
            setup_id="setup-123",
            content=json_format.ParseDict({"existing": "config"}, struct_pb2.Struct()),
        )

        request = lifecycle_pb2.ConfigSetupModuleRequest(
            mission_id="mission-456",
            setup_version=setup_version,
            content=json_format.ParseDict({"new": "config"}, struct_pb2.Struct()),
        )

        response = await module_servicer.ConfigSetupModule(request, fake_context)

        assert response.success is True
        assert response.setup_version is not None
        mock_job_manager.create_config_setup_instance_job.assert_called_once()
        mock_job_manager.generate_config_setup_module_response.assert_called_once_with("test-config-job-id")

    @pytest.mark.asyncio
    async def test_config_setup_module_job_creation_fails(self, module_servicer, fake_context, mock_job_manager):
        """Test config setup when job creation fails."""
        mock_job_manager.create_config_setup_instance_job = AsyncMock(return_value=None)

        setup_version = setup_pb2.SetupVersion(
            id="version-123",
            setup_id="setup-123",
            content=json_format.ParseDict({"existing": "config"}, struct_pb2.Struct()),
        )

        request = lifecycle_pb2.ConfigSetupModuleRequest(
            mission_id="mission-456",
            setup_version=setup_version,
            content=json_format.ParseDict({"new": "config"}, struct_pb2.Struct()),
        )

        response = await module_servicer.ConfigSetupModule(request, fake_context)

        assert response.success is False
        assert fake_context.get_code() == grpc.StatusCode.NOT_FOUND
        assert "Failed to create module instance" in fake_context.get_details()

    @pytest.mark.asyncio
    async def test_config_setup_module_no_setup_data(self, module_servicer, fake_context):
        """Test config setup when setup data creation fails."""
        with patch.object(MockModule, "create_setup_model", return_value=None):
            setup_version = setup_pb2.SetupVersion(
                id="version-123",
                setup_id="setup-123",
                content=json_format.ParseDict({"existing": "config"}, struct_pb2.Struct()),
            )

            request = lifecycle_pb2.ConfigSetupModuleRequest(
                mission_id="mission-456",
                setup_version=setup_version,
                content=json_format.ParseDict({"new": "config"}, struct_pb2.Struct()),
            )

            with pytest.raises(Exception, match="No setup data returned"):
                await module_servicer.ConfigSetupModule(request, fake_context)

    @pytest.mark.asyncio
    async def test_config_setup_module_no_config_setup_data(self, module_servicer, fake_context):
        """Test config setup when config setup data creation fails."""
        with patch.object(MockModule, "create_config_setup_model", return_value=None):
            setup_version = setup_pb2.SetupVersion(
                id="version-123",
                setup_id="setup-123",
                content=json_format.ParseDict({"existing": "config"}, struct_pb2.Struct()),
            )

            request = lifecycle_pb2.ConfigSetupModuleRequest(
                mission_id="mission-456",
                setup_version=setup_version,
                content=json_format.ParseDict({"new": "config"}, struct_pb2.Struct()),
            )

            with pytest.raises(Exception, match="No config setup data returned"):
                await module_servicer.ConfigSetupModule(request, fake_context)
