"""Unit tests for ModuleServicer gRPC service.

This module contains comprehensive tests for the ModuleServicer class, which handles
module lifecycle, monitoring, and schema introspection operations.
"""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock, patch

import grpc
import pytest
from agentic_mesh_protocol.module.v1 import module_dto_pb2
from agentic_mesh_protocol.setup.v1 import setup_messages_pb2
from google.protobuf import json_format, struct_pb2
from pydantic import BaseModel, ValidationError

from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.grpc_servers.module_servicer import ModuleServicer
from digitalkin.models.module.module import ModuleCodeModel
from digitalkin.models.settings.module import get_module_settings
from digitalkin.modules._base_module import BaseModule
from tests.fixtures.grpc_fixtures import FakeContext

if TYPE_CHECKING:
    import asyncio

MODULE_ID = "modules:test"


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
    async def get_select_input_format(cls) -> str:
        """Mock select input format schema."""
        return '{"type": "object", "properties": {"trigger": {"type": "string"}}}'

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

    @classmethod
    async def get_cost_format(cls, *, llm_format: bool = False) -> str:  # noqa: ARG003
        """Mock cost format schema."""
        return '{"type": "object", "properties": {"tokens": {"type": "number"}}}'


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
    setup_data.current_setup_version.setup_id = "setups:123"
    setup_data.current_setup_version.id = "setup_versions:123"
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
    servicer.user_profile = AsyncMock()
    servicer.user_profile.check_resource_access = AsyncMock(return_value=True)
    servicer._redis_client = AsyncMock()
    servicer._setup_cache = {}
    servicer._setup_inflight: dict[str, asyncio.Future] = {}
    servicer._registry_cache = None
    servicer._tool_cache_by_setup = {}
    servicer._communication_cache = None

    return servicer


@pytest.fixture
def fake_context():
    """Create a fake gRPC context for testing."""
    return FakeContext()


def config_setup_request(content: dict[str, Any] | None = None) -> module_dto_pb2.ConfigSetupModuleRequest:
    """Build a ConfigSetupModuleRequest for setup version ``setup_versions:123``."""
    setup_version = setup_messages_pb2.SetupVersion(
        id="setup_versions:123",
        setup_id="setups:123",
        version="v1",
        content=json_format.ParseDict({"existing": "config"} if content is None else content, struct_pb2.Struct()),
    )
    return module_dto_pb2.ConfigSetupModuleRequest(
        mission_id="missions:456",
        setup_version=setup_version,
        content=json_format.ParseDict({"new": "config"}, struct_pb2.Struct()),
    )


class TestGetModuleInput:
    """Tests for GetModuleInput endpoint."""

    @pytest.fixture(autouse=True)
    def module_id_env(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
        """Serve as ``MODULE_ID``, set the way a deployment sets it: ``DIGITALKIN_MODULE_ID``.

        Yields:
            Nothing; the settings cache is cleared on both sides so no other test sees the id.
        """
        monkeypatch.setenv("DIGITALKIN_MODULE_ID", MODULE_ID)
        get_module_settings.cache_clear()
        yield
        get_module_settings.cache_clear()

    @pytest.mark.validation
    async def test_get_module_input_other_module_id_is_not_found(self, module_servicer, fake_context):
        """A request addressed to another module aborts with NOT_FOUND before the input format is built."""
        request = module_dto_pb2.GetModuleInputRequest(module_id="modules:other")

        with (
            patch.object(MockModule, "get_input_format", AsyncMock(return_value="{}")) as get_format,
            pytest.raises(Exception, match="RPC aborted"),
        ):
            await module_servicer.GetModuleInput(request, fake_context)

        assert fake_context.get_code() == grpc.StatusCode.NOT_FOUND
        assert fake_context.get_details() == f"module modules:other is not served here (this module is {MODULE_ID})"
        get_format.assert_not_awaited()

    async def test_get_module_input_success(self, module_servicer, fake_context):
        """The input schema rides in ``result.input_schema``, identified by the module id."""
        request = module_dto_pb2.GetModuleInputRequest(module_id=MODULE_ID, llm_format=False)

        response = await module_servicer.GetModuleInput(request, fake_context)

        assert response.result.WhichOneof("outcome") == "input_schema"
        assert response.result.identifier == MODULE_ID
        assert "message" in json_format.MessageToDict(response.result.input_schema)["properties"]
        assert fake_context.get_code() == grpc.StatusCode.OK

    async def test_get_module_input_llm_format(self, module_servicer, fake_context):
        """The llm_format flag is forwarded to the module."""
        request = module_dto_pb2.GetModuleInputRequest(module_id=MODULE_ID, llm_format=True)

        with patch.object(MockModule, "get_input_format", AsyncMock(return_value="{}")) as get_format:
            response = await module_servicer.GetModuleInput(request, fake_context)

        get_format.assert_awaited_once_with(llm_format=True)
        assert response.result.WhichOneof("outcome") == "input_schema"

    async def test_get_module_input_not_implemented(self, module_servicer, fake_context):
        """A module without an input format aborts with UNIMPLEMENTED."""
        with patch.object(MockModule, "get_input_format", side_effect=NotImplementedError("Not implemented")):
            request = module_dto_pb2.GetModuleInputRequest(module_id=MODULE_ID, llm_format=False)

            with pytest.raises(Exception, match="RPC aborted"):
                await module_servicer.GetModuleInput(request, fake_context)

        assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED
        assert "Not implemented" in fake_context.get_details()

    async def test_get_module_input_failure_is_internal(self, module_servicer, fake_context):
        """An unexpected failure aborts with INTERNAL, not UNKNOWN."""
        with patch.object(MockModule, "get_input_format", side_effect=RuntimeError("boom")):
            request = module_dto_pb2.GetModuleInputRequest(module_id=MODULE_ID)

            with pytest.raises(Exception, match="RPC aborted"):
                await module_servicer.GetModuleInput(request, fake_context)

        assert fake_context.get_code() == grpc.StatusCode.INTERNAL
        assert "boom" in fake_context.get_details()


class TestGetModuleSelectInput:
    """Tests for GetModuleSelectInput endpoint."""

    async def test_get_module_select_input_success(self, module_servicer, fake_context):
        """The select input schema rides in ``result.select_input_schema``."""
        request = module_dto_pb2.GetModuleSelectInputRequest(module_id=MODULE_ID)

        response = await module_servicer.GetModuleSelectInput(request, fake_context)

        assert response.result.WhichOneof("outcome") == "select_input_schema"
        assert response.result.identifier == MODULE_ID
        assert "trigger" in json_format.MessageToDict(response.result.select_input_schema)["properties"]

    async def test_get_module_select_input_failure_is_internal(self, module_servicer, fake_context):
        """A failing select input format aborts with INTERNAL."""
        with patch.object(MockModule, "get_select_input_format", side_effect=RuntimeError("boom")):
            request = module_dto_pb2.GetModuleSelectInputRequest(module_id=MODULE_ID)

            with pytest.raises(Exception, match="RPC aborted"):
                await module_servicer.GetModuleSelectInput(request, fake_context)

        assert fake_context.get_code() == grpc.StatusCode.INTERNAL


class TestGetModuleOutput:
    """Tests for GetModuleOutput endpoint."""

    async def test_get_module_output_success(self, module_servicer, fake_context):
        """The output schema rides in ``result.output_schema``."""
        request = module_dto_pb2.GetModuleOutputRequest(module_id=MODULE_ID, llm_format=False)

        response = await module_servicer.GetModuleOutput(request, fake_context)

        assert response.result.WhichOneof("outcome") == "output_schema"
        assert response.result.identifier == MODULE_ID
        assert "result" in json_format.MessageToDict(response.result.output_schema)["properties"]

    async def test_get_module_output_not_implemented(self, module_servicer, fake_context):
        """A module without an output format aborts with UNIMPLEMENTED."""
        with patch.object(MockModule, "get_output_format", side_effect=NotImplementedError("Not implemented")):
            request = module_dto_pb2.GetModuleOutputRequest(module_id=MODULE_ID, llm_format=False)

            with pytest.raises(Exception, match="RPC aborted"):
                await module_servicer.GetModuleOutput(request, fake_context)

        assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED


class TestGetModuleSetup:
    """Tests for GetModuleSetup endpoint."""

    async def test_get_module_setup_success(self, module_servicer, fake_context):
        """The setup schema rides in ``result.setup_schema``."""
        request = module_dto_pb2.GetModuleSetupRequest(module_id=MODULE_ID, llm_format=False)

        response = await module_servicer.GetModuleSetup(request, fake_context)

        assert response.result.WhichOneof("outcome") == "setup_schema"
        assert response.result.identifier == MODULE_ID
        assert "config" in json_format.MessageToDict(response.result.setup_schema)["properties"]

    async def test_get_module_setup_not_implemented(self, module_servicer, fake_context):
        """A module without a setup format aborts with UNIMPLEMENTED."""
        with patch.object(MockModule, "get_setup_format", side_effect=NotImplementedError("Not implemented")):
            request = module_dto_pb2.GetModuleSetupRequest(module_id=MODULE_ID, llm_format=False)

            with pytest.raises(Exception, match="RPC aborted"):
                await module_servicer.GetModuleSetup(request, fake_context)

        assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED


class TestGetModuleSecret:
    """Tests for GetModuleSecret endpoint."""

    async def test_get_module_secret_success(self, module_servicer, fake_context):
        """The secret schema rides in ``result.secret_schema``."""
        request = module_dto_pb2.GetModuleSecretRequest(module_id=MODULE_ID, llm_format=False)

        response = await module_servicer.GetModuleSecret(request, fake_context)

        assert response.result.WhichOneof("outcome") == "secret_schema"
        assert response.result.identifier == MODULE_ID
        assert "api_key" in json_format.MessageToDict(response.result.secret_schema)["properties"]

    async def test_get_module_secret_not_implemented(self, module_servicer, fake_context):
        """A module without a secret format aborts with UNIMPLEMENTED."""
        with patch.object(MockModule, "get_secret_format", side_effect=NotImplementedError("Not implemented")):
            request = module_dto_pb2.GetModuleSecretRequest(module_id=MODULE_ID, llm_format=False)

            with pytest.raises(Exception, match="RPC aborted"):
                await module_servicer.GetModuleSecret(request, fake_context)

        assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED


class TestGetConfigSetupModule:
    """Tests for GetConfigSetupModule endpoint."""

    async def test_get_config_setup_module_success(self, module_servicer, fake_context):
        """The config setup schema rides in ``result.config_setup_schema``."""
        request = module_dto_pb2.GetConfigSetupModuleRequest(module_id=MODULE_ID, llm_format=False)

        response = await module_servicer.GetConfigSetupModule(request, fake_context)

        assert response.result.WhichOneof("outcome") == "config_setup_schema"
        assert response.result.identifier == MODULE_ID
        assert "setup_config" in json_format.MessageToDict(response.result.config_setup_schema)["properties"]

    async def test_get_config_setup_module_not_implemented(self, module_servicer, fake_context):
        """A module without a config setup format aborts with UNIMPLEMENTED."""
        with patch.object(MockModule, "get_config_setup_format", side_effect=NotImplementedError("Not implemented")):
            request = module_dto_pb2.GetConfigSetupModuleRequest(module_id=MODULE_ID, llm_format=False)

            with pytest.raises(Exception, match="RPC aborted"):
                await module_servicer.GetConfigSetupModule(request, fake_context)

        assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED


class TestGetModuleCost:
    """Tests for GetModuleCost endpoint."""

    async def test_get_module_cost_success(self, module_servicer, fake_context):
        """The cost schema rides in ``result.cost_schema``."""
        request = module_dto_pb2.GetModuleCostRequest(module_id=MODULE_ID)

        response = await module_servicer.GetModuleCost(request, fake_context)

        assert response.result.WhichOneof("outcome") == "cost_schema"
        assert response.result.identifier == MODULE_ID
        assert "tokens" in json_format.MessageToDict(response.result.cost_schema)["properties"]

    async def test_get_module_cost_not_implemented(self, module_servicer, fake_context):
        """A module without a cost format aborts with UNIMPLEMENTED."""
        with patch.object(MockModule, "get_cost_format", side_effect=NotImplementedError("Not implemented")):
            request = module_dto_pb2.GetModuleCostRequest(module_id=MODULE_ID)

            with pytest.raises(Exception, match="RPC aborted"):
                await module_servicer.GetModuleCost(request, fake_context)

        assert fake_context.get_code() == grpc.StatusCode.UNIMPLEMENTED


class TestConfigSetupModule:
    """Tests for ConfigSetupModule endpoint."""

    async def test_config_setup_module_success(self, module_servicer, fake_context, mock_job_manager):
        """The configured setup version rides in ``result.setup_version``, identified by its id."""
        response = await module_servicer.ConfigSetupModule(config_setup_request(), fake_context)

        assert response.result.WhichOneof("outcome") == "setup_version"
        assert response.result.identifier == "setup_versions:123"
        assert response.result.setup_version.setup_id == "setups:123"
        assert json_format.MessageToDict(response.result.setup_version.content) == {"updated": "config"}
        mock_job_manager.create_config_setup_instance_job.assert_awaited_once()
        mock_job_manager.generate_config_setup_module_response.assert_awaited_once_with("test-config-job-id")

    async def test_config_setup_module_access_denied(self, module_servicer, fake_context, mock_job_manager):
        """A setup the caller may not access aborts with PERMISSION_DENIED before any job starts."""
        module_servicer.user_profile.check_resource_access = AsyncMock(return_value=False)

        with pytest.raises(Exception, match="RPC aborted"):
            await module_servicer.ConfigSetupModule(config_setup_request(), fake_context)

        assert fake_context.get_code() == grpc.StatusCode.PERMISSION_DENIED
        assert "setups:123" in fake_context.get_details()
        module_servicer.user_profile.check_resource_access.assert_awaited_once_with("setup_id", "setups:123")
        mock_job_manager.create_config_setup_instance_job.assert_not_awaited()

    async def test_config_setup_module_start_failure_is_internal(
            self, module_servicer, fake_context, mock_job_manager
    ):
        """Regression: a module failing to start escaped the servicer and surfaced as UNKNOWN."""
        mock_job_manager.create_config_setup_instance_job = AsyncMock(side_effect=RuntimeError("boom"))

        with pytest.raises(Exception, match="RPC aborted"):
            await module_servicer.ConfigSetupModule(config_setup_request(), fake_context)

        assert fake_context.get_code() == grpc.StatusCode.INTERNAL
        assert "module failed to start config setup: boom" in fake_context.get_details()
        mock_job_manager.generate_config_setup_module_response.assert_not_awaited()

    @pytest.mark.parametrize(
        ("code", "status"),
        [
            (str(grpc.StatusCode.DEADLINE_EXCEEDED), grpc.StatusCode.DEADLINE_EXCEEDED),
            (str(grpc.StatusCode.NOT_FOUND), grpc.StatusCode.NOT_FOUND),
            ("INVALID_ARGUMENT", grpc.StatusCode.INVALID_ARGUMENT),
            ("OK", grpc.StatusCode.INTERNAL),
        ],
    )
    async def test_config_setup_module_failure_keeps_its_status(
            self, module_servicer, fake_context, mock_job_manager, code: str, status: grpc.StatusCode
    ):
        """Regression: a timed-out or vanished job read as INTERNAL; the status it names now rides through."""
        mock_job_manager.generate_config_setup_module_response = AsyncMock(
            return_value=ModuleCodeModel(code=code, message="config setup failed")
        )

        with pytest.raises(Exception, match="RPC aborted"):
            await module_servicer.ConfigSetupModule(config_setup_request(), fake_context)

        assert fake_context.get_code() == status
        assert fake_context.get_details() == "config setup failed"

    async def test_config_setup_module_module_failure_is_internal(
            self, module_servicer, fake_context, mock_job_manager
    ):
        """A module reporting a ModuleCodeModel aborts with INTERNAL and its message, caching nothing."""
        mock_job_manager.generate_config_setup_module_response = AsyncMock(
            return_value=ModuleCodeModel(code="CONFIG_FAILED", message="tool unreachable")
        )

        with pytest.raises(Exception, match="RPC aborted"):
            await module_servicer.ConfigSetupModule(config_setup_request(), fake_context)

        assert fake_context.get_code() == grpc.StatusCode.INTERNAL
        assert fake_context.get_details() == "tool unreachable"
        assert "setups:123" not in module_servicer._setup_cache

    @pytest.mark.regression
    async def test_a_setup_not_matching_the_model_is_invalid_argument(self, module_servicer, fake_context):
        """A malformed setup must name its bad fields, not escape as an UNKNOWN servicer crash.

        Production (archetype-ada, 2026-09-09) raised pydantic's ValidationError straight out
        of the servicer: grpc logged "Unexpected [ValidationError] raised by servicer method"
        and the caller got UNKNOWN with a stack trace instead of the three missing field names.
        """

        class _Expected(BaseModel):
            agent_setup: dict
            tools: list
            knowledge: dict

        try:
            _Expected()
        except ValidationError as error:
            mismatch = error

        with (
            patch.object(MockModule, "create_setup_model", side_effect=mismatch),
            pytest.raises(Exception, match="RPC aborted"),
        ):
            await module_servicer.ConfigSetupModule(config_setup_request({}), fake_context)

        assert fake_context.get_code() == grpc.StatusCode.INVALID_ARGUMENT
        details = fake_context.get_details()
        assert "setup_versions:123" in details
        assert "agent_setup" in details
        assert "tools" in details
        assert "knowledge" in details


class TestSetupAccessGate:
    """Setup access control gating resolve_setup."""

    async def test_resolve_setup_denied_raises(self, module_servicer: ModuleServicer) -> None:
        """A denied setup blocks resolve_setup with PermissionDeniedError before any fetch."""
        module_servicer.user_profile.check_resource_access = AsyncMock(return_value=False)
        with pytest.raises(PermissionDeniedError):
            await module_servicer.resolve_setup("setups:x", "missions:m")
        module_servicer.user_profile.check_resource_access.assert_awaited_once_with("setup_id", "setups:x")
        module_servicer.setup.get_setup.assert_not_called()
