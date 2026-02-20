"""Tests for BaseModule format methods (get_*_format).

These tests exercise the actual format method implementations on BaseModule
rather than mocked versions, covering schema generation and SchemaSplitter integration.
"""

import json
from typing import Literal
from unittest.mock import Mock

import pytest
from pydantic import BaseModel, Field

from digitalkin.models.module.base_types import DataModel, DataTrigger
from digitalkin.models.module.module_types import SetupModel
from digitalkin.models.module.select_schema import SelectSchema
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.cost.cost_strategy import CostConfig
from digitalkin.utils.package_discover import ModuleDiscoverer


class _InputTrigger(DataTrigger):
    protocol: Literal["test"] = "test"
    message: str = ""


class _InputModel(DataModel[_InputTrigger]):
    pass


class _OutputTrigger(DataTrigger):
    protocol: Literal["test"] = "test"
    result: str = ""


class _OutputModel(DataModel[_OutputTrigger]):
    pass


class _SetupModel(SetupModel):
    name: str = Field(default="default")
    timeout: int = Field(default=30, json_schema_extra={"config": True})
    internal: str = Field(default="", json_schema_extra={"ui:widget": "hidden"})


class _SecretModel(BaseModel):
    api_key: str = Field(default="", json_schema_extra={"ui:widget": "password"})


class _FormatModule(BaseModule[_InputModel, _OutputModel, _SetupModel, _SecretModel]):
    """Minimal concrete module for testing format methods."""

    name = "format_test"
    description = "Test module"
    setup_format = _SetupModel
    input_format = _InputModel
    output_format = _OutputModel
    secret_format = _SecretModel
    select_format = SelectSchema
    metadata = {"module_id": "test"}
    triggers_discoverer = ModuleDiscoverer("test")
    services_config_strategies = {}
    services_config_params = {}

    async def initialize(self, context, setup_data) -> None:  # noqa: ARG002
        pass

    async def cleanup(self) -> None:
        pass


# Register a fake trigger so get_registered_protocols_with_info works
_mock_handler = Mock()
_mock_handler.protocol = "test"
_mock_handler.description = "Test trigger"
_FormatModule.triggers_discoverer._trigger_handlers_cls["test"] = [_mock_handler]


class TestGetSecretFormat:
    """Tests for BaseModule.get_secret_format."""

    @pytest.mark.asyncio
    async def test_secret_format_raw(self) -> None:
        """Raw format returns plain JSON schema."""
        result = await _FormatModule.get_secret_format(llm_format=False)
        schema = json.loads(result)
        assert "properties" in schema
        assert "api_key" in schema["properties"]

    @pytest.mark.asyncio
    async def test_secret_format_llm(self) -> None:
        """LLM format returns json_schema + ui_schema."""
        result = await _FormatModule.get_secret_format(llm_format=True)
        data = json.loads(result)
        assert "json_schema" in data
        assert "ui_schema" in data
        assert "api_key" in data["json_schema"]["properties"]
        assert data["ui_schema"]["api_key"]["ui:widget"] == "password"

    @pytest.mark.asyncio
    async def test_secret_format_not_implemented(self) -> None:
        """Raises NotImplementedError when secret_format is None."""

        class NoSecret(BaseModule):
            secret_format = None

        with pytest.raises(NotImplementedError):
            await NoSecret.get_secret_format(llm_format=False)


class TestGetInputFormat:
    """Tests for BaseModule.get_input_format."""

    @pytest.mark.asyncio
    async def test_input_format_raw(self) -> None:
        """Raw format returns plain JSON schema."""
        result = await _FormatModule.get_input_format(llm_format=False)
        schema = json.loads(result)
        assert "properties" in schema or "$defs" in schema

    @pytest.mark.asyncio
    async def test_input_format_llm(self) -> None:
        """LLM format returns only json_schema (no ui_schema)."""
        result = await _FormatModule.get_input_format(llm_format=True)
        data = json.loads(result)
        assert "json_schema" in data
        assert "ui_schema" not in data

    @pytest.mark.asyncio
    async def test_input_format_not_implemented(self) -> None:
        """Raises NotImplementedError when input_format is None."""

        class NoInput(BaseModule):
            input_format = None

        with pytest.raises(NotImplementedError):
            await NoInput.get_input_format(llm_format=False)


class TestGetOutputFormat:
    """Tests for BaseModule.get_output_format."""

    @pytest.mark.asyncio
    async def test_output_format_raw(self) -> None:
        """Raw format returns plain JSON schema."""
        result = await _FormatModule.get_output_format(llm_format=False)
        schema = json.loads(result)
        assert "properties" in schema or "$defs" in schema

    @pytest.mark.asyncio
    async def test_output_format_llm(self) -> None:
        """LLM format returns only json_schema (no ui_schema)."""
        result = await _FormatModule.get_output_format(llm_format=True)
        data = json.loads(result)
        assert "json_schema" in data
        assert "ui_schema" not in data

    @pytest.mark.asyncio
    async def test_output_format_not_implemented(self) -> None:
        """Raises NotImplementedError when output_format is None."""

        class NoOutput(BaseModule):
            output_format = None

        with pytest.raises(NotImplementedError):
            await NoOutput.get_output_format(llm_format=False)


class TestGetConfigSetupFormat:
    """Tests for BaseModule.get_config_setup_format."""

    @pytest.mark.asyncio
    async def test_config_setup_format_raw(self) -> None:
        """Raw format returns plain JSON schema with config fields, without hidden."""
        result = await _FormatModule.get_config_setup_format(llm_format=False)
        schema = json.loads(result)
        props = schema.get("properties", {})
        assert "timeout" in props
        assert "internal" not in props
        assert "resolved_tools" not in props

    @pytest.mark.asyncio
    async def test_config_setup_format_llm(self) -> None:
        """LLM format returns json_schema + ui_schema, without hidden fields."""
        result = await _FormatModule.get_config_setup_format(llm_format=True)
        data = json.loads(result)
        assert "json_schema" in data
        assert "ui_schema" in data
        assert "resolved_tools" not in data["json_schema"].get("properties", {})

    @pytest.mark.asyncio
    async def test_config_setup_format_not_implemented(self) -> None:
        """Raises NotImplementedError when setup_format is None."""

        class NoSetup(BaseModule):
            setup_format = None

        with pytest.raises(NotImplementedError):
            await NoSetup.get_config_setup_format(llm_format=False)


class TestGetSetupFormat:
    """Tests for BaseModule.get_setup_format."""

    @pytest.mark.asyncio
    async def test_setup_format_raw(self) -> None:
        """Raw format returns plain JSON schema with hidden fields, without config."""
        result = await _FormatModule.get_setup_format(llm_format=False)
        schema = json.loads(result)
        props = schema.get("properties", {})
        assert "name" in props
        assert "internal" in props
        assert "timeout" not in props

    @pytest.mark.asyncio
    async def test_setup_format_llm(self) -> None:
        """LLM format returns only json_schema (no ui_schema)."""
        result = await _FormatModule.get_setup_format(llm_format=True)
        data = json.loads(result)
        assert "json_schema" in data
        assert "ui_schema" not in data

    @pytest.mark.asyncio
    async def test_setup_format_not_implemented(self) -> None:
        """Raises NotImplementedError when setup_format is None."""

        class NoSetup(BaseModule):
            setup_format = None

        with pytest.raises(NotImplementedError):
            await NoSetup.get_setup_format(llm_format=False)


class TestGetSelectInputFormat:
    """Tests for BaseModule.get_select_input_format."""

    @pytest.mark.asyncio
    async def test_select_input_format_with_protocols(self) -> None:
        """Returns json_schema + ui_schema for trigger selection."""
        result = await _FormatModule.get_select_input_format()
        data = json.loads(result)
        assert "json_schema" in data
        assert "ui_schema" in data

    @pytest.mark.asyncio
    async def test_select_input_format_none(self) -> None:
        """Returns empty dict when select_format is None."""

        class NoSelect(BaseModule):
            select_format = None

        result = await NoSelect.get_select_input_format()
        assert json.loads(result) == {}

    @pytest.mark.asyncio
    async def test_select_input_format_returns_schema_structure(self) -> None:
        """Returns json_schema + ui_schema structure from SelectSchema.build."""
        result = await _FormatModule.get_select_input_format()
        data = json.loads(result)
        # SelectSchema.build returns {"json_schema": ..., "ui_schema": ...}
        assert "json_schema" in data
        assert "ui_schema" in data
        # The "test" protocol should appear
        assert "test" in data["json_schema"]["properties"]


class TestGetCostFormat:
    """Tests for BaseModule.get_cost_format."""

    @pytest.mark.asyncio
    async def test_cost_format_empty(self) -> None:
        """Returns empty dict when no cost config."""
        result = await _FormatModule.get_cost_format(llm_format=False)
        assert json.loads(result) == {}

    @pytest.mark.asyncio
    async def test_cost_format_with_config(self) -> None:
        """Returns cost schema when config is present."""
        cost_config = CostConfig(
            cost_name="api_call",
            cost_type="API_CALL",
            description="Cost per API call",
            unit="USD",
            rate=0.01,
        )

        class CostModule(BaseModule):
            services_config_params = {"cost": {"config": {"api": cost_config}}}

        result = await CostModule.get_cost_format(llm_format=False)
        data = json.loads(result)
        assert "api" in data
        assert data["api"]["name"] == "api_call"
        assert data["api"]["rate"] == 0.01

    @pytest.mark.asyncio
    async def test_cost_format_llm(self) -> None:
        """LLM format returns json_schema + ui_schema."""
        cost_config = CostConfig(
            cost_name="tokens",
            cost_type="TOKEN_INPUT",
            description="Cost per token",
            unit="USD",
            rate=0.001,
        )

        class CostModule(BaseModule):
            services_config_params = {"cost": {"config": {"tok": cost_config}}}

        result = await CostModule.get_cost_format(llm_format=True)
        data = json.loads(result)
        assert "json_schema" in data
        assert "ui_schema" in data

    @pytest.mark.asyncio
    async def test_cost_format_none_params(self) -> None:
        """Returns empty dict when cost params is None."""

        class NoCostModule(BaseModule):
            services_config_params = {"cost": None}

        result = await NoCostModule.get_cost_format(llm_format=False)
        assert json.loads(result) == {}
