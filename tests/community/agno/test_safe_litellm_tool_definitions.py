"""SafeLiteLLM sends tool definitions every provider accepts.

agno serializes a tool with its persistence fields (``requires_confirmation``, ``external_execution``,
``approval_type``) inside ``function``: Mistral rejects unknown fields with a 422. Pydantic writes a
discriminated union as ``oneOf`` plus an OpenAPI ``discriminator``: Kimi on Bedrock rejects ``oneOf``.
"""

import logging
from typing import Annotated, Literal

import jsonschema
import pytest
from agno.tools.function import Function
from pydantic import BaseModel, Field, TypeAdapter

from digitalkin.community.agno.safe_litellm import SafeLiteLLM
from digitalkin.community.agno.toolkits import ServicesManager
from digitalkin.services.registry import DefaultRegistry
from digitalkin.services.setup.default_setup import DefaultSetup


class GetAction(BaseModel):
    action: Literal["get"]
    kin_id: str


class SearchAction(BaseModel):
    action: Literal["search"]
    query: str = ""


class ManagerArgs(BaseModel):
    action: Annotated[GetAction | SearchAction, Field(discriminator="action")]


def _manager_function(**flags: object) -> Function:
    return Function(
        name="kins_manager",
        description="Manage kins.",
        parameters=TypeAdapter(ManagerArgs).json_schema(),
        **flags,
    )


def _sent_tools(*functions: Function, **model_kwargs: object) -> list[dict]:
    model = SafeLiteLLM(id="mistral-large-4", **model_kwargs)
    return model.get_request_params(tools=model._format_tools(list(functions)))["tools"]


def _keys(node: object) -> set[str]:
    if isinstance(node, dict):
        return set(node) | {k for v in node.values() for k in _keys(v)}
    if isinstance(node, list):
        return {k for v in node for k in _keys(v)}
    return set()


class TestToolDefinitionFields:
    def test_agno_execution_fields_never_reach_the_request(self) -> None:
        function = _manager_function(requires_confirmation=False, external_execution=False, approval_type="audit")
        assert {"requires_confirmation", "external_execution", "approval_type"} <= set(function.to_dict())

        (sent,) = _sent_tools(function)

        assert set(sent["function"]) == {"name", "description", "parameters"}

    def test_strict_is_kept(self) -> None:
        (sent,) = _sent_tools(_manager_function(strict=True))

        assert sent["function"]["strict"] is True

    def test_each_dropped_field_is_logged_with_its_value(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.DEBUG, logger="digitalkin"):
            _sent_tools(_manager_function(requires_confirmation=False, approval_type="audit"))

        assert "kins_manager" in caplog.text
        assert "requires_confirmation=False" in caplog.text
        assert "approval_type='audit'" in caplog.text


class TestToolSchemaUnions:
    def test_pydantic_discriminated_union_is_sent_as_anyof(self) -> None:
        original = TypeAdapter(ManagerArgs).json_schema()
        assert "oneOf" in _keys(original)
        assert "discriminator" in _keys(original)

        (sent,) = _sent_tools(_manager_function())

        sent_keys = _keys(sent["function"]["parameters"])
        assert "oneOf" not in sent_keys
        assert "discriminator" not in sent_keys
        assert "anyOf" in sent_keys

    @pytest.mark.parametrize(
        ("arguments", "valid"),
        [
            ({"action": {"action": "get", "kin_id": "setups:1"}}, True),
            ({"action": {"action": "search"}}, True),
            ({"action": {"action": "get"}}, False),
            ({"action": {"action": "delete", "kin_id": "setups:1"}}, False),
            ({"action": "search"}, False),
        ],
    )
    def test_sent_schema_accepts_exactly_what_the_original_accepts(self, arguments: dict, valid: bool) -> None:
        original = TypeAdapter(ManagerArgs).json_schema()
        (sent,) = _sent_tools(_manager_function())

        assert jsonschema.Draft202012Validator(original).is_valid(arguments) is valid
        assert jsonschema.Draft202012Validator(sent["function"]["parameters"]).is_valid(arguments) is valid

    @pytest.mark.edge_case
    def test_properties_named_like_keywords_are_kept(self) -> None:
        parameters = {
            "type": "object",
            "properties": {"oneOf": {"type": "string"}, "discriminator": {"type": "integer"}},
            "required": ["oneOf", "discriminator"],
        }

        (sent,) = _sent_tools(Function(name="odd_names", parameters=parameters))

        assert sent["function"]["parameters"] == parameters

    def test_discriminator_is_logged_when_dropped(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.DEBUG, logger="digitalkin"):
            _sent_tools(_manager_function())

        assert "discriminator=" in caplog.text
        assert "'propertyName': 'action'" in caplog.text


class TestToolDefinitionsElsewhere:
    def test_caller_tools_are_not_mutated(self) -> None:
        model = SafeLiteLLM(id="mistral-large-4")
        tools = model._format_tools([_manager_function(requires_confirmation=False)])
        before = repr(tools)

        model.get_request_params(tools=tools)

        assert repr(tools) == before

    def test_prompt_cache_marker_lands_on_the_cleaned_last_tool(self) -> None:
        sent = _sent_tools(
            Function(name="a", parameters={"type": "object", "properties": {}}),
            _manager_function(requires_confirmation=False),
            enable_prompt_caching=True,
        )

        assert "cache_control" not in sent[0]
        assert sent[1]["cache_control"] == {"type": "ephemeral"}
        assert "requires_confirmation" not in sent[1]["function"]

    def test_no_tools_sends_no_tools(self) -> None:
        assert "tools" not in SafeLiteLLM(id="mistral-large-4").get_request_params()

    def test_services_manager_reaches_the_request_with_every_action(self) -> None:
        function = ServicesManager(DefaultSetup(), DefaultRegistry("", "", "")).async_functions["services_manager"]
        original = function.to_dict()["parameters"]["properties"]["action"]

        (sent,) = _sent_tools(function)

        parameters = sent["function"]["parameters"]
        jsonschema.Draft202012Validator.check_schema(parameters)
        assert not {"oneOf", "discriminator"} & _keys(parameters)
        assert parameters["properties"]["action"]["anyOf"] == original["oneOf"]
