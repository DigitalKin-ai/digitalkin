"""Tests for _build_parameters_from_schema and _extract_tools_from_schema.

Validates that direct JSON Schema extraction (with inline_refs) handles
$ref, oneOf, anyOf, enums, const fields, dict types, nested models,
and list[Model] correctly.
"""

import pytest

from digitalkin.models.module.tool_cache import ToolModuleInfo
from digitalkin.utils.llm_ready_schema import LlmReadySchema

SCHEMA: dict = {
    "title": "Request",
    "$defs": {
        "Category": {
            "enum": ["billing", "support", "sales"],
            "type": "string",
        },
        "PriceRule": {
            "title": "PriceRule",
            "properties": {
                "max_value": {"type": "number"},
                "name": {"type": "string"},
                "rule_type": {
                    "default": "price",
                    "const": "price",
                    "type": "string",
                },
                "category": {"$ref": "#/$defs/Category"},
            },
            "type": "object",
            "required": ["name", "category", "max_value"],
        },
        "CountRule": {
            "title": "CountRule",
            "properties": {
                "max_value": {"type": "number"},
                "name": {"type": "string"},
                "rule_type": {
                    "default": "count",
                    "const": "count",
                    "type": "string",
                },
                "category": {"$ref": "#/$defs/Category"},
            },
            "type": "object",
            "required": ["name", "category", "max_value"],
        },
        "TextPayload": {
            "title": "TextPayload",
            "properties": {
                "kind": {
                    "default": "text",
                    "const": "text",
                    "type": "string",
                },
                "body": {"type": "string"},
            },
            "type": "object",
            "required": ["body"],
        },
        "PingPayload": {
            "title": "PingPayload",
            "properties": {
                "kind": {
                    "default": "ping",
                    "const": "ping",
                    "type": "string",
                },
            },
            "type": "object",
        },
    },
    "properties": {
        "tags": {
            "additionalProperties": {"type": "string"},
            "default": {},
            "type": "object",
        },
        "payload": {
            "oneOf": [
                {"$ref": "#/$defs/TextPayload"},
                {"$ref": "#/$defs/PingPayload"},
            ],
        },
        "rules": {
            "anyOf": [
                {
                    "items": {
                        "oneOf": [
                            {"$ref": "#/$defs/CountRule"},
                            {"$ref": "#/$defs/PriceRule"},
                        ],
                    },
                    "type": "array",
                },
                {"type": "null"},
            ]
        },
    },
    "type": "object",
    "required": ["payload"],
}


def _inline_def(def_name: str) -> dict:
    """Inline $refs for a sub-schema, mimicking _extract_tools_from_schema."""
    return LlmReadySchema.inline_refs({**SCHEMA["$defs"][def_name], "$defs": SCHEMA["$defs"]})


class TestBuildParametersFromSchema:
    """Tests for _build_parameters_from_schema (direct JSON Schema extraction)."""

    def test_text_payload_properties(self) -> None:
        """TextPayload keeps body, kind; protocol/created_at absent."""
        result = ToolModuleInfo._build_parameters_from_schema(_inline_def("TextPayload"))
        props = result["properties"]
        assert "body" in props
        assert "kind" in props
        assert "protocol" not in props
        assert "created_at" not in props

    def test_text_payload_required(self) -> None:
        """body is required, kind is not (has default)."""
        result = ToolModuleInfo._build_parameters_from_schema(_inline_def("TextPayload"))
        assert "body" in result["required"]
        assert "kind" not in result["required"]

    def test_ping_payload_no_required(self) -> None:
        """PingPayload has no required fields (kind has default)."""
        result = ToolModuleInfo._build_parameters_from_schema(_inline_def("PingPayload"))
        assert result["required"] == []

    def test_price_rule_ref_resolved(self) -> None:
        """PriceRule $ref to Category is inlined as enum."""
        result = ToolModuleInfo._build_parameters_from_schema(_inline_def("PriceRule"))
        cat_schema = result["properties"]["category"]
        assert "$ref" not in cat_schema
        assert "enum" in cat_schema

    def test_price_rule_required_fields(self) -> None:
        """PriceRule has name, category, max_value required."""
        result = ToolModuleInfo._build_parameters_from_schema(_inline_def("PriceRule"))
        for field in ("name", "category", "max_value"):
            assert field in result["required"]

    def test_count_rule_const_field_present(self) -> None:
        """CountRule keeps rule_type (const field, not protocol)."""
        result = ToolModuleInfo._build_parameters_from_schema(_inline_def("CountRule"))
        assert "rule_type" in result["properties"]
        assert result["properties"]["rule_type"]["const"] == "count"

    def test_protocol_and_created_at_skipped(self) -> None:
        """Protocol and created_at fields are stripped."""
        schema = {
            "properties": {
                "protocol": {"const": "test", "type": "string"},
                "created_at": {"type": "string"},
                "query": {"type": "string"},
            },
            "required": ["protocol", "query"],
        }
        result = ToolModuleInfo._build_parameters_from_schema(schema)
        assert "protocol" not in result["properties"]
        assert "created_at" not in result["properties"]
        assert "query" in result["properties"]
        assert "query" in result["required"]
        assert "protocol" not in result["required"]

    def test_dict_field_preserved(self) -> None:
        """dict[str, Any] field (additionalProperties) is preserved."""
        schema = {
            "properties": {
                "protocol": {"const": "test", "type": "string"},
                "patch": {"additionalProperties": True, "type": "object", "description": "Merge-patch"},
            },
            "required": ["patch"],
        }
        result = ToolModuleInfo._build_parameters_from_schema(schema)
        assert "patch" in result["properties"]
        assert result["properties"]["patch"]["type"] == "object"
        assert result["properties"]["patch"]["additionalProperties"] is True
        assert "patch" in result["required"]

    def test_dict_str_str_field_preserved(self) -> None:
        """dict[str, str] field is preserved with typed additionalProperties."""
        schema = {
            "properties": {
                "protocol": {"const": "test", "type": "string"},
                "arguments": {"additionalProperties": {"type": "string"}, "type": "object"},
            },
            "required": ["arguments"],
        }
        result = ToolModuleInfo._build_parameters_from_schema(schema)
        assert result["properties"]["arguments"]["additionalProperties"] == {"type": "string"}

    def test_any_field_preserved(self) -> None:
        """Any-typed field (no type constraint) is preserved."""
        schema = {
            "properties": {
                "protocol": {"const": "test", "type": "string"},
                "content": {"description": "Any content", "title": "Content"},
            },
            "required": ["content"],
        }
        result = ToolModuleInfo._build_parameters_from_schema(schema)
        assert "content" in result["properties"]
        assert result["properties"]["content"]["description"] == "Any content"

    def test_anyof_union_preserved(self) -> None:
        """str | None union (anyOf) is preserved."""
        schema = {
            "properties": {
                "protocol": {"const": "test", "type": "string"},
                "json_path": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "description": "Dot-notation path",
                },
            },
            "required": [],
        }
        result = ToolModuleInfo._build_parameters_from_schema(schema)
        assert "anyOf" in result["properties"]["json_path"]


class TestExtractToolsFromSchema:
    """Tests for _extract_tools_from_schema with protocol-based discriminator."""

    def _make_protocol_schema(self) -> dict:
        """Adapt SCHEMA to use 'protocol' as discriminator (as real triggers do)."""
        return {
            "$defs": {
                "Category": SCHEMA["$defs"]["Category"],
                "SearchTrigger": {
                    "title": "SearchTrigger",
                    "description": "Search for information",
                    "properties": {
                        "protocol": {"const": "search", "default": "search", "type": "string"},
                        "query": {"type": "string"},
                        "category": {"$ref": "#/$defs/Category"},
                    },
                    "type": "object",
                    "required": ["protocol", "query", "category"],
                },
                "PingTrigger": {
                    "title": "PingTrigger",
                    "description": "Ping the service",
                    "properties": {
                        "protocol": {"const": "ping", "default": "ping", "type": "string"},
                    },
                    "type": "object",
                },
            },
        }

    def test_extracts_tool_definitions(self) -> None:
        """Extracts ToolDefinitions from protocol-discriminated schema."""
        tools = ToolModuleInfo._extract_tools_from_schema(self._make_protocol_schema())
        names = {t.name for t in tools}
        assert "search" in names
        assert "ping" in names

    def test_protocol_stripped_from_parameters(self) -> None:
        """Protocol field is not in parameters_schema."""
        tools = ToolModuleInfo._extract_tools_from_schema(self._make_protocol_schema())
        for tool in tools:
            assert "protocol" not in tool.parameters_schema.get("properties", {})

    def test_search_tool_has_query_and_category(self) -> None:
        """Search tool parameters include query and resolved category."""
        tools = ToolModuleInfo._extract_tools_from_schema(self._make_protocol_schema())
        search = next(t for t in tools if t.name == "search")
        props = search.parameters_schema["properties"]
        assert "query" in props
        assert "category" in props

    def test_search_tool_category_ref_resolved(self) -> None:
        """$ref to Category enum is inlined in extracted tool."""
        tools = ToolModuleInfo._extract_tools_from_schema(self._make_protocol_schema())
        search = next(t for t in tools if t.name == "search")
        cat = search.parameters_schema["properties"]["category"]
        assert "$ref" not in cat
        assert "enum" in cat
        assert cat["enum"] == ["billing", "support", "sales"]

    def test_search_tool_required_excludes_protocol(self) -> None:
        """Required list has query and category but not protocol."""
        tools = ToolModuleInfo._extract_tools_from_schema(self._make_protocol_schema())
        search = next(t for t in tools if t.name == "search")
        assert "query" in search.parameters_schema["required"]
        assert "category" in search.parameters_schema["required"]
        assert "protocol" not in search.parameters_schema["required"]

    def test_ping_tool_empty_parameters(self) -> None:
        """Ping tool has no parameters (only protocol, which is stripped)."""
        tools = ToolModuleInfo._extract_tools_from_schema(self._make_protocol_schema())
        ping = next(t for t in tools if t.name == "ping")
        assert ping.parameters_schema["properties"] == {}
        assert ping.parameters_schema["required"] == []

    def test_description_extracted(self) -> None:
        """Tool description comes from schema description field."""
        tools = ToolModuleInfo._extract_tools_from_schema(self._make_protocol_schema())
        search = next(t for t in tools if t.name == "search")
        assert search.description == "Search for information"

    def test_non_protocol_defs_skipped(self) -> None:
        """$defs without protocol const (like Category enum) are skipped."""
        tools = ToolModuleInfo._extract_tools_from_schema(self._make_protocol_schema())
        names = {t.name for t in tools}
        assert "Category" not in names

    def test_no_tools_when_no_protocol_const(self) -> None:
        """Original schema (using 'kind'/'rule_type', not 'protocol') yields no tools."""
        tools = ToolModuleInfo._extract_tools_from_schema(SCHEMA)
        assert tools == []

    def test_nested_model_ref_inlined(self) -> None:
        """Nested model via $ref (CostBudget | None) is inlined."""
        schema = {
            "$defs": {
                "CostBudget": {
                    "properties": {
                        "max_cost_usd": {"type": "number"},
                        "prefer_low_cost": {"type": "boolean", "default": False},
                    },
                    "type": "object",
                    "title": "CostBudget",
                },
                "SearchTrigger": {
                    "description": "Search",
                    "properties": {
                        "protocol": {"const": "search", "default": "search", "type": "string"},
                        "query": {"type": "string"},
                        "cost_budget": {
                            "anyOf": [{"$ref": "#/$defs/CostBudget"}, {"type": "null"}],
                            "default": None,
                        },
                    },
                    "type": "object",
                    "required": ["query"],
                },
            },
        }
        tools = ToolModuleInfo._extract_tools_from_schema(schema)
        search = next(t for t in tools if t.name == "search")
        cost_budget = search.parameters_schema["properties"]["cost_budget"]
        # $ref should be resolved
        any_of = cost_budget["anyOf"]
        obj_variant = next(v for v in any_of if v.get("type") == "object")
        assert "properties" in obj_variant
        assert "max_cost_usd" in obj_variant["properties"]

    def test_list_nested_model_ref_inlined(self) -> None:
        """list[NestedModel] with $ref in items is inlined."""
        schema = {
            "$defs": {
                "CitationQuery": {
                    "properties": {
                        "journal": {"type": "string"},
                        "author": {"type": "string"},
                    },
                    "required": ["journal", "author"],
                    "type": "object",
                    "title": "CitationQuery",
                },
                "CitMatchTrigger": {
                    "description": "Citation match",
                    "properties": {
                        "protocol": {"const": "cit_match", "default": "cit_match", "type": "string"},
                        "citations": {
                            "items": {"$ref": "#/$defs/CitationQuery"},
                            "type": "array",
                        },
                    },
                    "type": "object",
                    "required": ["citations"],
                },
            },
        }
        tools = ToolModuleInfo._extract_tools_from_schema(schema)
        tool = next(t for t in tools if t.name == "cit_match")
        citations = tool.parameters_schema["properties"]["citations"]
        assert citations["type"] == "array"
        assert "$ref" not in citations["items"]
        assert "journal" in citations["items"]["properties"]

    def test_dict_field_not_lost(self) -> None:
        """dict[str, Any] field is preserved (the original bug)."""
        schema = {
            "$defs": {
                "PatchTrigger": {
                    "description": "Patch a record",
                    "properties": {
                        "protocol": {"const": "patch", "default": "patch", "type": "string"},
                        "patch": {"additionalProperties": True, "type": "object", "description": "Merge-patch"},
                        "collection": {"type": "string"},
                    },
                    "type": "object",
                    "required": ["patch", "collection"],
                },
            },
        }
        tools = ToolModuleInfo._extract_tools_from_schema(schema)
        tool = next(t for t in tools if t.name == "patch")
        assert "patch" in tool.parameters_schema["properties"]
        assert "patch" in tool.parameters_schema["required"]
        assert tool.parameters_schema["properties"]["patch"]["additionalProperties"] is True
