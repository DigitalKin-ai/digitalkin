"""Tests for conditional schema utilities."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from digitalkin.utils.conditional_schema import (
    Conditional,
    ConditionalField,
    ConditionalSchemaMixin,
)
from digitalkin.utils.schema_splitter import SchemaSplitter


class TestConditionalField:
    """Tests for the ConditionalField metadata class."""

    def test_creates_with_boolean_show_when(self) -> None:
        """Test ConditionalField with boolean show_when value."""
        cond = ConditionalField(trigger="enabled", show_when=True)

        assert cond.trigger == "enabled"
        assert cond.show_when is True
        assert cond.required_when_shown is True

    def test_creates_with_string_show_when(self) -> None:
        """Test ConditionalField with string show_when value."""
        cond = ConditionalField(trigger="mode", show_when="advanced")

        assert cond.trigger == "mode"
        assert cond.show_when == "advanced"

    def test_creates_with_list_show_when(self) -> None:
        """Test ConditionalField with list show_when value."""
        cond = ConditionalField(trigger="mode", show_when=["standard", "advanced"])

        assert cond.trigger == "mode"
        assert cond.show_when == ["standard", "advanced"]

    def test_normalizes_single_item_list(self) -> None:
        """Test that single-item list is normalized to scalar."""
        cond = ConditionalField(trigger="mode", show_when=["advanced"])

        assert cond.show_when == "advanced"

    def test_required_when_shown_default(self) -> None:
        """Test that required_when_shown defaults to True."""
        cond = ConditionalField(trigger="enabled", show_when=True)
        assert cond.required_when_shown is True

    def test_required_when_shown_false(self) -> None:
        """Test ConditionalField with required_when_shown=False."""
        cond = ConditionalField(trigger="enabled", show_when=True, required_when_shown=False)
        assert cond.required_when_shown is False

    def test_conditional_alias(self) -> None:
        """Test that Conditional is an alias for ConditionalField."""
        assert Conditional is ConditionalField


class TestGetConditionalMetadata:
    """Tests for get_conditional_metadata function."""

    def test_extracts_conditional_from_annotated(self) -> None:
        """Test extraction from Annotated field."""
        cond = ConditionalField(trigger="enabled", show_when=True)

        class Model(BaseModel):
            option: Annotated[str, cond] = "default"

        result = ConditionalSchemaMixin.get_conditional_metadata(Model.model_fields["option"])
        assert result is cond

    def test_returns_none_without_conditional(self) -> None:
        """Test returns None when no ConditionalField metadata."""

        class Model(BaseModel):
            field: str = "value"

        result = ConditionalSchemaMixin.get_conditional_metadata(Model.model_fields["field"])
        assert result is None

    def test_returns_none_with_other_metadata(self) -> None:
        """Test returns None with other Annotated metadata."""

        class Model(BaseModel):
            field: Annotated[str, "some_other_metadata"] = "value"

        result = ConditionalSchemaMixin.get_conditional_metadata(Model.model_fields["field"])
        assert result is None


class TestHasConditional:
    """Tests for the has_conditional function."""

    def test_returns_true_with_conditional_metadata(self) -> None:
        """Test detection when ConditionalField is present."""

        class Model(BaseModel):
            option: Annotated[str, Conditional(trigger="enabled", show_when=True)] = "value"

        assert ConditionalSchemaMixin.has_conditional(Model.model_fields["option"]) is True

    def test_returns_false_without_conditional(self) -> None:
        """Test returns False when no ConditionalField."""

        class Model(BaseModel):
            field: str = "value"

        assert ConditionalSchemaMixin.has_conditional(Model.model_fields["field"]) is False


class TestConditionalSchemaMixin:
    """Tests for ConditionalSchemaMixin JSON schema generation."""

    def test_generates_if_then_for_boolean_condition(self) -> None:
        """Test schema generation for boolean conditional."""

        class Config(ConditionalSchemaMixin):
            enabled: bool = Field(default=False)
            option: Annotated[
                str,
                Conditional(trigger="enabled", show_when=True),
            ] = Field(default="value")

        schema = Config.model_json_schema()

        # option should not be in main properties
        assert "option" not in schema["properties"]

        # enabled should be in main properties
        assert "enabled" in schema["properties"]

        # allOf should contain if/then clause
        assert "allOf" in schema
        assert len(schema["allOf"]) == 1

        if_then = schema["allOf"][0]
        assert if_then["if"]["properties"]["enabled"]["const"] is True
        assert "option" in if_then["then"]["properties"]
        assert "option" in if_then["then"]["required"]

    def test_generates_if_then_for_string_condition(self) -> None:
        """Test schema generation for string (enum) conditional."""

        class Config(ConditionalSchemaMixin):
            mode: Literal["basic", "advanced"] = Field(default="basic")
            advanced_option: Annotated[
                str,
                Conditional(trigger="mode", show_when="advanced"),
            ] = Field(default="value")

        schema = Config.model_json_schema()

        # advanced_option should not be in main properties
        assert "advanced_option" not in schema["properties"]

        # mode should be in main properties
        assert "mode" in schema["properties"]

        # allOf should contain if/then clause with const
        assert "allOf" in schema
        if_then = schema["allOf"][0]
        assert if_then["if"]["properties"]["mode"]["const"] == "advanced"

    def test_generates_if_then_for_list_condition(self) -> None:
        """Test schema generation for list (multiple values) conditional."""

        class Config(ConditionalSchemaMixin):
            mode: Literal["basic", "standard", "advanced"] = Field(default="basic")
            shared_option: Annotated[
                str,
                Conditional(trigger="mode", show_when=["standard", "advanced"]),
            ] = Field(default="value")

        schema = Config.model_json_schema()

        # shared_option should not be in main properties
        assert "shared_option" not in schema["properties"]

        # allOf should contain if/then clause with enum
        assert "allOf" in schema
        if_then = schema["allOf"][0]
        assert "enum" in if_then["if"]["properties"]["mode"]
        assert set(if_then["if"]["properties"]["mode"]["enum"]) == {"standard", "advanced"}

    def test_groups_multiple_fields_same_condition(self) -> None:
        """Test that multiple fields with same condition are grouped."""

        class Config(ConditionalSchemaMixin):
            enabled: bool = Field(default=False)
            option1: Annotated[
                str,
                Conditional(trigger="enabled", show_when=True),
            ] = Field(default="val1")
            option2: Annotated[
                str,
                Conditional(trigger="enabled", show_when=True),
            ] = Field(default="val2")

        schema = Config.model_json_schema()

        # Should have single allOf entry with both fields
        assert len(schema["allOf"]) == 1
        then_props = schema["allOf"][0]["then"]["properties"]
        assert "option1" in then_props
        assert "option2" in then_props

    def test_multiple_conditions_different_triggers(self) -> None:
        """Test multiple conditions with different triggers."""

        class Config(ConditionalSchemaMixin):
            feature_a: bool = Field(default=False)
            feature_b: bool = Field(default=False)
            option_a: Annotated[
                str,
                Conditional(trigger="feature_a", show_when=True),
            ] = Field(default="val_a")
            option_b: Annotated[
                str,
                Conditional(trigger="feature_b", show_when=True),
            ] = Field(default="val_b")

        schema = Config.model_json_schema()

        # Should have two allOf entries
        assert len(schema["allOf"]) == 2

        # Verify each condition
        triggers = [item["if"]["required"][0] for item in schema["allOf"]]
        assert set(triggers) == {"feature_a", "feature_b"}

    def test_required_when_shown_false_excludes_from_required(self) -> None:
        """Test that required_when_shown=False excludes field from required."""

        class Config(ConditionalSchemaMixin):
            enabled: bool = Field(default=False)
            optional_field: Annotated[
                str,
                Conditional(trigger="enabled", show_when=True, required_when_shown=False),
            ] = Field(default="value")

        schema = Config.model_json_schema()

        then_clause = schema["allOf"][0]["then"]
        # Field should be in properties but not in required
        assert "optional_field" in then_clause["properties"]
        assert "required" not in then_clause or "optional_field" not in then_clause.get("required", [])

    def test_preserves_ui_properties(self) -> None:
        """Test that ui:* properties are preserved in conditional fields."""

        class Config(ConditionalSchemaMixin):
            enabled: bool = Field(default=False)
            styled_option: Annotated[
                str,
                Conditional(trigger="enabled", show_when=True),
            ] = Field(
                default="value",
                json_schema_extra={"ui:widget": "select", "ui:placeholder": "Choose..."},
            )

        schema = Config.model_json_schema()

        field_schema = schema["allOf"][0]["then"]["properties"]["styled_option"]
        assert field_schema.get("ui:widget") == "select"
        assert field_schema.get("ui:placeholder") == "Choose..."

    def test_removes_conditional_fields_from_required(self) -> None:
        """Test that conditional fields are removed from main required array."""

        class Config(ConditionalSchemaMixin):
            enabled: bool = Field(...)  # Required
            conditional_required: Annotated[
                str,
                Conditional(trigger="enabled", show_when=True),
            ] = Field(...)  # Required when shown

        schema = Config.model_json_schema()

        # Main required should only have enabled
        assert "required" in schema
        assert "enabled" in schema["required"]
        assert "conditional_required" not in schema["required"]

        # conditional_required should be in then.required
        assert "conditional_required" in schema["allOf"][0]["then"]["required"]

    def test_no_allof_without_conditional_fields(self) -> None:
        """Test that allOf is not added when no conditional fields."""

        class Config(ConditionalSchemaMixin):
            normal_field: str = Field(default="value")
            another_field: int = Field(default=42)

        schema = Config.model_json_schema()

        assert "allOf" not in schema

    def test_model_config_preserved(self) -> None:
        """Test that model_config json_schema_extra is preserved."""

        class Config(ConditionalSchemaMixin):
            model_config = ConfigDict(json_schema_extra={"ui:order": ["enabled", "option"]})

            enabled: bool = Field(default=False)
            option: Annotated[
                str,
                Conditional(trigger="enabled", show_when=True),
            ] = Field(default="value")

        schema = Config.model_json_schema()

        assert schema.get("ui:order") == ["enabled", "option"]


class TestSchemaSplitterIntegration:
    """Tests for SchemaSplitter compatibility with conditional schemas."""

    def test_schema_splitter_extracts_ui_from_conditionals(self) -> None:
        """Test that SchemaSplitter correctly extracts ui:* from conditional fields."""

        class Config(ConditionalSchemaMixin):
            enabled: bool = Field(
                default=False,
                json_schema_extra={"ui:widget": "checkbox"},
            )
            option: Annotated[
                Literal["a", "b", "c"],
                Conditional(trigger="enabled", show_when=True),
            ] = Field(
                default="a",
                json_schema_extra={"ui:widget": "select"},
            )

        combined = Config.model_json_schema()
        json_schema, ui_schema = SchemaSplitter.split(combined)

        # JSON schema should have allOf structure
        assert "allOf" in json_schema

        # UI schema should have widget info for enabled
        assert ui_schema.get("enabled", {}).get("ui:widget") == "checkbox"

        # UI schema should also have widget info for conditional option
        assert ui_schema.get("option", {}).get("ui:widget") == "select"


class TestComplexConditionalScenarios:
    """Tests for complex conditional field scenarios."""

    def test_enum_trigger_with_multiple_dependent_fields(self) -> None:
        """Test enum trigger with fields depending on different values."""

        class Config(ConditionalSchemaMixin):
            mode: Literal["basic", "standard", "advanced"] = Field(default="basic")

            # Only for standard mode
            standard_only: Annotated[
                str,
                Conditional(trigger="mode", show_when="standard"),
            ] = Field(default="std")

            # Only for advanced mode
            advanced_only: Annotated[
                str,
                Conditional(trigger="mode", show_when="advanced"),
            ] = Field(default="adv")

            # For both standard and advanced
            non_basic: Annotated[
                str,
                Conditional(trigger="mode", show_when=["standard", "advanced"]),
            ] = Field(default="both")

        schema = Config.model_json_schema()

        # Should have 3 allOf entries
        assert len(schema["allOf"]) == 3

        # Collect conditions
        conditions = {}
        for item in schema["allOf"]:
            mode_cond = item["if"]["properties"]["mode"]
            fields = list(item["then"]["properties"].keys())
            if "const" in mode_cond:
                conditions[mode_cond["const"]] = fields
            else:
                conditions[tuple(sorted(mode_cond["enum"]))] = fields

        assert conditions.get("standard") == ["standard_only"]
        assert conditions.get("advanced") == ["advanced_only"]
        assert ("advanced", "standard") in conditions or ("standard", "advanced") in conditions

    def test_nested_model_with_conditionals(self) -> None:
        """Test that nested models with conditionals work correctly."""

        class NestedConfig(ConditionalSchemaMixin):
            enabled: bool = Field(default=False)
            nested_option: Annotated[
                str,
                Conditional(trigger="enabled", show_when=True),
            ] = Field(default="nested")

        class ParentConfig(BaseModel):
            name: str = Field(default="parent")
            nested: NestedConfig = Field(default_factory=NestedConfig)

        schema = ParentConfig.model_json_schema()

        # Nested schema should be in $defs
        assert "$defs" in schema
        nested_def = schema["$defs"].get("NestedConfig")
        assert nested_def is not None
        assert "allOf" in nested_def

    def test_with_literal_enum_field(self) -> None:
        """Test conditional with Literal type field."""

        class Config(ConditionalSchemaMixin):
            web_search_enabled: bool = Field(default=False)
            web_search_engine: Annotated[
                Literal["duckduckgo", "google", "bing"],
                Conditional(trigger="web_search_enabled", show_when=True),
            ] = Field(default="duckduckgo")

        schema = Config.model_json_schema()

        # Verify the enum is preserved in then clause
        then_props = schema["allOf"][0]["then"]["properties"]
        assert "enum" in then_props["web_search_engine"]
        assert set(then_props["web_search_engine"]["enum"]) == {"duckduckgo", "google", "bing"}


class TestAdaSetupIntegration:
    """Integration test with Ada setup pattern."""

    def test_ada_tools_pattern(self) -> None:
        """Test the pattern used in Ada Tools model."""

        class Tools(ConditionalSchemaMixin):
            model_config = ConfigDict(
                json_schema_extra={
                    "ui:order": [
                        "web_search_tool_enabled",
                        "web_search_tool",
                        "other_tool",
                    ]
                }
            )

            web_search_tool_enabled: bool = Field(
                ...,
                title="Web Search tool",
                description="Allow your agent to browse the web.",
                json_schema_extra={"ui:widget": "checkbox"},
            )
            web_search_tool: Annotated[
                Literal["duckduckgo", "tavily", "baidusearch"],
                Conditional(trigger="web_search_tool_enabled", show_when=True),
            ] = Field(
                default="duckduckgo",
                title="Web Search Engine",
                description="Select the search engine to use.",
                json_schema_extra={
                    "ui:widget": "GroupedIconSelectWidget",
                    "ui:placeholder": "Pick one...",
                },
            )
            other_tool: bool = Field(
                default=False,
                title="Other tool",
                json_schema_extra={"ui:widget": "checkbox"},
            )

        schema = Tools.model_json_schema()

        # Verify structure
        assert "web_search_tool_enabled" in schema["properties"]
        assert "other_tool" in schema["properties"]
        assert "web_search_tool" not in schema["properties"]

        # Verify allOf
        assert len(schema["allOf"]) == 1
        if_then = schema["allOf"][0]
        assert if_then["if"]["properties"]["web_search_tool_enabled"]["const"] is True

        # Verify then clause has all properties
        then_field = if_then["then"]["properties"]["web_search_tool"]
        assert then_field["title"] == "Web Search Engine"
        assert then_field["ui:widget"] == "GroupedIconSelectWidget"
        assert "enum" in then_field

        # Verify SchemaSplitter works
        _, ui_schema = SchemaSplitter.split(schema)
        assert "ui:order" in ui_schema
        assert ui_schema.get("web_search_tool_enabled", {}).get("ui:widget") == "checkbox"
        assert ui_schema.get("web_search_tool", {}).get("ui:widget") == "GroupedIconSelectWidget"
