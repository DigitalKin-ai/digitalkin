"""Tests for SchemaSplitter utility."""


from digitalkin.utils.schema_splitter import SchemaSplitter


class TestSchemaSplitter:
    """Test cases for SchemaSplitter class."""

    def test_split_basic_schema(self) -> None:
        """Test splitting a basic schema with ui:* properties."""
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string", "ui:widget": "text"},
                "email": {"type": "string", "ui:widget": "email"},
            },
        }

        json_schema, ui_schema = SchemaSplitter.split(schema)

        # JSON schema should not have ui:* properties
        assert "ui:widget" not in json_schema["properties"]["name"]
        assert "ui:widget" not in json_schema["properties"]["email"]

        # UI schema should have the ui:* properties
        assert ui_schema["name"]["ui:widget"] == "text"
        assert ui_schema["email"]["ui:widget"] == "email"

    def test_split_with_allof(self) -> None:
        """Test splitting a schema with allOf containing ui:* properties."""
        schema = {
            "type": "object",
            "properties": {"base": {"type": "string", "ui:widget": "text"}},
            "allOf": [
                {
                    "properties": {
                        "extended": {"type": "number", "ui:widget": "range"},
                    }
                }
            ],
        }

        _json_schema, ui_schema = SchemaSplitter.split(schema)

        assert ui_schema["base"]["ui:widget"] == "text"
        assert ui_schema["extended"]["ui:widget"] == "range"

    def test_split_with_conditional_ui_properties(self) -> None:
        """Test splitting schema with if/then/else conditional structures containing ui:* properties."""
        schema = {
            "type": "object",
            "properties": {"enabled": {"type": "boolean", "ui:widget": "checkbox"}},
            "allOf": [
                {
                    "if": {"properties": {"enabled": {"const": True}}},
                    "then": {
                        "properties": {
                            "option": {
                                "type": "string",
                                "ui:widget": "select",
                                "ui:options": {"groups": []},
                            }
                        }
                    },
                }
            ],
        }

        _json_schema, ui_schema = SchemaSplitter.split(schema)

        # ui:* from root properties should be extracted
        assert "enabled" in ui_schema
        assert ui_schema["enabled"]["ui:widget"] == "checkbox"

        # ui:* from then.properties should be extracted
        assert "option" in ui_schema
        assert ui_schema["option"]["ui:widget"] == "select"
        assert "ui:options" in ui_schema["option"]
        assert ui_schema["option"]["ui:options"] == {"groups": []}

    def test_split_with_else_conditional(self) -> None:
        """Test splitting schema with else conditional containing ui:* properties."""
        schema = {
            "type": "object",
            "properties": {"mode": {"type": "string"}},
            "allOf": [
                {
                    "if": {"properties": {"mode": {"const": "advanced"}}},
                    "then": {
                        "properties": {
                            "advanced_option": {"type": "string", "ui:widget": "textarea"}
                        }
                    },
                    "else": {
                        "properties": {
                            "simple_option": {"type": "string", "ui:widget": "text", "ui:placeholder": "Enter value"}
                        }
                    },
                }
            ],
        }

        _json_schema, ui_schema = SchemaSplitter.split(schema)

        # ui:* from then.properties
        assert "advanced_option" in ui_schema
        assert ui_schema["advanced_option"]["ui:widget"] == "textarea"

        # ui:* from else.properties
        assert "simple_option" in ui_schema
        assert ui_schema["simple_option"]["ui:widget"] == "text"
        assert ui_schema["simple_option"]["ui:placeholder"] == "Enter value"

    def test_split_with_nested_allof_conditionals(self) -> None:
        """Test splitting schema with nested allOf containing conditionals."""
        schema = {
            "type": "object",
            "properties": {
                "feature_enabled": {"type": "boolean", "ui:widget": "switch"}
            },
            "allOf": [
                {
                    "if": {"properties": {"feature_enabled": {"const": True}}},
                    "then": {
                        "properties": {
                            "feature_config": {
                                "type": "object",
                                "properties": {
                                    "setting": {"type": "string", "ui:widget": "select", "ui:options": {"enumNames": ["A", "B"]}}
                                }
                            }
                        }
                    },
                }
            ],
        }

        _json_schema, ui_schema = SchemaSplitter.split(schema)

        # ui:* from root
        assert ui_schema["feature_enabled"]["ui:widget"] == "switch"

        # ui:* from nested properties in then
        assert "feature_config" in ui_schema
        assert "setting" in ui_schema["feature_config"]
        assert ui_schema["feature_config"]["setting"]["ui:widget"] == "select"

    def test_split_preserves_json_schema_structure(self) -> None:
        """Test that JSON schema structure with if/then/else is preserved correctly."""
        schema = {
            "type": "object",
            "properties": {"enabled": {"type": "boolean"}},
            "allOf": [
                {
                    "if": {"properties": {"enabled": {"const": True}}, "required": ["enabled"]},
                    "then": {
                        "properties": {"option": {"type": "string", "ui:widget": "select"}},
                        "required": ["option"],
                    },
                }
            ],
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        # Verify JSON schema structure is preserved
        assert "allOf" in json_schema
        assert len(json_schema["allOf"]) == 1
        allof_item = json_schema["allOf"][0]
        assert "if" in allof_item
        assert "then" in allof_item
        assert allof_item["then"]["required"] == ["option"]

        # Verify ui:* is stripped from JSON schema
        assert "ui:widget" not in allof_item["then"]["properties"]["option"]

    def test_split_with_items_in_conditional(self) -> None:
        """Test splitting schema with array items inside conditionals."""
        schema = {
            "type": "object",
            "properties": {"use_list": {"type": "boolean"}},
            "allOf": [
                {
                    "if": {"properties": {"use_list": {"const": True}}},
                    "then": {
                        "properties": {
                            "items_list": {
                                "type": "array",
                                "items": {"type": "string", "ui:widget": "text"},
                                "ui:options": {"orderable": True},
                            }
                        }
                    },
                }
            ],
        }

        _json_schema, ui_schema = SchemaSplitter.split(schema)

        # ui:* from array items in then should be extracted
        assert "items_list" in ui_schema
        assert "items" in ui_schema["items_list"]
        assert ui_schema["items_list"]["items"]["ui:widget"] == "text"
        assert ui_schema["items_list"]["ui:options"] == {"orderable": True}

    def test_split_with_nested_ref_ui_properties(self) -> None:
        """Test that UI properties from $ref definitions are extracted."""
        schema = {
            "type": "object",
            "$defs": {
                "NestedModel": {
                    "type": "object",
                    "ui:order": ["field_a", "field_b"],
                    "properties": {
                        "field_a": {"type": "string", "ui:widget": "text"},
                        "field_b": {"type": "string", "ui:widget": "textarea"},
                    },
                }
            },
            "properties": {
                "top_level": {"type": "string", "ui:widget": "text"},
                "nested": {"$ref": "#/$defs/NestedModel"},
            },
        }

        _json_schema, ui_schema = SchemaSplitter.split(schema)

        # Top level UI props should be extracted
        assert ui_schema["top_level"]["ui:widget"] == "text"

        # Nested $ref UI props should also be extracted
        assert "nested" in ui_schema
        assert ui_schema["nested"]["ui:order"] == ["field_a", "field_b"]
        assert ui_schema["nested"]["field_a"]["ui:widget"] == "text"
        assert ui_schema["nested"]["field_b"]["ui:widget"] == "textarea"

    def test_split_with_deeply_nested_ref_ui_properties(self) -> None:
        """Test that UI properties from deeply nested $ref definitions are extracted."""
        schema = {
            "type": "object",
            "$defs": {
                "InnerModel": {
                    "type": "object",
                    "ui:order": ["inner_field"],
                    "properties": {
                        "inner_field": {"type": "string", "ui:widget": "password"},
                    },
                },
                "OuterModel": {
                    "type": "object",
                    "ui:order": ["outer_field", "inner"],
                    "properties": {
                        "outer_field": {"type": "string", "ui:widget": "email"},
                        "inner": {"$ref": "#/$defs/InnerModel"},
                    },
                },
            },
            "properties": {
                "root_field": {"type": "string", "ui:widget": "text"},
                "outer": {"$ref": "#/$defs/OuterModel"},
            },
        }

        _json_schema, ui_schema = SchemaSplitter.split(schema)

        # Root level UI props
        assert ui_schema["root_field"]["ui:widget"] == "text"

        # Outer model UI props
        assert "outer" in ui_schema
        assert ui_schema["outer"]["ui:order"] == ["outer_field", "inner"]
        assert ui_schema["outer"]["outer_field"]["ui:widget"] == "email"

        # Inner model UI props (nested $ref)
        assert "inner" in ui_schema["outer"]
        assert ui_schema["outer"]["inner"]["ui:order"] == ["inner_field"]
        assert ui_schema["outer"]["inner"]["inner_field"]["ui:widget"] == "password"
