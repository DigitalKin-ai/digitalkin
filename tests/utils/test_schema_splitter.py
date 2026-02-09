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

    def test_split_with_non_dict_property_value(self) -> None:
        """Test that non-dict property values are passed through unchanged."""
        schema = {
            "type": "object",
            "properties": {
                "normal": {"type": "string", "ui:widget": "text"},
                "scalar": "string",
            },
        }

        json_schema, ui_schema = SchemaSplitter.split(schema)

        assert json_schema["properties"]["scalar"] == "string"
        assert ui_schema["normal"]["ui:widget"] == "text"

    def test_split_with_non_dict_defs_value(self) -> None:
        """Test that non-dict $defs values are passed through."""
        schema = {
            "type": "object",
            "$defs": {
                "MyModel": {"type": "object", "properties": {"a": {"type": "string"}}},
                "Alias": "string",
            },
            "properties": {"field": {"type": "string"}},
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        assert json_schema["$defs"]["Alias"] == "string"
        assert "MyModel" in json_schema["$defs"]

    def test_split_with_root_level_items(self) -> None:
        """Test splitting schema with items at root level (array schema)."""
        schema = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "ui:widget": "text"},
                },
            },
        }

        json_schema, ui_schema = SchemaSplitter.split(schema)

        assert json_schema["type"] == "array"
        assert "ui:widget" not in json_schema["items"]["properties"]["name"]
        assert ui_schema["items"]["name"]["ui:widget"] == "text"

    def test_split_with_root_level_oneof(self) -> None:
        """Test splitting schema with oneOf at root level."""
        schema = {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string", "ui:widget": "textarea"},
                    },
                },
                {
                    "type": "object",
                    "properties": {
                        "number": {"type": "integer", "ui:widget": "range"},
                    },
                },
            ],
        }

        json_schema, ui_schema = SchemaSplitter.split(schema)

        assert len(json_schema["oneOf"]) == 2
        assert "ui:widget" not in json_schema["oneOf"][0]["properties"]["text"]
        assert "ui:widget" not in json_schema["oneOf"][1]["properties"]["number"]
        assert len(ui_schema["oneOf"]) == 2
        assert ui_schema["oneOf"][0]["text"]["ui:widget"] == "textarea"
        assert ui_schema["oneOf"][1]["number"]["ui:widget"] == "range"

    def test_split_with_root_level_anyof(self) -> None:
        """Test splitting schema with anyOf at root level."""
        schema = {
            "anyOf": [
                {"type": "string", "ui:widget": "text"},
                {"type": "integer"},
            ],
        }

        json_schema, ui_schema = SchemaSplitter.split(schema)

        assert len(json_schema["anyOf"]) == 2
        assert "ui:widget" not in json_schema["anyOf"][0]
        assert len(ui_schema["anyOf"]) == 2
        assert ui_schema["anyOf"][0]["ui:widget"] == "text"

    def test_split_with_non_dict_allof_item(self) -> None:
        """Test that non-dict allOf items are preserved."""
        schema = {
            "type": "object",
            "allOf": [
                {"properties": {"a": {"type": "string"}}},
                True,
            ],
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        assert len(json_schema["allOf"]) == 2
        assert json_schema["allOf"][1] is True

    def test_split_with_non_dict_oneof_item(self) -> None:
        """Test that non-dict oneOf items are preserved."""
        schema = {
            "oneOf": [
                {"type": "string"},
                True,
            ],
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        assert json_schema["oneOf"][1] is True

    def test_split_with_root_level_if_then_else(self) -> None:
        """Test splitting schema with if/then/else at root level (not inside allOf)."""
        schema = {
            "type": "object",
            "properties": {"mode": {"type": "string"}},
            "if": {"properties": {"mode": {"const": "advanced"}}},
            "then": {
                "properties": {
                    "detail": {"type": "string", "ui:widget": "textarea"},
                }
            },
            "else": {
                "properties": {
                    "simple": {"type": "string", "ui:widget": "text"},
                }
            },
        }

        json_schema, ui_schema = SchemaSplitter.split(schema)

        assert "if" in json_schema
        assert "then" in json_schema
        assert "else" in json_schema
        assert "ui:widget" not in json_schema["then"]["properties"]["detail"]
        assert "ui:widget" not in json_schema["else"]["properties"]["simple"]
        assert ui_schema["detail"]["ui:widget"] == "textarea"
        assert ui_schema["simple"]["ui:widget"] == "text"

    def test_split_property_with_nested_properties(self) -> None:
        """Test _process_property handles nested properties within a property."""
        schema = {
            "type": "object",
            "properties": {
                "nested_obj": {
                    "type": "object",
                    "properties": {
                        "inner": {"type": "string", "ui:widget": "text"},
                        "plain": "string",
                    },
                },
            },
        }

        json_schema, ui_schema = SchemaSplitter.split(schema)

        assert "ui:widget" not in json_schema["properties"]["nested_obj"]["properties"]["inner"]
        assert json_schema["properties"]["nested_obj"]["properties"]["plain"] == "string"
        assert ui_schema["nested_obj"]["inner"]["ui:widget"] == "text"

    def test_split_property_with_items(self) -> None:
        """Test _process_property handles items within a property."""
        schema = {
            "type": "object",
            "properties": {
                "my_list": {
                    "type": "array",
                    "items": {"type": "string", "ui:widget": "text"},
                },
            },
        }

        json_schema, ui_schema = SchemaSplitter.split(schema)

        assert "ui:widget" not in json_schema["properties"]["my_list"]["items"]
        assert ui_schema["my_list"]["items"]["ui:widget"] == "text"

    def test_split_property_with_oneof(self) -> None:
        """Test _process_property handles oneOf within a property."""
        schema = {
            "type": "object",
            "$defs": {
                "OptionA": {"type": "object", "ui:order": ["a"], "properties": {"a": {"type": "string"}}},
            },
            "properties": {
                "choice": {
                    "oneOf": [
                        {"$ref": "#/$defs/OptionA"},
                        {"type": "string", "ui:widget": "text"},
                    ],
                },
            },
        }

        json_schema, ui_schema = SchemaSplitter.split(schema)

        assert len(json_schema["properties"]["choice"]["oneOf"]) == 2
        assert "ui:widget" not in json_schema["properties"]["choice"]["oneOf"][1]
        assert len(ui_schema["choice"]["oneOf"]) == 2
        # First item resolves $ref UI from defs_ui
        assert ui_schema["choice"]["oneOf"][0]["ui:order"] == ["a"]

    def test_split_property_with_anyof_non_dict(self) -> None:
        """Test _process_property handles non-dict items in anyOf."""
        schema = {
            "type": "object",
            "properties": {
                "val": {
                    "anyOf": [
                        {"type": "string"},
                        True,
                    ],
                },
            },
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        assert json_schema["properties"]["val"]["anyOf"][1] is True

    def test_strip_ui_with_defs(self) -> None:
        """Test _strip_ui_properties handles $defs with dict and non-dict values."""
        schema = {
            "type": "object",
            "allOf": [
                {
                    "$defs": {
                        "Inner": {"type": "object", "ui:order": ["x"], "properties": {"x": {"type": "string"}}},
                        "Scalar": "string",
                    },
                    "properties": {"a": {"type": "string"}},
                },
            ],
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        # allOf items go through _strip_ui_properties
        stripped = json_schema["allOf"][0]
        assert "ui:order" not in stripped["$defs"]["Inner"]
        assert stripped["$defs"]["Scalar"] == "string"

    def test_strip_ui_with_items(self) -> None:
        """Test _strip_ui_properties handles items."""
        schema = {
            "type": "object",
            "allOf": [
                {
                    "type": "array",
                    "items": {"type": "string", "ui:widget": "text"},
                },
            ],
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        stripped = json_schema["allOf"][0]
        assert "ui:widget" not in stripped["items"]

    def test_strip_ui_with_allof(self) -> None:
        """Test _strip_ui_properties handles nested allOf."""
        schema = {
            "type": "object",
            "allOf": [
                {
                    "allOf": [
                        {"properties": {"x": {"type": "string", "ui:widget": "text"}}},
                        True,
                    ],
                },
            ],
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        nested_allof = json_schema["allOf"][0]["allOf"]
        assert "ui:widget" not in nested_allof[0]["properties"]["x"]
        assert nested_allof[1] is True

    def test_strip_ui_with_oneof(self) -> None:
        """Test _strip_ui_properties handles oneOf/anyOf."""
        schema = {
            "type": "object",
            "allOf": [
                {
                    "oneOf": [
                        {"type": "string", "ui:widget": "text"},
                        True,
                    ],
                },
            ],
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        nested_oneof = json_schema["allOf"][0]["oneOf"]
        assert "ui:widget" not in nested_oneof[0]
        assert nested_oneof[1] is True

    def test_strip_ui_with_if_then_else(self) -> None:
        """Test _strip_ui_properties handles if/then/else."""
        schema = {
            "type": "object",
            "allOf": [
                {
                    "if": {"properties": {"x": {"const": True}}},
                    "then": {"properties": {"y": {"type": "string", "ui:widget": "text"}}},
                    "else": {"properties": {"z": {"type": "string", "ui:widget": "range"}}},
                },
            ],
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        stripped = json_schema["allOf"][0]
        assert "ui:widget" not in stripped["then"]["properties"]["y"]
        assert "ui:widget" not in stripped["else"]["properties"]["z"]

    def test_strip_ui_with_non_dict_property(self) -> None:
        """Test _strip_ui_properties handles non-dict property values."""
        schema = {
            "type": "object",
            "allOf": [
                {
                    "properties": {
                        "normal": {"type": "string"},
                        "scalar": "string",
                    },
                },
            ],
        }

        json_schema, _ui_schema = SchemaSplitter.split(schema)

        stripped = json_schema["allOf"][0]
        assert stripped["properties"]["scalar"] == "string"

    def test_extract_ui_with_oneof_in_defs(self) -> None:
        """Test _extract_ui_properties handles oneOf inside $defs."""
        schema = {
            "type": "object",
            "$defs": {
                "Choice": {
                    "oneOf": [
                        {"type": "string", "ui:widget": "text"},
                        {"type": "integer", "ui:widget": "number"},
                    ],
                },
            },
            "properties": {
                "pick": {"$ref": "#/$defs/Choice"},
            },
        }

        _json_schema, ui_schema = SchemaSplitter.split(schema)

        # The $ref resolves and pulls UI from Choice def
        assert "pick" in ui_schema
        assert "oneOf" in ui_schema["pick"]
        assert ui_schema["pick"]["oneOf"][0]["ui:widget"] == "text"
        assert ui_schema["pick"]["oneOf"][1]["ui:widget"] == "number"

    def test_extract_ui_with_items_in_defs(self) -> None:
        """Test _extract_ui_properties handles items inside $defs."""
        schema = {
            "type": "object",
            "$defs": {
                "MyList": {
                    "type": "array",
                    "items": {"type": "string", "ui:widget": "text"},
                },
            },
            "properties": {
                "list_field": {"$ref": "#/$defs/MyList"},
            },
        }

        _json_schema, ui_schema = SchemaSplitter.split(schema)

        assert "list_field" in ui_schema
        assert "items" in ui_schema["list_field"]
        assert ui_schema["list_field"]["items"]["ui:widget"] == "text"
