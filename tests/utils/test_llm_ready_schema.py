"""Tests for llm_ready_schema utility."""

from pydantic import BaseModel, Field

from digitalkin.utils.llm_ready_schema import CustomOrderSchema, inline_refs, llm_ready_schema


class TestCustomOrderSchema:
    """Tests for CustomOrderSchema.sort."""

    def test_sort_orders_preferred_keys_first(self) -> None:
        """Preferred keys appear in specified order before others."""
        gen = CustomOrderSchema({})
        result = gen.sort({
            "properties": {"a": {"type": "string"}},
            "type": "object",
            "title": "Test",
            "extra": "value",
            "description": "A test",
        })
        keys = list(result.keys())
        assert keys.index("title") < keys.index("description")
        assert keys.index("description") < keys.index("type")
        assert keys.index("type") < keys.index("properties")
        assert keys.index("properties") < keys.index("extra")

    def test_sort_recurses_into_dicts(self) -> None:
        """Nested dicts are also sorted."""
        gen = CustomOrderSchema({})
        result = gen.sort({
            "properties": {
                "field": {
                    "type": "string",
                    "title": "Field",
                }
            },
        })
        inner = result["properties"]["field"]
        keys = list(inner.keys())
        assert keys.index("title") < keys.index("type")

    def test_sort_recurses_into_lists(self) -> None:
        """List items are sorted recursively."""
        gen = CustomOrderSchema({})
        result = gen.sort([
            {"type": "string", "title": "A"},
            {"description": "B", "type": "int"},
        ])
        assert list(result[0].keys())[0] == "title"
        assert list(result[1].keys())[0] == "description"

    def test_sort_scalar_passthrough(self) -> None:
        """Scalars are returned unchanged."""
        gen = CustomOrderSchema({})
        assert gen.sort("hello") == "hello"
        assert gen.sort(42) == 42
        assert gen.sort(True) is True
        assert gen.sort(None) is None

    def test_sort_missing_preferred_keys_skipped(self) -> None:
        """Preferred keys that don't exist in the dict are skipped."""
        gen = CustomOrderSchema({})
        result = gen.sort({"extra": 1, "type": "string"})
        keys = list(result.keys())
        assert keys == ["type", "extra"]


class TestInlineRefs:
    """Tests for inline_refs."""

    def test_inline_simple_ref(self) -> None:
        """Resolves a simple $ref to its definition."""
        schema = {
            "$defs": {
                "Inner": {"type": "object", "properties": {"x": {"type": "string"}}},
            },
            "type": "object",
            "properties": {
                "nested": {"$ref": "#/$defs/Inner"},
            },
        }
        result = inline_refs(schema)
        assert "$defs" not in result
        assert result["properties"]["nested"]["type"] == "object"
        assert result["properties"]["nested"]["properties"]["x"]["type"] == "string"

    def test_inline_nested_refs(self) -> None:
        """Resolves nested $ref chains."""
        schema = {
            "$defs": {
                "Leaf": {"type": "string"},
                "Branch": {"type": "object", "properties": {"leaf": {"$ref": "#/$defs/Leaf"}}},
            },
            "properties": {"branch": {"$ref": "#/$defs/Branch"}},
        }
        result = inline_refs(schema)
        assert result["properties"]["branch"]["properties"]["leaf"]["type"] == "string"

    def test_inline_refs_in_lists(self) -> None:
        """Resolves $ref inside list items (e.g. oneOf)."""
        schema = {
            "$defs": {
                "TypeA": {"type": "string"},
                "TypeB": {"type": "integer"},
            },
            "oneOf": [
                {"$ref": "#/$defs/TypeA"},
                {"$ref": "#/$defs/TypeB"},
            ],
        }
        result = inline_refs(schema)
        assert result["oneOf"][0]["type"] == "string"
        assert result["oneOf"][1]["type"] == "integer"

    def test_inline_no_refs(self) -> None:
        """Schema without $ref is returned unchanged (minus $defs)."""
        schema = {"type": "object", "properties": {"a": {"type": "string"}}}
        result = inline_refs(schema)
        assert result == schema

    def test_inline_does_not_mutate_original(self) -> None:
        """Original schema is not modified."""
        schema = {
            "$defs": {"X": {"type": "string"}},
            "properties": {"field": {"$ref": "#/$defs/X"}},
        }
        inline_refs(schema)
        assert "$defs" in schema
        assert "$ref" in schema["properties"]["field"]

    def test_inline_scalar_values(self) -> None:
        """Scalar values in schema are preserved."""
        schema = {
            "type": "object",
            "title": "Test",
            "required": ["a"],
            "properties": {"a": {"type": "string"}},
        }
        result = inline_refs(schema)
        assert result["title"] == "Test"
        assert result["required"] == ["a"]


class TestLlmReadySchema:
    """Tests for llm_ready_schema end-to-end."""

    def test_simple_model(self) -> None:
        """Generates clean schema for a simple model."""

        class SimpleModel(BaseModel):
            name: str = Field(description="The name")
            age: int = Field(default=0)

        result = llm_ready_schema(SimpleModel)
        assert "properties" in result
        assert "name" in result["properties"]
        assert "age" in result["properties"]
        assert "$defs" not in result

    def test_nested_model_refs_inlined(self) -> None:
        """Nested model $ref are resolved inline."""

        class Inner(BaseModel):
            value: str = "default"

        class Outer(BaseModel):
            inner: Inner

        result = llm_ready_schema(Outer)
        assert "$defs" not in result
        # Inner model should be inlined into properties
        inner_schema = result["properties"]["inner"]
        assert "properties" in inner_schema
        assert "value" in inner_schema["properties"]

    def test_key_ordering(self) -> None:
        """Keys are ordered with title/description/type before properties."""

        class OrderedModel(BaseModel):
            """A model."""

            field: str = "value"

        result = llm_ready_schema(OrderedModel)
        keys = list(result.keys())
        # title should come before type, type before properties
        if "title" in keys and "type" in keys:
            assert keys.index("title") < keys.index("type")
        if "type" in keys and "properties" in keys:
            assert keys.index("type") < keys.index("properties")

    def test_list_field_model(self) -> None:
        """Models with list fields produce valid schemas."""

        class Item(BaseModel):
            name: str

        class Container(BaseModel):
            items: list[Item]

        result = llm_ready_schema(Container)
        assert "$defs" not in result
        items_schema = result["properties"]["items"]
        assert items_schema["type"] == "array"
        assert "properties" in items_schema["items"]
