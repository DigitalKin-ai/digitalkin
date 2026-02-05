"""Integration tests for SetupModel with dynamic schema support."""

from typing import Annotated

import pytest
from pydantic import BaseModel, ConfigDict, Field

from digitalkin.models.module.module_types import SetupModel
from digitalkin.models.module.tool_reference import tool_reference_input
from digitalkin.utils import Dynamic
from digitalkin.utils.dynamic_schema import has_dynamic


class TestSetupModelGetCleanModel:
    """Tests for SetupModel.get_clean_model() with force parameter."""

    @pytest.mark.asyncio
    async def test_get_clean_model_without_force_basic(self) -> None:
        """Test basic get_clean_model without force parameter."""

        class TestSetup(SetupModel):
            name: str = Field(default="test")
            value: int = Field(default=42)

        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True)

        assert "name" in model.model_fields
        assert "value" in model.model_fields

    @pytest.mark.asyncio
    async def test_get_clean_model_filters_config_fields(self) -> None:
        """Test that config fields are properly filtered."""

        class TestSetup(SetupModel):
            normal_field: str = Field(default="normal")
            config_field: str = Field(
                default="config",
                json_schema_extra={"config": True},
            )

        # Without config_fields=True, config field should be excluded
        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True)
        assert "normal_field" in model.model_fields
        assert "config_field" not in model.model_fields

        # With config_fields=True, config field should be included
        model_with_config = await TestSetup.get_clean_model(config_fields=True, hidden_fields=False)
        assert "config_field" in model_with_config.model_fields

    @pytest.mark.asyncio
    async def test_get_clean_model_filters_hidden_fields(self) -> None:
        """Test that hidden fields are properly filtered."""

        class TestSetup(SetupModel):
            visible_field: str = Field(default="visible")
            hidden_field: str = Field(
                default="hidden",
                json_schema_extra={"hidden": True},
            )

        # Without hidden_fields=True, hidden field should be excluded
        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=False)
        assert "visible_field" in model.model_fields
        assert "hidden_field" not in model.model_fields

        # With hidden_fields=True, hidden field should be included
        model_with_hidden = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True)
        assert "hidden_field" in model_with_hidden.model_fields

    @pytest.mark.asyncio
    async def test_get_clean_model_with_force_sync_fetcher(self) -> None:
        """Test that force=True calls sync fetcher."""
        call_count = 0

        def sync_fetcher() -> list[str]:
            nonlocal call_count
            call_count += 1
            return ["model1", "model2", "model3"]

        class TestSetup(SetupModel):
            model_name: Annotated[str, Dynamic(enum=sync_fetcher)] = Field(default="model1")

        # Call with force=True
        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        # Fetcher should have been called
        assert call_count == 1

        # Check that the field's json_schema_extra has the resolved enum
        field_info = model.model_fields["model_name"]
        extra = field_info.json_schema_extra

        # Enum should be resolved in json_schema_extra
        assert extra["enum"] == ["model1", "model2", "model3"]

        # Dynamic metadata should be removed after resolution
        assert not has_dynamic(field_info)

    @pytest.mark.asyncio
    async def test_get_clean_model_with_force_async_fetcher(self) -> None:
        """Test that force=True calls async fetcher."""

        async def async_fetcher() -> list[str]:
            return ["async_opt1", "async_opt2"]

        class TestSetup(SetupModel):
            option: Annotated[str, Dynamic(enum=async_fetcher)] = Field(default="async_opt1")

        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        field_info = model.model_fields["option"]
        extra = field_info.json_schema_extra

        assert extra["enum"] == ["async_opt1", "async_opt2"]
        assert not has_dynamic(field_info)

    @pytest.mark.asyncio
    async def test_get_clean_model_force_false_preserves_fetchers(self) -> None:
        """Test that force=False doesn't call fetchers."""
        call_count = 0

        def fetcher() -> list[str]:
            nonlocal call_count
            call_count += 1
            return ["a", "b"]

        class TestSetup(SetupModel):
            field: Annotated[str, Dynamic(enum=fetcher)] = Field(default="a")

        await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=False)

        # Fetcher should NOT have been called
        assert call_count == 0

    @pytest.mark.asyncio
    async def test_get_clean_model_fetcher_error_graceful_fallback(self) -> None:
        """Test that fetcher errors are handled gracefully."""

        def failing_fetcher() -> list[str]:
            msg = "Fetcher failed"
            raise RuntimeError(msg)

        class TestSetup(SetupModel):
            field: Annotated[str, Dynamic(enum=failing_fetcher)] = Field(default="default")

        # Should not raise, should handle gracefully
        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        assert model is not None
        assert "field" in model.model_fields

    @pytest.mark.asyncio
    async def test_get_clean_model_multiple_dynamic_fields(self) -> None:
        """Test that multiple dynamic fields are all refreshed."""
        calls: list[str] = []

        def fetcher_a() -> list[str]:
            calls.append("a")
            return ["opt_a1", "opt_a2"]

        def fetcher_b() -> list[str]:
            calls.append("b")
            return ["opt_b1", "opt_b2"]

        class TestSetup(SetupModel):
            field_a: Annotated[str, Dynamic(enum=fetcher_a)] = Field(default="opt_a1")
            field_b: Annotated[str, Dynamic(enum=fetcher_b)] = Field(default="opt_b1")

        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        # Both fetchers should have been called
        assert "a" in calls
        assert "b" in calls

        # Both fields should have resolved enums
        assert model.model_fields["field_a"].json_schema_extra["enum"] == ["opt_a1", "opt_a2"]
        assert model.model_fields["field_b"].json_schema_extra["enum"] == ["opt_b1", "opt_b2"]

    @pytest.mark.asyncio
    async def test_get_clean_model_preserves_static_with_dynamic(self) -> None:
        """Test that static values are preserved alongside dynamic ones."""

        class TestSetup(SetupModel):
            field: Annotated[str, Dynamic(enum=lambda: ["opt1", "opt2"])] = Field(
                default="opt1",
                json_schema_extra={
                    "config": True,
                    "ui:widget": "dropdown",
                },
            )

        model = await TestSetup.get_clean_model(config_fields=True, hidden_fields=False, force=True)

        field_info = model.model_fields["field"]
        extra = field_info.json_schema_extra

        # Static values should be preserved
        assert extra["config"] is True
        assert extra["ui:widget"] == "dropdown"
        # Dynamic value should be resolved
        assert extra["enum"] == ["opt1", "opt2"]
        # Dynamic metadata should be removed
        assert not has_dynamic(field_info)

    @pytest.mark.asyncio
    async def test_get_clean_model_preserves_other_field_attributes(self) -> None:
        """Test that other FieldInfo attributes are preserved during refresh."""

        class TestSetup(SetupModel):
            field: Annotated[str, Dynamic(enum=lambda: ["a", "b"])] = Field(
                default="default_value",
                title="My Field",
                description="A test field",
            )

        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        field_info = model.model_fields["field"]

        # Core attributes should be preserved
        assert field_info.default == "default_value"
        assert field_info.title == "My Field"
        assert field_info.description == "A test field"


class TestSetupModelSchema:
    """Tests for schema generation with dynamic fields."""

    @pytest.mark.asyncio
    async def test_schema_contains_resolved_enum(self) -> None:
        """Test that the generated schema contains resolved enum values."""

        class TestSetup(SetupModel):
            model_name: Annotated[str, Dynamic(enum=lambda: ["gpt-4", "gpt-3.5", "claude"])] = Field(default="gpt-4")

        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        schema = model.model_json_schema()

        # The schema should contain the enum values
        assert "properties" in schema
        assert "model_name" in schema["properties"]
        assert "enum" in schema["properties"]["model_name"]
        assert schema["properties"]["model_name"]["enum"] == ["gpt-4", "gpt-3.5", "claude"]

    @pytest.mark.asyncio
    async def test_schema_without_force_no_enum(self) -> None:
        """Test that schema without force doesn't call fetchers."""
        call_count = 0

        def fetcher() -> list[str]:
            nonlocal call_count
            call_count += 1
            return ["gpt-4", "gpt-3.5"]

        class TestSetup(SetupModel):
            model_name: Annotated[str, Dynamic(enum=fetcher)] = Field(default="gpt-4")

        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=False)

        # Fetcher should NOT have been called
        assert call_count == 0

        # The field should still have Dynamic metadata (not resolved)
        field_info = model.model_fields["model_name"]
        assert has_dynamic(field_info)


class TestNestedSetupModels:
    """Tests for nested SetupModel structures with dynamic fields."""

    @pytest.mark.asyncio
    async def test_nested_model_with_dynamic_field(self) -> None:
        """Test that nested models with dynamic fields work correctly."""

        class NestedConfig(BaseModel):
            nested_option: Annotated[str, Dynamic(enum=lambda: ["opt1", "opt2"])] = Field(default="opt1")

        class TestSetup(SetupModel):
            name: str = Field(default="test")
            config: NestedConfig = Field(default_factory=NestedConfig)

        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        # Top-level fields should be present
        assert "name" in model.model_fields
        assert "config" in model.model_fields

        # Nested model's dynamic fields ARE refreshed by get_clean_model
        # when force=True. The config field should have the refreshed nested model.
        config_field = model.model_fields["config"]
        nested_annotation = config_field.annotation
        # The nested model's dynamic field should be resolved
        if hasattr(nested_annotation, "model_fields"):
            nested_field = nested_annotation.model_fields.get("nested_option")
            if nested_field:
                assert not has_dynamic(nested_field), "Nested dynamic field should be resolved"

    @pytest.mark.asyncio
    async def test_nested_model_refreshed_with_force(self) -> None:
        """Test that nested BaseModel dynamic fields are refreshed with force=True."""
        call_count = 0

        def nested_fetcher() -> list[str]:
            nonlocal call_count
            call_count += 1
            return ["nested_a", "nested_b"]

        class NestedConfig(BaseModel):
            nested_option: Annotated[str, Dynamic(enum=nested_fetcher)] = Field(default="nested_a")

        class TestSetup(SetupModel):
            config: NestedConfig = Field(default_factory=NestedConfig)

        model = await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        # The fetcher should have been called
        assert call_count == 1

        # The nested model's field should have resolved enum
        config_field = model.model_fields["config"]
        nested_model = config_field.annotation
        nested_field = nested_model.model_fields["nested_option"]
        assert nested_field.json_schema_extra["enum"] == ["nested_a", "nested_b"]
        assert not has_dynamic(nested_field)


class TestGenericTypeDetection:
    """Tests for generic type detection in SetupModel._get_base_model_type."""

    def test_detects_direct_base_model(self) -> None:
        """Test detection of direct BaseModel subclass."""

        class MyModel(BaseModel):
            field: str = "test"

        result = SetupModel._get_base_model_type(MyModel)
        assert result is MyModel

    def test_detects_optional_base_model(self) -> None:
        """Test detection of Optional[BaseModel]."""

        class MyModel(BaseModel):
            field: str = "test"

        result = SetupModel._get_base_model_type(MyModel | None)
        assert result is MyModel

    def test_detects_list_of_base_model(self) -> None:
        """Test detection of list[BaseModel]."""

        class MyModel(BaseModel):
            field: str = "test"

        result = SetupModel._get_base_model_type(list[MyModel])
        assert result is MyModel

    def test_detects_dict_value_base_model(self) -> None:
        """Test detection of dict[str, BaseModel]."""

        class MyModel(BaseModel):
            field: str = "test"

        result = SetupModel._get_base_model_type(dict[str, MyModel])
        assert result is MyModel

    def test_detects_set_of_base_model(self) -> None:
        """Test detection of set[BaseModel]."""

        class MyModel(BaseModel):
            field: str = "test"

        result = SetupModel._get_base_model_type(set[MyModel])
        assert result is MyModel

    def test_detects_tuple_of_base_model(self) -> None:
        """Test detection of tuple[BaseModel, ...]."""

        class MyModel(BaseModel):
            field: str = "test"

        result = SetupModel._get_base_model_type(tuple[MyModel, ...])
        assert result is MyModel

    def test_returns_none_for_plain_types(self) -> None:
        """Test returns None for non-BaseModel types."""
        assert SetupModel._get_base_model_type(str) is None
        assert SetupModel._get_base_model_type(int) is None
        assert SetupModel._get_base_model_type(list[str]) is None
        assert SetupModel._get_base_model_type(dict[str, int]) is None

    def test_returns_none_for_none_type(self) -> None:
        """Test returns None for None."""
        assert SetupModel._get_base_model_type(None) is None

    def test_detects_union_with_base_model(self) -> None:
        """Test detection of Union containing BaseModel."""

        class MyModel(BaseModel):
            field: str = "test"

        result = SetupModel._get_base_model_type(str | MyModel)
        assert result is MyModel

    @pytest.mark.asyncio
    async def test_list_nested_model_refreshed(self) -> None:
        """Test that list[BaseModel] with dynamic fields gets refreshed."""
        call_count = 0

        def item_fetcher() -> list[str]:
            nonlocal call_count
            call_count += 1
            return ["item_a", "item_b"]

        class ItemConfig(BaseModel):
            item_type: Annotated[str, Dynamic(enum=item_fetcher)] = Field(default="item_a")

        class TestSetup(SetupModel):
            items: list[ItemConfig] = Field(default_factory=list)

        await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        # The nested fetcher should have been called
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_optional_nested_model_refreshed(self) -> None:
        """Test that Optional[BaseModel] with dynamic fields gets refreshed."""
        call_count = 0

        def optional_fetcher() -> list[str]:
            nonlocal call_count
            call_count += 1
            return ["opt_a", "opt_b"]

        class OptionalConfig(BaseModel):
            opt_field: Annotated[str, Dynamic(enum=optional_fetcher)] = Field(default="opt_a")

        class TestSetup(SetupModel):
            optional_config: OptionalConfig | None = Field(default=None)

        await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        # The nested fetcher should have been called
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_dict_value_nested_model_refreshed(self) -> None:
        """Test that dict[str, BaseModel] with dynamic fields gets refreshed."""
        call_count = 0

        def dict_value_fetcher() -> list[str]:
            nonlocal call_count
            call_count += 1
            return ["val_a", "val_b"]

        class ValueConfig(BaseModel):
            value_type: Annotated[str, Dynamic(enum=dict_value_fetcher)] = Field(default="val_a")

        class TestSetup(SetupModel):
            configs: dict[str, ValueConfig] = Field(default_factory=dict)

        await TestSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        # The nested fetcher should have been called
        assert call_count == 1


class TestNestedModelUiOrder:
    """Tests for ui:order preservation in nested models."""

    @pytest.mark.asyncio
    async def test_nested_model_preserves_own_ui_order(self) -> None:
        """Test that nested model's ui:order is preserved, not overwritten by parent's."""

        class NestedConfig(BaseModel):
            """A nested configuration with its own ui:order."""

            model_config = ConfigDict(
                json_schema_extra={
                    "ui:order": ["field_b", "field_a"],
                }
            )

            field_a: str = Field(default="a")
            field_b: Annotated[str, Dynamic(enum=lambda: ["opt1", "opt2"])] = Field(default="opt1")

        class ParentSetup(SetupModel):
            """Parent setup with a different ui:order."""

            model_config = ConfigDict(
                json_schema_extra={
                    "ui:order": ["name", "config"],
                }
            )

            name: str = Field(default="test")
            config: NestedConfig = Field(default_factory=NestedConfig)

        model = await ParentSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        # Parent's ui:order should be preserved
        parent_extra = model.model_config.get("json_schema_extra", {})
        assert parent_extra.get("ui:order") == ["name", "config"]

        # Nested model's ui:order should be preserved (not parent's)
        config_field = model.model_fields["config"]
        nested_model = config_field.annotation
        nested_extra = nested_model.model_config.get("json_schema_extra", {})
        assert nested_extra.get("ui:order") == ["field_b", "field_a"], (
            "Nested model should preserve its own ui:order, not inherit parent's"
        )

    @pytest.mark.asyncio
    async def test_deeply_nested_model_ui_order_preserved(self) -> None:
        """Test that deeply nested models preserve their ui:order at all levels."""

        class DeepNested(BaseModel):
            """Deepest level config."""

            model_config = ConfigDict(
                json_schema_extra={
                    "ui:order": ["deep_z", "deep_y"],
                }
            )

            deep_y: str = Field(default="y")
            deep_z: Annotated[str, Dynamic(enum=lambda: ["z1", "z2"])] = Field(default="z1")

        class MiddleNested(BaseModel):
            """Middle level config."""

            model_config = ConfigDict(
                json_schema_extra={
                    "ui:order": ["middle_b", "deep_config"],
                }
            )

            deep_config: DeepNested = Field(default_factory=DeepNested)
            middle_b: Annotated[str, Dynamic(enum=lambda: ["m1", "m2"])] = Field(default="m1")

        class RootSetup(SetupModel):
            """Root setup model."""

            model_config = ConfigDict(
                json_schema_extra={
                    "ui:order": ["root_field", "middle_config"],
                }
            )

            root_field: str = Field(default="root")
            middle_config: MiddleNested = Field(default_factory=MiddleNested)

        model = await RootSetup.get_clean_model(config_fields=False, hidden_fields=True, force=True)

        # Check root level ui:order
        root_extra = model.model_config.get("json_schema_extra", {})
        assert root_extra.get("ui:order") == ["root_field", "middle_config"]

        # Check middle level ui:order
        middle_model = model.model_fields["middle_config"].annotation
        middle_extra = middle_model.model_config.get("json_schema_extra", {})
        assert middle_extra.get("ui:order") == ["middle_b", "deep_config"]

        # Check deep level ui:order
        deep_model = middle_model.model_fields["deep_config"].annotation
        deep_extra = deep_model.model_config.get("json_schema_extra", {})
        assert deep_extra.get("ui:order") == ["deep_z", "deep_y"]


class TestCleanModelSchemaIsolation:
    """Tests that get_clean_model produces schemas without SetupModel internals."""

    @pytest.mark.asyncio
    async def test_clean_model_schema_excludes_resolved_tools(self) -> None:
        """Clean model schema should not contain resolved_tools or its type defs."""

        class ToolSetup(SetupModel):
            enabled: bool = Field(default=True)

        model = await ToolSetup.get_clean_model(config_fields=False, hidden_fields=False)
        schema = model.model_json_schema()

        assert "resolved_tools" not in schema.get("properties", {})
        defs = schema.get("$defs", {})
        assert "ToolModuleInfo" not in defs
        assert "ToolDefinition" not in defs
        assert "ToolParameter" not in defs
        assert "RegistryModuleType" not in defs
        assert "RegistryModuleStatus" not in defs

    @pytest.mark.asyncio
    async def test_clean_model_schema_only_contains_declared_fields(self) -> None:
        """Clean model schema properties should match declared fields only."""

        class SimpleSetup(SetupModel):
            name: str = Field(default="test")
            value: int = Field(default=42)

        model = await SimpleSetup.get_clean_model(config_fields=False, hidden_fields=False)
        schema = model.model_json_schema()

        properties = schema.get("properties", {})
        assert set(properties.keys()) == {"name", "value"}
        assert "$defs" not in schema

    @pytest.mark.asyncio
    async def test_clean_model_with_hidden_true_includes_resolved_tools(self) -> None:
        """Clean model with hidden_fields=True should include resolved_tools."""

        class ToolSetup(SetupModel):
            enabled: bool = Field(default=True)

        model = await ToolSetup.get_clean_model(config_fields=False, hidden_fields=True)

        assert "resolved_tools" in model.model_fields

    @pytest.mark.asyncio
    async def test_clean_model_with_tool_reference_field(self) -> None:
        """Clean model with tool_reference_input field produces valid schema without ToolModuleInfo defs."""

        class ArchetypeSetup(SetupModel):
            name: str = Field(default="test")
            my_tools: tool_reference_input()  # type: ignore[valid-type]

        model = await ArchetypeSetup.get_clean_model(config_fields=False, hidden_fields=False)
        schema = model.model_json_schema()

        # Tool reference field should be in properties with custom array schema
        properties = schema.get("properties", {})
        assert "my_tools" in properties
        assert properties["my_tools"]["type"] == "array"
        assert properties["my_tools"]["items"]["properties"]["setupId"]["type"] == "string"
        assert properties["my_tools"]["ui:widget"] == "toolSelect"

        # resolved_tools and its type defs should NOT leak
        assert "resolved_tools" not in properties
        defs = schema.get("$defs", {})
        assert "ToolModuleInfo" not in defs
        assert "ToolDefinition" not in defs
        assert "ToolParameter" not in defs
