"""Conditional field visibility for react-jsonschema-form.

This module provides a clean way to mark fields as conditional using Annotated metadata,
generating JSON Schema with if/then clauses for react-jsonschema-form.

Example:
    from typing import Annotated, Literal
    from pydantic import BaseModel, Field
    from digitalkin.utils import Conditional, ConditionalSchemaMixin

    class Tools(ConditionalSchemaMixin, BaseModel):
        web_search_enabled: bool = Field(...)

        web_search_engine: Annotated[
            Literal["duckduckgo", "tavily"],
            Conditional(trigger="web_search_enabled", show_when=True),
        ] = Field(...)

See Also:
    - Documentation: docs/api/conditional_schema.md
    - Tests: tests/utils/test_conditional_schema.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel

if TYPE_CHECKING:
    from pydantic.annotated_handlers import GetJsonSchemaHandler
    from pydantic.fields import FieldInfo
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core.core_schema import CoreSchema


@dataclass
class ConditionalField:
    """Metadata for conditional field visibility.

    Use with typing.Annotated to mark fields that should only appear
    when a trigger field has a specific value.

    Args:
        trigger: Name of the field that controls visibility.
        show_when: Value(s) that trigger field must have to show this field.
            Can be a boolean, string, or list of strings for multiple values.
        required_when_shown: Whether field is required when visible. Defaults to True.

    Example:
        # Boolean condition
        web_search_engine: Annotated[
            str,
            Conditional(trigger="web_search_enabled", show_when=True),
        ] = Field(...)

        # Enum condition
        advanced_option: Annotated[
            str,
            Conditional(trigger="mode", show_when="advanced"),
        ] = Field(...)

        # Multiple values condition
        shared_feature: Annotated[
            bool,
            Conditional(trigger="mode", show_when=["standard", "advanced"]),
        ] = Field(...)
    """

    trigger: str
    show_when: bool | str | list[str]
    required_when_shown: bool = True

    def __post_init__(self) -> None:
        """Normalize single-item lists to scalar values."""
        if isinstance(self.show_when, list) and len(self.show_when) == 1:
            self.show_when = self.show_when[0]


# Short alias for cleaner API
Conditional = ConditionalField


def get_conditional_metadata(field_info: FieldInfo) -> ConditionalField | None:
    """Extract ConditionalField from field metadata.

    Args:
        field_info: The Pydantic FieldInfo object to inspect.

    Returns:
        The ConditionalField metadata instance if found, None otherwise.
    """
    for meta in field_info.metadata:
        if isinstance(meta, ConditionalField):
            return meta
    return None


def has_conditional(field_info: FieldInfo) -> bool:
    """Check if field has ConditionalField metadata.

    Args:
        field_info: The Pydantic FieldInfo object to check.

    Returns:
        True if the field has ConditionalField metadata, False otherwise.
    """
    return get_conditional_metadata(field_info) is not None


def _collect_conditions(
    model_fields: dict[str, FieldInfo],
    props: dict[str, Any],
) -> tuple[dict[tuple[str, Any], list[tuple[str, bool]]], set[str]]:
    """Collect conditional fields grouped by trigger and show_when value.

    Args:
        model_fields: The model's field definitions.
        props: The schema properties dict.

    Returns:
        Tuple of (conditions dict, fields to remove set).
    """
    conditions: dict[tuple[str, Any], list[tuple[str, bool]]] = {}
    fields_to_remove: set[str] = set()

    for field_name, field_info in model_fields.items():
        cond = get_conditional_metadata(field_info)
        if cond is None or field_name not in props:
            continue

        show_key = tuple(cond.show_when) if isinstance(cond.show_when, list) else cond.show_when
        key = (cond.trigger, show_key)

        if key not in conditions:
            conditions[key] = []
        conditions[key].append((field_name, cond.required_when_shown))
        fields_to_remove.add(field_name)

    return conditions, fields_to_remove


def _build_if_clause(trigger: str, *, show_when: bool | str | tuple[str, ...]) -> dict[str, Any]:
    """Build the if clause for a conditional.

    Args:
        trigger: The trigger field name.
        show_when: The value(s) that trigger visibility.

    Returns:
        The if clause dict.
    """
    if isinstance(show_when, tuple):
        return {"properties": {trigger: {"enum": list(show_when)}}, "required": [trigger]}
    return {"properties": {trigger: {"const": show_when}}, "required": [trigger]}


def _resolve_field_schema(
    field_schema: dict[str, Any],
    handler: GetJsonSchemaHandler,
) -> dict[str, Any]:
    """Resolve $ref in field schema if present.

    Args:
        field_schema: The field's schema dict.
        handler: The JSON schema handler for resolving refs.

    Returns:
        The resolved schema dict.
    """
    if "$ref" not in field_schema:
        return field_schema

    resolved = handler.resolve_ref_schema(field_schema)
    extra = {k: v for k, v in field_schema.items() if k != "$ref"}
    return {**resolved, **extra}


class ConditionalSchemaMixin(BaseModel):
    """Mixin for automatic conditional field processing in JSON schema.

    Inherit from this mixin to automatically generate JSON Schema with
    if/then clauses for fields marked with ConditionalField metadata.

    The mixin processes Annotated fields with Conditional metadata and:
    1. Removes conditional fields from main properties
    2. Adds them to allOf with if/then clauses
    3. Groups multiple fields with the same condition together

    Example:
        class Config(ConditionalSchemaMixin, BaseModel):
            mode: Literal["basic", "advanced"] = Field(...)

            advanced_option: Annotated[
                str,
                Conditional(trigger="mode", show_when="advanced"),
            ] = Field(...)

        # Generates schema with:
        # {
        #     "properties": {"mode": {...}},
        #     "allOf": [{
        #         "if": {"properties": {"mode": {"const": "advanced"}}},
        #         "then": {"properties": {"advanced_option": {...}}}
        #     }]
        # }
    """

    model_fields: ClassVar[dict[str, FieldInfo]]  # type: ignore[misc]

    @classmethod
    def __get_pydantic_json_schema__(  # noqa: PLW3201
        cls,
        core_schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        """Generate JSON schema with conditional field handling.

        Args:
            core_schema: The Pydantic core schema.
            handler: The JSON schema handler for resolving refs.

        Returns:
            The JSON schema with if/then clauses for conditional fields.
        """
        schema = handler(core_schema)
        props = schema.get("properties", {})
        if not props:
            return schema

        conditions, fields_to_remove = _collect_conditions(cls.model_fields, props)
        if not conditions:
            return schema

        all_of = schema.setdefault("allOf", [])

        for (trigger, show_when), field_list in conditions.items():
            then_props: dict[str, Any] = {}
            then_required: list[str] = []

            for field_name, required in field_list:
                then_props[field_name] = _resolve_field_schema(props[field_name], handler)
                if required:
                    then_required.append(field_name)

            if_clause = _build_if_clause(trigger, show_when=show_when)
            then_clause: dict[str, Any] = {"properties": then_props}
            if then_required:
                then_clause["required"] = then_required

            all_of.append({"if": if_clause, "then": then_clause})

        for field_name in fields_to_remove:
            del props[field_name]

        if "required" in schema:
            schema["required"] = [r for r in schema["required"] if r not in fields_to_remove]

        return schema
