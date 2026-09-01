"""Conditional field visibility for react-jsonschema-form.

Mark fields as conditional with ``Annotated`` metadata to generate JSON
Schema with if/then clauses for react-jsonschema-form.

See ``docs/api/conditional_schema.md`` and ``tests/utils/test_conditional_schema.py``.
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
        show_when: Value(s) the trigger field must have. Bool, str, or list.
        required_when_shown: Whether field is required when visible.
    """

    trigger: str
    show_when: bool | str | list[str]
    required_when_shown: bool = True

    def __post_init__(self) -> None:
        """Normalize single-item lists to scalar values."""
        if isinstance(self.show_when, list) and len(self.show_when) == 1:
            self.show_when = self.show_when[0]


Conditional = ConditionalField


class ConditionalSchemaMixin(BaseModel):
    """Mixin that rewrites JSON Schema with if/then clauses for Conditional fields."""

    model_fields: ClassVar[dict[str, FieldInfo]]  # Pydantic ClassVar redeclaration for mixin type access # type: ignore[misc]

    @staticmethod
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

    @staticmethod
    def has_conditional(field_info: FieldInfo) -> bool:
        """Check if field has ConditionalField metadata.

        Args:
            field_info: The Pydantic FieldInfo object to check.

        Returns:
            True if the field has ConditionalField metadata, False otherwise.
        """
        return ConditionalSchemaMixin.get_conditional_metadata(field_info) is not None

    @staticmethod
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
            cond = ConditionalSchemaMixin.get_conditional_metadata(field_info)
            if cond is None or field_name not in props:
                continue

            show_key = tuple(cond.show_when) if isinstance(cond.show_when, list) else cond.show_when
            key = (cond.trigger, show_key)

            if key not in conditions:
                conditions[key] = []
            conditions[key].append((field_name, cond.required_when_shown))
            fields_to_remove.add(field_name)

        return conditions, fields_to_remove

    @staticmethod
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

    @staticmethod
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

    @classmethod
    def __get_pydantic_json_schema__(
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

        conditions, fields_to_remove = cls._collect_conditions(cls.model_fields, props)
        if not conditions:
            return schema

        all_of = schema.setdefault("allOf", [])

        for (trigger, show_when), field_list in conditions.items():
            then_props: dict[str, Any] = {}
            then_required: list[str] = []

            for field_name, required in field_list:
                then_props[field_name] = cls._resolve_field_schema(props[field_name], handler)
                if required:
                    then_required.append(field_name)

            if_clause = cls._build_if_clause(trigger, show_when=show_when)
            then_clause: dict[str, Any] = {"properties": then_props}
            if then_required:
                then_clause["required"] = then_required

            all_of.append({"if": if_clause, "then": then_clause})

        for field_name in fields_to_remove:
            del props[field_name]

        if "required" in schema:
            schema["required"] = [r for r in schema["required"] if r not in fields_to_remove]

        return schema
