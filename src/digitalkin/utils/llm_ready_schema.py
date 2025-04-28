"""LLM format schema for Pydantic models.

This module provides functionality to generate JSON schemas for Pydantic models ready for LLMs.
"""

import copy
from typing import Any

from pydantic import BaseModel
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaValue


class CustomOrderSchema(GenerateJsonSchema):
    """Custom schema generator to sort keys in a specific order."""

    def sort(self, value: JsonSchemaValue, parent_key: str | None = None) -> JsonSchemaValue:  # noqa: ARG002
        """Sort the keys of the schema in a specific order.

        Args:
            value: The schema value to sort.
            parent_key: The parent key of the schema value.

        Returns:
            The sorted schema value.
        """
        if isinstance(value, dict):
            # Define your preferred order
            preferred = ["title", "description", "type", "examples", "properties"]
            # Collect all keys, putting preferred ones first
            keys = preferred + [k for k in value if k not in preferred]
            # Recurse for each value
            return {k: self.sort(value[k], k) for k in keys if k in value}
        if isinstance(value, list):
            return [self.sort(v) for v in value]
        return value


def inline_refs(schema: dict) -> dict:
    """Recursively resolve and inline all $ref in the schema.

    Args:
        schema: The JSON schema to inline.

    Returns:
        The inlined JSON schema.
    """
    schema = copy.deepcopy(schema)
    defs = schema.pop("$defs", {})

    def _resolve(obj: Any) -> Any:  # noqa: ANN401
        if isinstance(obj, dict):
            if "$ref" in obj:
                ref = obj["$ref"]
                if ref.startswith("#/$defs/"):
                    key = ref.split("/")[-1]
                    return _resolve(defs[key])
            return {k: _resolve(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_resolve(item) for item in obj]
        return obj

    return _resolve(schema)


def llm_ready_schema(model: type[BaseModel]) -> dict:
    """Convert a Pydantic model to a JSON schema ready for LLMs.

    Args:
        model: The Pydantic model to convert.

    Returns:
        The JSON schema as a dictionary.
    """
    schema = model.model_json_schema(schema_generator=CustomOrderSchema)
    return inline_refs(schema)
