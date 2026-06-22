"""LLM-ready JSON schema generation for Pydantic models."""

import copy
from typing import Any

from pydantic import BaseModel
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaValue


class CustomOrderSchema(GenerateJsonSchema):
    """Custom schema generator to sort keys in a specific order."""

    def sort(
        self,
        value: JsonSchemaValue,
        parent_key: str | None = None,  # noqa: ARG002
    ) -> JsonSchemaValue:  # Overrides Pydantic GenerateJsonSchema.sort signature
        """Sort the keys of the schema in a specific order.

        Args:
            value: The schema value to sort.
            parent_key: The parent key of the schema value.

        Returns:
            The sorted schema value.
        """
        if isinstance(value, dict):
            preferred = ["title", "description", "type", "examples", "properties"]
            keys = preferred + [k for k in value if k not in preferred]
            return {k: self.sort(value[k], k) for k in keys if k in value}
        if isinstance(value, list):
            return [self.sort(v) for v in value]
        return value


class LlmReadySchema:
    """Generate and inline JSON schemas for LLM consumption."""

    @staticmethod
    def inline_refs(schema: dict) -> dict:
        """Recursively resolve and inline all $ref in the schema.

        Args:
            schema: The JSON schema to inline.

        Returns:
            The inlined JSON schema.
        """
        schema = copy.deepcopy(schema)
        defs = schema.pop("$defs", {})

        def _resolve(obj: Any) -> Any:
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

    @staticmethod
    def llm_ready_schema(model: type[BaseModel]) -> dict:
        """Convert a Pydantic model to a JSON schema ready for LLMs.

        Args:
            model: The Pydantic model to convert.

        Returns:
            The JSON schema as a dictionary.
        """
        schema = model.model_json_schema(schema_generator=CustomOrderSchema)
        return LlmReadySchema.inline_refs(schema)
