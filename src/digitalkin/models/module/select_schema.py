"""SelectSchema for trigger selection UI generation."""

from typing import Any

from pydantic import BaseModel

from digitalkin.utils.schema_splitter import SchemaSplitter


class SelectSchema(BaseModel):
    """Base class for generating trigger selection schema.

    Subclass and add boolean fields to customize the selection UI.
    If no fields are defined, schema is auto-generated from registered protocols.
    Set `select_format = None` in your module to disable.

    Example:
        class MySelectSchema(SelectSchema):
            message: bool = Field(default=True, title="Message", description="Process messages")
            file: bool = Field(default=False, title="File", description="Process files")
    """

    @classmethod
    def build(cls, protocols_info: dict[str, str]) -> dict[str, Any] | None:
        """Build the select schema.

        If the subclass has user-defined fields, uses those.
        Otherwise, auto-generates from protocols_info.

        Args:
            protocols_info: Dict mapping protocol name to description.

        Returns:
            Dict with json_schema and ui_schema keys, or None to exclude.
        """
        has_custom_fields = cls is not SelectSchema and bool(cls.model_fields)

        if has_custom_fields:
            json_schema, ui_schema = SchemaSplitter.split(cls.model_json_schema())
            for field_name, field_info in cls.model_fields.items():
                if field_info.annotation is bool:
                    if field_name not in ui_schema:
                        ui_schema[field_name] = {}
                    if "ui:widget" not in ui_schema[field_name]:
                        ui_schema[field_name]["ui:widget"] = "checkbox"
            return {
                "json_schema": json_schema,
                "ui_schema": ui_schema,
            }

        if not protocols_info:
            return None

        trigger_select_json = {
            "type": "object",
            "title": "Trigger Selection",
            "properties": {
                protocol: {
                    "type": "boolean",
                    "title": protocol,
                    "description": description,
                    "default": False,
                }
                for protocol, description in protocols_info.items()
            },
        }
        trigger_select_ui = {protocol: {"ui:widget": "checkbox"} for protocol in protocols_info}

        return {
            "json_schema": trigger_select_json,
            "ui_schema": trigger_select_ui,
        }
