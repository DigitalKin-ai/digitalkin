"""Schema splitter for react-jsonschema-form."""

from typing import Any


class SchemaSplitter:
    """Splits a combined JSON schema into jsonschema and uischema for react-jsonschema-form."""

    @classmethod
    def split(cls, combined_schema: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """Split schema into (jsonschema, uischema).

        Args:
            combined_schema: Combined JSON schema with ui:* properties.

        Returns:
            Tuple of (jsonschema, uischema).
        """
        defs_ui: dict[str, dict[str, Any]] = {}
        if schema_defs := combined_schema.get("$defs", {}):
            for def_name, def_value in schema_defs.items():
                if isinstance(def_value, dict):
                    defs_ui[def_name] = {}
                    cls._extract_ui_properties(def_value, defs_ui[def_name], schema_defs)

        json_schema: dict[str, Any] = {}
        ui_schema: dict[str, Any] = {}
        cls._process_object(combined_schema, json_schema, ui_schema, defs_ui, schema_defs)
        return json_schema, ui_schema

    @classmethod
    def _extract_ui_properties(  # Complex: recursive traversal of nested JSON schema structures # noqa: C901, PLR0912
        cls,
        source: dict[str, Any],
        ui_target: dict[str, Any],
        defs: dict[str, Any] | None = None,
    ) -> None:
        """Extract ui:* properties from source into ui_target recursively.

        Args:
            source: Source dict to extract from.
            ui_target: Target dict for ui properties.
            defs: The $defs dictionary to resolve $ref references.
        """
        for key, value in source.items():
            if key.startswith("ui:"):
                ui_target[key] = value
            elif key == "properties" and isinstance(value, dict):
                for prop_name, prop_value in value.items():
                    if isinstance(prop_value, dict):
                        prop_ui: dict[str, Any] = {}
                        cls._extract_ui_properties(prop_value, prop_ui, defs)
                        if prop_ui:
                            ui_target[prop_name] = prop_ui
            elif key == "items" and isinstance(value, dict):
                items_ui: dict[str, Any] = {}
                cls._extract_ui_properties(value, items_ui, defs)
                if items_ui:
                    ui_target["items"] = items_ui
            elif key in {"allOf", "oneOf", "anyOf"} and isinstance(value, list):
                # For oneOf/anyOf, create a mirrored structure in ui_target
                if key in {"oneOf", "anyOf"}:
                    ui_target[key] = []
                    for item in value:
                        if isinstance(item, dict):
                            item_ui: dict[str, Any] = {}
                            cls._extract_ui_properties(item, item_ui, defs)
                            ui_target[key].append(item_ui)
                else:  # allOf - merge into parent
                    for item in value:
                        if isinstance(item, dict):
                            cls._extract_ui_properties(item, ui_target, defs)
            elif key in {"if", "then", "else"} and isinstance(value, dict):
                cls._extract_ui_properties(value, ui_target, defs)
            elif key == "$ref" and isinstance(value, str) and defs is not None and value.startswith("#/$defs/"):
                # Resolve $ref and extract UI properties from the referenced definition
                def_name = value[8:]
                if def_name in defs:
                    cls._extract_ui_properties(defs[def_name], ui_target, defs)

    @classmethod
    def _process_object(  # Complex: JSON schema node splitting into json/ui # noqa: C901, PLR0912, PLR0915
        cls,
        source: dict[str, Any],
        json_target: dict[str, Any],
        ui_target: dict[str, Any],
        defs_ui: dict[str, dict[str, Any]],
        schema_defs: dict[str, Any] | None = None,
    ) -> None:
        """Process an object, splitting json and ui properties.

        Args:
            source: Source object to process.
            json_target: Target dict for json schema.
            ui_target: Target dict for ui schema.
            defs_ui: Pre-extracted UI properties from $defs.
            schema_defs: The original $defs dictionary to resolve $ref in _extract_ui_properties.
        """
        for key, value in source.items():
            if key.startswith("ui:"):
                ui_target[key] = value
            elif key == "properties" and isinstance(value, dict):
                json_target["properties"] = {}
                for prop_name, prop_value in value.items():
                    if isinstance(prop_value, dict):
                        json_target["properties"][prop_name] = {}
                        prop_ui: dict[str, Any] = {}
                        cls._process_property(prop_value, json_target["properties"][prop_name], prop_ui, defs_ui)
                        if prop_ui:
                            ui_target[prop_name] = prop_ui
                    else:
                        json_target["properties"][prop_name] = prop_value
            elif key == "$defs" and isinstance(value, dict):
                json_target["$defs"] = {}
                for def_name, def_value in value.items():
                    if isinstance(def_value, dict):
                        json_target["$defs"][def_name] = {}
                        cls._strip_ui_properties(def_value, json_target["$defs"][def_name])
                    else:
                        json_target["$defs"][def_name] = def_value
            elif key == "items" and isinstance(value, dict):
                json_target["items"] = {}
                items_ui: dict[str, Any] = {}
                cls._process_property(value, json_target["items"], items_ui, defs_ui)
                if items_ui:
                    ui_target["items"] = items_ui
            elif key == "allOf" and isinstance(value, list):
                json_target["allOf"] = []
                for item in value:
                    if isinstance(item, dict):
                        item_json_all_of: dict[str, Any] = {}
                        cls._strip_ui_properties(item, item_json_all_of)
                        json_target["allOf"].append(item_json_all_of)
                        # Extract UI properties from allOf item
                        cls._extract_ui_properties(item, ui_target, schema_defs)
                    else:
                        json_target["allOf"].append(item)
            elif key in {"oneOf", "anyOf"} and isinstance(value, list):
                json_target[key] = []
                ui_target[key] = []  # Mirror structure in ui_schema
                for item in value:
                    if isinstance(item, dict):
                        item_json_oneof_anyof: dict[str, Any] = {}
                        cls._strip_ui_properties(item, item_json_oneof_anyof)
                        json_target[key].append(item_json_oneof_anyof)
                        # Extract UI properties into a separate dict for this variant
                        item_ui_oneof_anyof: dict[str, Any] = {}
                        cls._extract_ui_properties(item, item_ui_oneof_anyof, schema_defs)
                        ui_target[key].append(item_ui_oneof_anyof)
                    else:
                        json_target[key].append(item)
            elif key in {"if", "then", "else"} and isinstance(value, dict):
                json_target[key] = {}
                cls._strip_ui_properties(value, json_target[key])
                # Extract UI properties from conditional
                cls._extract_ui_properties(value, ui_target, schema_defs)
            else:
                json_target[key] = value

    @classmethod
    def _process_property(  # Complex: recursive property splitting with $ref resolution # noqa: C901, PLR0912
        cls,
        source: dict[str, Any],
        json_target: dict[str, Any],
        ui_target: dict[str, Any],
        defs_ui: dict[str, dict[str, Any]],
    ) -> None:
        """Process a property, resolving $ref for UI properties.

        Args:
            source: Source property dict.
            json_target: Target dict for json schema.
            ui_target: Target dict for ui schema.
            defs_ui: Pre-extracted UI properties from $defs.
        """
        if "$ref" in source:
            ref_path = source["$ref"]
            if ref_path.startswith("#/$defs/"):
                def_name = ref_path[8:]
                if def_name in defs_ui:
                    ui_target.update(defs_ui[def_name])

        for key, value in source.items():
            if key.startswith("ui:"):
                ui_target[key] = value
            elif key == "properties" and isinstance(value, dict):
                json_target["properties"] = {}
                for prop_name, prop_value in value.items():
                    if isinstance(prop_value, dict):
                        json_target["properties"][prop_name] = {}
                        prop_ui: dict[str, Any] = {}
                        cls._process_property(prop_value, json_target["properties"][prop_name], prop_ui, defs_ui)
                        if prop_ui:
                            ui_target[prop_name] = prop_ui
                    else:
                        json_target["properties"][prop_name] = prop_value
            elif key == "items" and isinstance(value, dict):
                json_target["items"] = {}
                items_ui: dict[str, Any] = {}
                cls._process_property(value, json_target["items"], items_ui, defs_ui)
                if items_ui:
                    ui_target["items"] = items_ui
            elif key in {"oneOf", "anyOf"} and isinstance(value, list):
                json_target[key] = []
                ui_target[key] = []  # Mirror structure in ui_schema
                for item in value:
                    if isinstance(item, dict):
                        item_json_oneof_anyof: dict[str, Any] = {}
                        cls._strip_ui_properties(item, item_json_oneof_anyof)
                        json_target[key].append(item_json_oneof_anyof)
                        # Extract UI properties for this variant
                        item_ui_oneof_anyof: dict[str, Any] = {}
                        ref_path = item.get("$ref", "")
                        if ref_path.startswith("#/$defs/") and ref_path[8:] in defs_ui:
                            item_ui_oneof_anyof.update(defs_ui[ref_path[8:]])
                        ui_target[key].append(item_ui_oneof_anyof)
                    else:
                        json_target[key].append(item)
            else:
                json_target[key] = value

    @classmethod
    def _strip_ui_properties(  # noqa: C901, PLR0912
        cls, source: dict[str, Any], json_target: dict[str, Any]
    ) -> None:  # Complex: recursive traversal of nested JSON schema structures
        """Copy source to json_target, stripping ui:* properties.

        Args:
            source: Source dict.
            json_target: Target dict without ui:* properties.
        """
        for key, value in source.items():
            if key.startswith("ui:"):
                continue
            if key == "properties" and isinstance(value, dict):
                json_target["properties"] = {}
                for prop_name, prop_value in value.items():
                    if isinstance(prop_value, dict):
                        json_target["properties"][prop_name] = {}
                        cls._strip_ui_properties(prop_value, json_target["properties"][prop_name])
                    else:
                        json_target["properties"][prop_name] = prop_value
            elif key == "$defs" and isinstance(value, dict):
                json_target["$defs"] = {}
                for def_name, def_value in value.items():
                    if isinstance(def_value, dict):
                        json_target["$defs"][def_name] = {}
                        cls._strip_ui_properties(def_value, json_target["$defs"][def_name])
                    else:
                        json_target["$defs"][def_name] = def_value
            elif key == "items" and isinstance(value, dict):
                json_target["items"] = {}
                cls._strip_ui_properties(value, json_target["items"])
            elif key == "allOf" and isinstance(value, list):
                json_target["allOf"] = []
                for item in value:
                    if isinstance(item, dict):
                        item_json_all_of: dict[str, Any] = {}
                        cls._strip_ui_properties(item, item_json_all_of)
                        json_target["allOf"].append(item_json_all_of)
                    else:
                        json_target["allOf"].append(item)
            elif key in {"oneOf", "anyOf"} and isinstance(value, list):
                json_target[key] = []
                for item in value:
                    if isinstance(item, dict):
                        item_json_oneof_anyof: dict[str, Any] = {}
                        cls._strip_ui_properties(item, item_json_oneof_anyof)
                        json_target[key].append(item_json_oneof_anyof)
                    else:
                        json_target[key].append(item)
            elif key in {"if", "then", "else"} and isinstance(value, dict):
                json_target[key] = {}
                cls._strip_ui_properties(value, json_target[key])
            else:
                json_target[key] = value
