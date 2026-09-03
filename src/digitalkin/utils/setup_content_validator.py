"""Validate a setup's ``content`` against a module's config-setup JSON schema.

The archetype/module owns the schema (its ``SetupModel``); over the wire we only get the JSON
schema. This compiles a throwaway Pydantic model from that schema — resolving ``$ref``/``$defs``,
nesting objects, typing array elements, closing enumerations and honouring nullability/constraints
— and validates the content, so a caller (e.g. an LLM driving ``kins_manager.update``) that forgets
a required field, sends a wrong-typed one, an out-of-enum value, a null on a typed field or an
undeclared key gets a correctable error *before* the write instead of breaking the setup.
"""

from __future__ import annotations

from typing import Annotated, Any, ClassVar, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationError, create_model


class SetupContentValidator:
    """Compile a Pydantic model from a config-setup JSON schema and validate content against it.

    Mirrors the schema's own strictness rather than a loose superset: objects reject non-objects,
    arrays type their elements, ``enum``/``const`` become closed ``Literal`` choices, a field is
    nullable only when the schema says so, undeclared keys are forbidden unless
    ``additionalProperties`` is ``true``, scalars are validated in strict mode (no ``"2"``→number or
    ``true``→number coercion), strings reject control characters, and numeric/array/string
    constraints are enforced. An empty schema is a no-op; the module's own ``ConfigSetupModule``
    stays the authoritative check.
    """

    _JSON_TO_PY: ClassVar[dict[str, Any]] = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "array": list,
        "object": dict,
        "null": type(None),
    }
    _STRICT_SCALARS: ClassVar[tuple[type, ...]] = (int, float, bool)
    _FIRST_PRINTABLE: ClassVar[int] = 0x20  # code points below this are C0 control characters.
    _LAST_BMP: ClassVar[int] = 0xFFFF  # code points above this are astral (non-BMP, e.g. emoji).
    _OUTPUT_FORMAT_SPEC_KEY: ClassVar[str] = "output_format_spec"
    _OUTPUT_FORMAT_SPEC_MAX: ClassVar[int] = 4096  # at or past this the written setup is unusable.
    _NUMERIC_CONSTRAINTS: ClassVar[dict[str, str]] = {
        "minimum": "ge",
        "maximum": "le",
        "exclusiveMinimum": "gt",
        "exclusiveMaximum": "lt",
    }
    _STRING_CONSTRAINTS: ClassVar[dict[str, str]] = {
        "minLength": "min_length",
        "maxLength": "max_length",
        "pattern": "pattern",
    }
    _ARRAY_CONSTRAINTS: ClassVar[dict[str, str]] = {
        "minItems": "min_length",
        "maxItems": "max_length",
    }
    _OBJECT_CONSTRAINTS: ClassVar[dict[str, str]] = {
        "minProperties": "min_length",
        "maxProperties": "max_length",
    }

    @classmethod
    def reject_control_chars(cls, value: str) -> str:
        """Refuse C0 control characters (except tab/newline/carriage-return) in a string.

        Returns:
            The value unchanged when clean.

        Raises:
            ValueError: The string carries a control character (e.g. a NUL byte or ANSI escape),
                which downstream consumers persist verbatim and mis-render.
        """
        if any(ord(char) < cls._FIRST_PRINTABLE and char not in "\t\n\r" for char in value):
            msg = "string must not contain control characters (e.g. NUL, ANSI escape)"
            raise ValueError(msg)
        return value

    @classmethod
    def reject_unsafe_keys(cls, content: dict[str, Any]) -> dict[str, Any]:
        """Refuse content keys carrying characters persistence silently drops or mangles.

        The schema check validates values, but object KEYS bypass it and reach storage verbatim,
        where a non-BMP character (astral plane, e.g. the emoji U+1F525) or a C0 control character
        is stripped — so the key read back differs from the key written, with ``success:true`` and
        no diagnostic. Reject up front, naming the offending key, rather than mutate in silence.
        Recurses through nested objects and arrays.

        Args:
            content: The setup ``content`` about to be written.

        Returns:
            The content unchanged when every key (at any depth) is safe.

        Raises:
            ValueError: A key carries a C0 control character or a non-BMP character.
        """
        stack: list[tuple[str, Any]] = [("", content)]
        while stack:
            path, node = stack.pop()
            if isinstance(node, dict):
                for key, value in node.items():
                    where = f"{path}.{key}" if path else key
                    if any(
                        ord(ch) > cls._LAST_BMP or (ord(ch) < cls._FIRST_PRINTABLE and ch not in "\t\n\r") for ch in key
                    ):
                        msg = (
                            f"content key {where!r} must not contain control or non-BMP characters "
                            "(e.g. emoji): they are silently dropped on write"
                        )
                        raise ValueError(msg)
                    stack.append((where, value))
            elif isinstance(node, list):
                stack.extend((path, item) for item in node)
        return content

    @classmethod
    def reject_oversized_output_format_spec(cls, content: dict[str, Any]) -> dict[str, Any]:
        """Refuse an ``output_format_spec`` that reaches the length the setup cannot carry.

        The write itself succeeds, so the breakage surfaces later as an unusable setup with no
        diagnostic pointing back at the field. Reject up front, naming the path and the actual
        size. Recurses through nested objects and arrays; a non-string value is left to the
        schema check, which owns typing.

        Args:
            content: The setup ``content`` about to be written.

        Returns:
            The content unchanged when every ``output_format_spec`` fits.

        Raises:
            ValueError: An ``output_format_spec`` string is at or over the limit.
        """
        stack: list[tuple[str, Any]] = [("", content)]
        while stack:
            path, node = stack.pop()
            if isinstance(node, dict):
                for key, value in node.items():
                    where = f"{path}.{key}" if path else key
                    if (
                        key == cls._OUTPUT_FORMAT_SPEC_KEY
                        and isinstance(value, str)
                        and len(value) >= cls._OUTPUT_FORMAT_SPEC_MAX
                    ):
                        msg = (
                            f"content field {where!r} is {len(value)} characters; it must stay under "
                            f"{cls._OUTPUT_FORMAT_SPEC_MAX} or the setup breaks"
                        )
                        raise ValueError(msg)
                    stack.append((where, value))
            elif isinstance(node, list):
                stack.extend((path, item) for item in node)
        return content

    @classmethod
    def validate(cls, content: dict[str, Any], schema: dict[str, Any]) -> None:
        """Validate ``content`` against ``schema``.

        Args:
            content: The setup ``content`` the caller wants to write.
            schema: The module's config-setup JSON schema (``{}`` / no ``properties`` skips).

        Raises:
            ValueError: The content violates the schema (missing/wrong-typed/out-of-enum/null/extra
                field), with a concise per-field message the caller can act on.
        """
        raw_defs = schema.get("$defs")
        defs: dict[str, Any] = raw_defs if isinstance(raw_defs, dict) else {}
        model = cls._build_model(schema, defs, frozenset())
        if model is None:
            return
        try:
            model.model_validate(content)
        except ValidationError as error:
            detail = "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in error.errors())
            msg = f"invalid content for this setup's schema: {detail}"
            raise ValueError(msg) from error

    @classmethod
    def _build_model(
        cls, obj_schema: dict[str, Any], defs: dict[str, Any], seen: frozenset[str]
    ) -> type[BaseModel] | None:
        """Build a Pydantic model from an object schema's ``properties`` (``None`` if it has none).

        Returns:
            The compiled model, or ``None`` when the schema declares no usable properties.
        """
        properties = obj_schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            return None
        required = set(obj_schema.get("required", []))
        fields: dict[str, Any] = {}
        for name, prop in properties.items():
            if not name.isidentifier():  # create_model needs valid identifiers; skip exotic keys.
                continue
            fields[name] = cls._field(prop if isinstance(prop, dict) else {}, defs, seen, required=name in required)
        if not fields:
            return None
        # Undeclared keys are refused unless the schema explicitly opts in with additionalProperties.
        extra: Literal["forbid", "ignore"] = "ignore" if obj_schema.get("additionalProperties") is True else "forbid"
        return create_model("SetupContent", __config__=ConfigDict(extra=extra), **fields)

    @classmethod
    def _field(cls, prop: dict[str, Any], defs: dict[str, Any], seen: frozenset[str], *, required: bool) -> Any:
        """Build one ``create_model`` field spec (annotation + default/constraints) from a property.

        Returns:
            A ``(annotation, default)`` or ``(annotation, FieldInfo)`` tuple for ``create_model``.
        """
        py, nullable = cls._py_type(prop, defs, seen)
        annotation = cls._decorate(py)
        # Only accept an explicit null when the schema declares the field nullable; an optional
        # field keeps default None (absence tolerated) but rejects a null value on a typed field.
        if nullable and py is not Any:
            annotation |= None
        default: Any = ... if required else None
        constraints = cls._constraints(prop)
        if constraints:
            return (annotation, Field(default, **constraints))
        return (annotation, default)

    @classmethod
    def _decorate(cls, py: Any) -> Any:
        """Wrap a leaf type with strict validation and control-char rejection for strings.

        Returns:
            ``Annotated`` strict scalars (str additionally control-char checked); other types verbatim.
        """
        if py is str:
            return Annotated[str, Field(strict=True), AfterValidator(cls.reject_control_chars)]
        if py in cls._STRICT_SCALARS:
            return Annotated[py, Field(strict=True)]
        return py

    @classmethod
    def _py_type(cls, prop: dict[str, Any], defs: dict[str, Any], seen: frozenset[str]) -> tuple[Any, bool]:
        """Map a JSON-schema property to a ``(python_type, nullable)`` pair.

        Resolves ``$ref``, closes ``enum``/``const`` to a ``Literal``, types array elements and
        nests objects; ``nullable`` is true only when the schema explicitly allows null.

        Returns:
            The mapped type and whether null is an accepted value.
        """
        ref = prop.get("$ref")
        if isinstance(ref, str):
            name = ref.rsplit("/", maxsplit=1)[-1]
            if name in seen:  # break reference cycles — enforce the container type only.
                return dict, False
            target = defs.get(name)
            return cls._py_type(target, defs, seen | {name}) if isinstance(target, dict) else (Any, False)
        literal = cls._literal_type(prop)  # enum/const → closed choice.
        if literal is not None:
            return literal
        for combinator in ("anyOf", "oneOf"):
            branches = prop.get(combinator)
            if isinstance(branches, list):
                nullable = any(isinstance(b, dict) and b.get("type") == "null" for b in branches)
                branch = next((b for b in branches if isinstance(b, dict) and b.get("type") != "null"), None)
                if branch is None:
                    return Any, nullable
                inner, inner_null = cls._py_type(branch, defs, seen)
                return inner, nullable or inner_null
        json_type = prop.get("type")
        nullable = False
        if isinstance(json_type, list):
            nullable = "null" in json_type
            json_type = next((candidate for candidate in json_type if candidate != "null"), None)
        if json_type == "object":
            base: Any = cls._build_model(prop, defs, seen) or cls._mapping_type(prop, defs, seen)
        elif json_type == "array":
            base = cls._list_type(prop, defs, seen)
        else:
            base = cls._JSON_TO_PY.get(json_type, Any) if isinstance(json_type, str) else Any
        return base, nullable

    @classmethod
    def _mapping_type(cls, prop: dict[str, Any], defs: dict[str, Any], seen: frozenset[str]) -> Any:
        """Type a property-less object by its ``additionalProperties`` value schema; else ``dict``.

        A ``{"type": "object", "additionalProperties": {"type": "boolean"}}`` (e.g. tool ``triggers``)
        becomes ``dict[str, bool]`` so a string value is refused, instead of a bare ``dict`` that lets
        anything through.

        Returns:
            ``dict[str, value]`` when ``additionalProperties`` types the values, else ``dict``.
        """
        additional = prop.get("additionalProperties")
        if not isinstance(additional, dict) or not additional:
            return dict
        value, value_nullable = cls._py_type(additional, defs, seen)
        if value is Any:
            return dict
        value = cls._decorate(value)
        if value_nullable:
            value |= None
        return dict[str, value]  # type: ignore[valid-type]

    @classmethod
    def _literal_type(cls, prop: dict[str, Any]) -> tuple[Any, bool] | None:
        """Turn an ``enum``/``const`` property into a ``(Literal[...], nullable)`` pair.

        Returns:
            The ``Literal`` of the hashable non-null values and whether null is allowed, or ``None``
            when the property is not an enumeration (or carries no usable literal values).
        """
        enum = prop.get("enum")
        values = enum if isinstance(enum, list) else ([prop["const"]] if "const" in prop else None)
        if not values:
            return None
        nullable = None in values
        usable = [v for v in values if isinstance(v, (str, int)) and v is not None]  # bool ⊂ int; all hashable.
        if not usable:
            return None
        return Literal[tuple(usable)], nullable

    @classmethod
    def _list_type(cls, prop: dict[str, Any], defs: dict[str, Any], seen: frozenset[str]) -> Any:
        """Parameterise an array by its declared element type; bare ``list`` when unknown.

        Returns:
            ``list[element]`` when ``items`` types the element, else ``list``.
        """
        items = prop.get("items")
        if not isinstance(items, dict) or not items:
            return list
        element, element_nullable = cls._py_type(items, defs, seen)
        if element is Any:
            return list
        element = cls._decorate(element)  # array elements are strict and control-char checked too.
        return list[element | None] if element_nullable else list[element]  # type: ignore[valid-type]

    @classmethod
    def _constraints(cls, prop: dict[str, Any]) -> dict[str, Any]:
        """Extract enforceable numeric/string/array constraints declared on a property.

        Covers numeric bounds (once a ``maximum`` is declared), array/object cardinality (once
        ``minItems``/``minProperties`` is declared) and string length/pattern (once a
        ``pattern`` is declared).

        Returns:
            ``Field`` keyword arguments for the constraints the schema declares (empty if none).
        """
        constraints: dict[str, Any] = {}
        for schema_key, field_key in cls._NUMERIC_CONSTRAINTS.items():
            value = prop.get(schema_key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                constraints[field_key] = value
        length_constraints = {**cls._STRING_CONSTRAINTS, **cls._ARRAY_CONSTRAINTS, **cls._OBJECT_CONSTRAINTS}
        for schema_key, field_key in length_constraints.items():
            value = prop.get(schema_key)
            expected = str if field_key == "pattern" else int
            if isinstance(value, expected) and not isinstance(value, bool):
                constraints[field_key] = value
        return constraints
