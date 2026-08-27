"""Render a ``.env`` template from the declared settings classes.

The settings models are the single declaration point for every environment
variable, so the template is generated from them instead of being maintained by
hand — a variable that no field backs cannot appear, and a field that no
template documents cannot be forgotten.

Run it over this package, plus a module's own settings package when it has one::

    python -m digitalkin.utils.env_template --package digitalkin.models.settings \
        --output .env.exemple
    python -m digitalkin.utils.env_template --package digitalkin.models.settings \
        --minimum --output .env.minimum.exemple

Mark the variables a deployment must set with
``Field(..., json_schema_extra={"env_required": True})``; ``--minimum`` keeps
only those. Add ``"env_example": "sk-xxx"`` to show a placeholder in the
template instead of the runtime default, so a secret can default to empty (and
fail the boot check) while the template still hints at the expected shape.
"""

import argparse
import importlib
import inspect
import pkgutil
import sys
from collections.abc import Sequence
from enum import Enum
from pathlib import Path
from typing import Any, get_args

from pydantic import BaseModel, SecretStr
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings


class EnvTemplate:
    """Turn ``BaseSettings`` declarations into a ``.env`` template."""

    _HEADER = (
        "# ─────────────────────────────────────────────────────────────────────\n"
        "# GENERATED FILE — do not edit by hand.\n"
        "# Regenerate with: task env-template\n"
        "# Every variable below is a field of a pydantic-settings class; add or\n"
        "# change one there and rerun the command.\n"
        "# ─────────────────────────────────────────────────────────────────────\n"
    )
    _MINIMUM_HEADER = (
        "# ─────────────────────────────────────────────────────────────────────\n"
        "# GENERATED FILE — do not edit by hand.\n"
        "# Regenerate with: task env-template\n"
        "# The minimum a deployment must define for the module to run. Every\n"
        "# other variable has a usable default; the full template lists them all.\n"
        "# ─────────────────────────────────────────────────────────────────────\n"
    )

    @classmethod
    def _settings_classes(cls, packages: Sequence[str]) -> list[type[BaseSettings]]:
        """Collect the ``BaseSettings`` subclasses declared in the given packages.

        Args:
            packages: Importable package or module names to scan.

        Returns:
            The classes, ordered by declaring module then declaration order.
        """
        found: dict[str, type[BaseSettings]] = {}
        for name in packages:
            root = importlib.import_module(name)
            modules = [name]
            # A package carries submodule search locations; a plain module carries None.
            if root.__spec__ is not None and root.__spec__.submodule_search_locations is not None:
                modules += [m.name for m in pkgutil.walk_packages(root.__spec__.submodule_search_locations, f"{name}.")]
            for module_name in modules:
                module = importlib.import_module(module_name)
                for obj in vars(module).values():
                    if (
                        inspect.isclass(obj)
                        and issubclass(obj, BaseSettings)
                        and obj is not BaseSettings
                        and obj.__module__ == module_name
                    ):
                        found[f"{obj.__module__}.{obj.__qualname__}"] = obj
        return [found[key] for key in sorted(found)]

    @classmethod
    def _render_value(cls, value: Any) -> str:
        """Render a field default as an env-file value.

        Args:
            value: The default to render.

        Returns:
            The value as it appears after the ``=``.
        """
        if value is None:
            return ""
        if isinstance(value, SecretStr):
            return value.get_secret_value()
        if isinstance(value, Enum):
            return str(value.value)
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    @classmethod
    def _entries(cls, model: type[BaseSettings], *, minimum: bool) -> list[tuple[str, str, str, bool]]:
        """Build the (name, value, description, commented) rows of one settings class.

        Args:
            model: The settings class to render.
            minimum: Keep only the fields marked ``env_required``.

        Returns:
            One row per environment variable.
        """
        prefix = model.model_config.get("env_prefix", "")
        delimiter = model.model_config.get("env_nested_delimiter")
        rows: list[tuple[str, str, str, bool]] = []

        for field_name, field in model.model_fields.items():
            annotation = field.annotation
            if inspect.isclass(annotation) and issubclass(annotation, BaseSettings):
                continue  # Composed settings: rendered under their own class.
            if not prefix and not (field.alias or field.validation_alias):
                continue  # No prefix and no alias: the class supplies its prefix at runtime.

            extra = field.json_schema_extra if isinstance(field.json_schema_extra, dict) else {}
            required = bool(extra.get("env_required"))
            if minimum and not required:
                continue

            name = str(field.alias or field.validation_alias or f"{prefix}{field_name}").upper()
            description = field.description or ""
            nested = cls._nested_rows(field, name, delimiter)
            if nested:
                rows += nested
                continue
            value = str(extra["env_example"]) if "env_example" in extra else cls._render_value(field.default)
            # Nothing to show — an absent default, or one this machine computes
            # (cpu_count) that must not be frozen into the template.
            rows.append((name, value, description, not required and not value))

        return rows

    @classmethod
    def _nested_rows(cls, field: FieldInfo, name: str, delimiter: str | None) -> list[tuple[str, str, str, bool]]:
        """Expand a nested model field into its ``PREFIX__SUBFIELD`` rows.

        Args:
            field: The field to expand.
            name: The env-var name of the field itself.
            delimiter: The class's ``env_nested_delimiter``, if any.

        Returns:
            One commented row per sub-field, empty when the field is not a nested model.
        """
        if delimiter is None:
            return []
        for candidate in get_args(field.annotation):
            if inspect.isclass(candidate) and issubclass(candidate, BaseModel):
                return [
                    (f"{name}{delimiter}{sub}".upper(), "", info.description or "", True)
                    for sub, info in candidate.model_fields.items()
                ]
        return []

    @classmethod
    def render(cls, packages: Sequence[str], *, minimum: bool = False) -> str:
        """Render the whole template.

        Args:
            packages: Importable package or module names to scan.
            minimum: Keep only the fields marked ``env_required``.

        Returns:
            The template contents.
        """
        blocks = [cls._MINIMUM_HEADER if minimum else cls._HEADER]
        models = cls._settings_classes(packages)

        for model in models:
            # A base class's own prefix is never read: every concrete subclass
            # overrides it, so rendering it would document variables nothing uses.
            if any(other is not model and issubclass(other, model) for other in models):
                continue
            rows = cls._entries(model, minimum=minimum)
            if not rows:
                continue
            summary = model.__doc__.strip().splitlines()[0] if model.__doc__ else ""
            lines = [f"\n# ══ {model.__name__} ══════════════════════════════════════════════ #"]
            if summary:
                lines.append(f"# {summary}")
            for name, value, description, commented in rows:
                lines.append("")
                if description:
                    lines.append(f"# {description}")
                lines.append(f"{'#' if commented else ''}{name}={value}")
            blocks.append("\n".join(lines) + "\n")

        return "".join(blocks)

    @classmethod
    def main(cls, argv: Sequence[str] | None = None) -> int:
        """Write the template, or check that the file on disk matches it.

        Args:
            argv: Command-line arguments, defaulting to ``sys.argv``.

        Returns:
            0 on success, 1 when ``--check`` finds the file out of date.
        """
        parser = argparse.ArgumentParser(description="Render a .env template from the settings classes.")
        parser.add_argument("--package", action="append", required=True, dest="packages")
        parser.add_argument("--output", required=True, type=Path)
        parser.add_argument("--minimum", action="store_true")
        parser.add_argument("--check", action="store_true", help="Fail if the file differs instead of writing it.")
        args = parser.parse_args(argv)

        rendered = cls.render(args.packages, minimum=args.minimum)
        if args.check:
            current = args.output.read_text() if args.output.exists() else ""
            if current != rendered:
                sys.stderr.write(f"{args.output} is out of date — run `task env-template`.\n")
                return 1
            return 0

        args.output.write_text(rendered)
        return 0


if __name__ == "__main__":
    sys.exit(EnvTemplate.main())
