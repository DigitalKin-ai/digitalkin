"""Setup model types with dynamic schema resolution."""

from __future__ import annotations

import copy
import types
import typing
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, cast, get_args, get_origin

from pydantic import BaseModel, ConfigDict, PrivateAttr, create_model

from digitalkin.logger import logger
from digitalkin.models.module.tool_cache import ToolCache
from digitalkin.models.module.tool_reference import ToolReference
from digitalkin.utils.dynamic_schema import (
    DynamicField,
    get_fetchers,
    has_dynamic,
    resolve_safe,
)

if TYPE_CHECKING:
    from pydantic.fields import FieldInfo

    from digitalkin.services.registry import RegistryStrategy

SetupModelT = TypeVar("SetupModelT", bound="SetupModel")


class SetupModel(BaseModel, Generic[SetupModelT]):
    """Base definition of setup model showing mandatory root fields.

    Optionally, the setup model can define a config option in json_schema_extra
    to be used to initialize the Kin. Supports dynamic schema providers for
    runtime value generation.

    The tool_cache is populated during run_config_setup and contains resolved
    ModuleInfo indexed by slug. It is validated during initialize.

    Attributes:
        model_fields: Inherited from Pydantic BaseModel, contains field definitions.

    See Also:
        - Documentation: docs/api/dynamic_schema.md
        - Tests: tests/modules/test_setup_model.py
    """

    _clean_model_cache: ClassVar[dict[tuple[type, bool, bool], type]] = {}
    _tool_cache: ToolCache = PrivateAttr(default_factory=ToolCache)

    @classmethod
    async def get_clean_model(
        cls,
        *,
        config_fields: bool,
        hidden_fields: bool,
        force: bool = False,
    ) -> type[SetupModelT]:
        """Dynamically builds and returns a new BaseModel subclass with filtered fields.

        This method filters fields based on their `json_schema_extra` metadata:
        - Fields with `{"config": True}` are included only when `config_fields=True`
        - Fields with `{"hidden": True}` are included only when `hidden_fields=True`

        When `force=True`, fields with dynamic schema providers will have their
        providers called to fetch fresh values for schema metadata like enums.
        This includes recursively processing nested BaseModel fields.

        Args:
            config_fields: If True, include fields marked with `{"config": True}`.
                These are typically initial configuration fields.
            hidden_fields: If True, include fields marked with `{"hidden": True}`.
                These are typically runtime-only fields not shown in initial config.
            force: If True, refresh dynamic schema fields by calling their providers.
                Use this when you need up-to-date values from external sources like
                databases or APIs. Default is False for performance.

        Returns:
            A new BaseModel subclass with filtered fields.
        """
        # Check cache for non-forced requests
        cache_key = (cls, config_fields, hidden_fields)
        if not force and cache_key in cls._clean_model_cache:
            return cast("type[SetupModelT]", cls._clean_model_cache[cache_key])

        clean_fields: dict[str, Any] = {}

        for name, field_info in cls.model_fields.items():
            extra = field_info.json_schema_extra or {}
            is_config = bool(extra.get("config", False)) if isinstance(extra, dict) else False
            is_hidden = bool(extra.get("hidden", False)) if isinstance(extra, dict) else False

            # Skip config unless explicitly included
            if is_config and not config_fields:
                logger.debug("Skipping '%s' (config-only)", name)
                continue

            # Skip hidden unless explicitly included
            if is_hidden and not hidden_fields:
                logger.debug("Skipping '%s' (hidden-only)", name)
                continue

            # Refresh dynamic schema fields when force=True
            current_field_info = field_info
            current_annotation = field_info.annotation

            if force:
                # Check if this field has DynamicField metadata
                if has_dynamic(field_info):
                    current_field_info = await cls._refresh_field_schema(name, field_info)

                # Check if the annotation is a nested BaseModel that might have dynamic fields
                nested_model = cls._get_base_model_type(current_annotation)
                if nested_model is not None:
                    refreshed_nested = await cls._refresh_nested_model(nested_model)
                    if refreshed_nested is not nested_model:
                        # Update annotation to use refreshed nested model
                        current_annotation = refreshed_nested
                        # Create new field_info with updated annotation (deep copy for safety)
                        current_field_info = copy.deepcopy(current_field_info)
                        current_field_info.annotation = current_annotation

            clean_fields[name] = (current_annotation, current_field_info)

        # Dynamically create a model e.g. "SetupModel"
        m = create_model(
            f"{cls.__name__}",
            __base__=BaseModel,
            __config__=ConfigDict(arbitrary_types_allowed=True),
            **clean_fields,
        )

        # Cache for non-forced requests
        if not force:
            cls._clean_model_cache[cache_key] = m

        return cast("type[SetupModelT]", m)

    @classmethod
    def _get_base_model_type(cls, annotation: type | None) -> type[BaseModel] | None:
        """Extract BaseModel type from an annotation.

        Handles direct types, Optional, Union, list, dict, set, tuple, and other generics.

        Args:
            annotation: The type annotation to inspect.

        Returns:
            The BaseModel subclass if found, None otherwise.
        """
        if annotation is None:
            return None

        # Direct BaseModel subclass check
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            return annotation

        origin = get_origin(annotation)
        if origin is None:
            return None

        args = get_args(annotation)
        return cls._extract_base_model_from_args(origin, args)

    @classmethod
    def _extract_base_model_from_args(
        cls,
        origin: type,
        args: tuple[type, ...],
    ) -> type[BaseModel] | None:
        """Extract BaseModel from generic type arguments.

        Args:
            origin: The generic origin type (list, dict, Union, etc.).
            args: The type arguments.

        Returns:
            The BaseModel subclass if found, None otherwise.
        """
        # Union/Optional: check each arg (supports both typing.Union and types.UnionType)
        # Python 3.10+ uses types.UnionType for X | Y syntax
        if origin is typing.Union or origin is types.UnionType:
            return cls._find_base_model_in_args(args)

        # list, set, frozenset: check first arg
        if origin in {list, set, frozenset} and args:
            return cls._check_base_model(args[0])

        # dict: check value type (second arg)
        dict_value_index = 1
        if origin is dict and len(args) > dict_value_index:
            return cls._check_base_model(args[dict_value_index])

        # tuple: check first non-ellipsis arg
        if origin is tuple:
            return cls._find_base_model_in_args(args, skip_ellipsis=True)

        return None

    @classmethod
    def _check_base_model(cls, arg: type) -> type[BaseModel] | None:
        """Check if arg is a BaseModel subclass.

        Returns:
            The BaseModel subclass if arg is one, None otherwise.
        """
        if isinstance(arg, type) and issubclass(arg, BaseModel):
            return arg
        return None

    @classmethod
    def _find_base_model_in_args(
        cls,
        args: tuple[type, ...],
        *,
        skip_ellipsis: bool = False,
    ) -> type[BaseModel] | None:
        """Find first BaseModel in args.

        Returns:
            The first BaseModel subclass found, None otherwise.
        """
        for arg in args:
            if arg is type(None):
                continue
            if skip_ellipsis and arg is ...:
                continue
            result = cls._check_base_model(arg)
            if result is not None:
                return result
        return None

    @classmethod
    async def _refresh_nested_model(cls, model_cls: type[BaseModel]) -> type[BaseModel]:
        """Refresh dynamic fields in a nested BaseModel.

        Creates a new model class with all DynamicField metadata resolved.

        Args:
            model_cls: The nested model class to refresh.

        Returns:
            A new model class with refreshed fields, or the original if no changes.
        """
        has_changes = False
        clean_fields: dict[str, Any] = {}

        for name, field_info in model_cls.model_fields.items():
            current_field_info = field_info
            current_annotation = field_info.annotation

            # Check if field has DynamicField metadata
            if has_dynamic(field_info):
                current_field_info = await cls._refresh_field_schema(name, field_info)
                has_changes = True

            # Recursively check nested models
            nested_model = cls._get_base_model_type(current_annotation)
            if nested_model is not None:
                refreshed_nested = await cls._refresh_nested_model(nested_model)
                if refreshed_nested is not nested_model:
                    current_annotation = refreshed_nested
                    current_field_info = copy.deepcopy(current_field_info)
                    current_field_info.annotation = current_annotation
                    has_changes = True

            clean_fields[name] = (current_annotation, current_field_info)

        if not has_changes:
            return model_cls

        # Create new model with refreshed fields
        logger.debug("Creating refreshed nested model for '%s'", model_cls.__name__)
        return create_model(
            model_cls.__name__,
            __base__=BaseModel,
            __config__=ConfigDict(arbitrary_types_allowed=True),
            **clean_fields,
        )

    @classmethod
    async def _refresh_field_schema(cls, field_name: str, field_info: FieldInfo) -> FieldInfo:
        """Refresh a field's json_schema_extra with fresh values from dynamic providers.

        This method calls all dynamic providers registered for a field (via Annotated
        metadata) and creates a new FieldInfo with the resolved values. The original
        field_info is not modified.

        Uses `resolve_safe()` for structured error handling, allowing partial success
        when some fetchers fail. Successfully resolved values are still applied.

        Args:
            field_name: The name of the field being refreshed (used for logging).
            field_info: The original FieldInfo object containing the dynamic providers.

        Returns:
            A new FieldInfo object with the same attributes as the original, but with
            `json_schema_extra` containing resolved values and Dynamic metadata removed.

        Note:
            If all fetchers fail, the original field_info is returned unchanged.
            If some fetchers fail, successfully resolved values are still applied.
        """
        fetchers = get_fetchers(field_info)

        if not fetchers:
            return field_info

        fetcher_keys = list(fetchers.keys())
        logger.debug(
            "Refreshing dynamic schema for field '%s' with fetchers: %s",
            field_name,
            fetcher_keys,
            extra={"field_name": field_name, "fetcher_keys": fetcher_keys},
        )

        # Resolve all fetchers with structured error handling
        result = await resolve_safe(fetchers)

        # Log any errors that occurred with full details
        if result.errors:
            for key, error in result.errors.items():
                logger.warning(
                    "Failed to resolve '%s' for field '%s': %s: %s",
                    key,
                    field_name,
                    type(error).__name__,
                    str(error) or "(no message)",
                    extra={
                        "field_name": field_name,
                        "fetcher_key": key,
                        "error_type": type(error).__name__,
                        "error_message": str(error),
                        "error_repr": repr(error),
                    },
                )

        # If no values were resolved, return original field_info
        if not result.values:
            logger.warning(
                "All fetchers failed for field '%s', keeping original",
                field_name,
            )
            return field_info

        # Build new json_schema_extra with resolved values merged
        extra = field_info.json_schema_extra or {}
        new_extra = {**extra, **result.values} if isinstance(extra, dict) else result.values

        # Create a deep copy of the FieldInfo to avoid shared mutable state
        new_field_info = copy.deepcopy(field_info)
        new_field_info.json_schema_extra = new_extra

        # Remove Dynamic from metadata (it's been resolved)
        new_metadata = [m for m in new_field_info.metadata if not isinstance(m, DynamicField)]
        new_field_info.metadata = new_metadata

        logger.debug(
            "Refreshed '%s' with dynamic values: %s",
            field_name,
            list(result.values.keys()),
        )

        return new_field_info

    def resolve_tool_references(self, registry: RegistryStrategy) -> None:
        """Resolve all ToolReference fields in this setup instance.

        Recursively walks through all fields, including nested BaseModel instances,
        and resolves any ToolReference fields using the provided registry.

        Args:
            registry: Registry service to use for resolution.
        """
        self._resolve_tool_references_recursive(self, registry)

    @classmethod
    def _resolve_tool_references_recursive(
        cls,
        model_instance: BaseModel,
        registry: RegistryStrategy,
    ) -> None:
        """Recursively resolve ToolReference fields in a model instance.

        Args:
            model_instance: The model instance to process.
            registry: Registry service to use for resolution.
        """
        for field_name, field_value in model_instance.__dict__.items():
            if field_value is None:
                continue

            cls._resolve_field_value(field_name, field_value, registry)

    @classmethod
    def _resolve_field_value(
        cls,
        field_name: str,
        field_value: BaseModel | ToolReference | list | dict,
        registry: RegistryStrategy,
    ) -> None:
        """Resolve a single field value, handling different types.

        Args:
            field_name: Name of the field for logging.
            field_value: The value to process.
            registry: Registry service to use for resolution.
        """
        if isinstance(field_value, ToolReference):
            cls._resolve_single_tool_reference(field_name, field_value, registry)
        elif isinstance(field_value, BaseModel):
            cls._resolve_tool_references_recursive(field_value, registry)
        elif isinstance(field_value, list):
            cls._resolve_list_items(field_value, registry)
        elif isinstance(field_value, dict):
            cls._resolve_dict_values(field_value, registry)

    @classmethod
    def _resolve_single_tool_reference(
        cls,
        field_name: str,
        tool_ref: ToolReference,
        registry: RegistryStrategy,
    ) -> None:
        """Resolve a single ToolReference instance.

        Args:
            field_name: Name of the field for logging.
            tool_ref: The ToolReference instance.
            registry: Registry service to use for resolution.
        """
        try:
            tool_ref.resolve(registry)
            logger.debug(
                "Resolved ToolReference field '%s'",
                field_name,
                extra={"field_name": field_name, "mode": tool_ref.config.mode.value},
            )
        except Exception:
            logger.exception(
                "Failed to resolve ToolReference field '%s'",
                field_name,
                extra={"field_name": field_name, "config": tool_ref.config.model_dump()},
            )

    @classmethod
    def _resolve_list_items(
        cls,
        items: list,
        registry: RegistryStrategy,
    ) -> None:
        """Resolve ToolReference instances in a list.

        Args:
            items: List of items to process.
            registry: Registry service to use for resolution.
        """
        for item in items:
            if isinstance(item, ToolReference):
                cls._resolve_single_tool_reference("list_item", item, registry)
            elif isinstance(item, BaseModel):
                cls._resolve_tool_references_recursive(item, registry)

    @classmethod
    def _resolve_dict_values(
        cls,
        mapping: dict,
        registry: RegistryStrategy,
    ) -> None:
        """Resolve ToolReference instances in a dict's values.

        Args:
            mapping: Dict to process.
            registry: Registry service to use for resolution.
        """
        for item in mapping.values():
            if isinstance(item, ToolReference):
                cls._resolve_single_tool_reference("dict_value", item, registry)
            elif isinstance(item, BaseModel):
                cls._resolve_tool_references_recursive(item, registry)

    @property
    def tool_cache(self) -> ToolCache:
        """Get the tool cache for this setup instance.

        Returns:
            The ToolCache containing resolved tools.
        """
        return self._tool_cache

    def build_tool_cache(self) -> ToolCache:
        """Build the tool cache from resolved ToolReferences.

        This should be called during run_config_setup after resolve_tool_references.
        It walks all ToolReference fields and adds resolved ones to the cache.

        Returns:
            The populated ToolCache.
        """
        self._build_tool_cache_recursive(self)
        logger.debug(
            "Tool cache built",
            extra={"slugs": self._tool_cache.list_slugs()},
        )
        return self._tool_cache

    def _build_tool_cache_recursive(self, model_instance: BaseModel) -> None:
        """Recursively build tool cache from model fields.

        Args:
            model_instance: The model instance to process.
        """
        for field_name, field_value in model_instance.__dict__.items():
            if field_value is None:
                continue

            if isinstance(field_value, ToolReference):
                self._add_tool_reference_to_cache(field_name, field_value)
            elif isinstance(field_value, BaseModel):
                self._build_tool_cache_recursive(field_value)
            elif isinstance(field_value, list):
                self._build_tool_cache_from_list(field_value)
            elif isinstance(field_value, dict):
                self._build_tool_cache_from_dict(field_value)

    def _add_tool_reference_to_cache(self, field_name: str, tool_ref: ToolReference) -> None:
        """Add a resolved ToolReference to the cache.

        Args:
            field_name: Name of the field (used as fallback slug).
            tool_ref: The ToolReference instance.
        """
        if tool_ref.module_info:
            # Use slug from config, or field name as fallback
            slug = tool_ref.slug or field_name
            self._tool_cache.add(slug, tool_ref.module_info)

    def _build_tool_cache_from_list(self, items: list) -> None:
        """Build tool cache from list items.

        Args:
            items: List of items to process.
        """
        for idx, item in enumerate(items):
            if isinstance(item, ToolReference):
                self._add_tool_reference_to_cache(f"list_{idx}", item)
            elif isinstance(item, BaseModel):
                self._build_tool_cache_recursive(item)

    def _build_tool_cache_from_dict(self, mapping: dict) -> None:
        """Build tool cache from dict values.

        Args:
            mapping: Dict to process.
        """
        for key, item in mapping.items():
            if isinstance(item, ToolReference):
                self._add_tool_reference_to_cache(str(key), item)
            elif isinstance(item, BaseModel):
                self._build_tool_cache_recursive(item)

    def validate_tool_cache(self, registry: RegistryStrategy) -> list[str]:
        """Validate all cached tools are still available.

        Should be called during initialize to ensure tools are still valid.

        Args:
            registry: Registry to validate against.

        Returns:
            List of slugs that are no longer valid.
        """
        return self._tool_cache.validate(registry)
