"""Setup model types with dynamic schema resolution and tool reference support."""

import copy
import types
import typing
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, cast, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, create_model

from digitalkin.logger import logger
from digitalkin.models.module.tool_cache import ToolCache
from digitalkin.models.module.tool_reference import ToolReference
from digitalkin.models.services.registry import ModuleInfo
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
    """Base setup model with dynamic schema and tool cache support."""

    _clean_model_cache: ClassVar[dict[tuple[type, bool, bool], type]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:  # noqa: ANN401
        """Inject hidden companion fields for ToolReference annotations.

        Args:
            **kwargs: Keyword arguments passed to parent.
        """
        super().__init_subclass__(**kwargs)
        cls._inject_tool_cache_fields()

    @classmethod
    def _inject_tool_cache_fields(cls) -> None:
        """Inject hidden companion fields for ToolReference annotations."""
        annotations = getattr(cls, "__annotations__", {})
        new_annotations: dict[str, Any] = {}

        for field_name, annotation in annotations.items():
            if cls._is_tool_reference_annotation(annotation):
                cache_field_name = f"{field_name}_cache"
                if cache_field_name not in annotations:
                    # Check if it's a list type
                    origin = get_origin(annotation)
                    if origin is list:
                        new_annotations[cache_field_name] = list[ModuleInfo]
                        setattr(
                            cls,
                            cache_field_name,
                            Field(default_factory=list, json_schema_extra={"hidden": True}),
                        )
                    else:
                        new_annotations[cache_field_name] = ModuleInfo | None
                        setattr(
                            cls,
                            cache_field_name,
                            Field(default=None, json_schema_extra={"hidden": True}),
                        )

        if new_annotations:
            cls.__annotations__ = {**annotations, **new_annotations}

    @classmethod
    def _is_tool_reference_annotation(cls, annotation: object) -> bool:
        """Check if annotation is ToolReference or Optional[ToolReference].

        Args:
            annotation: Type annotation to check.

        Returns:
            True if annotation is or contains ToolReference.
        """
        origin = get_origin(annotation)
        if origin is typing.Union or origin is types.UnionType:
            return any(
                arg is ToolReference or (isinstance(arg, type) and issubclass(arg, ToolReference))
                for arg in get_args(annotation)
                if arg is not type(None)
            )
        return annotation is ToolReference or (isinstance(annotation, type) and issubclass(annotation, ToolReference))

    @classmethod
    async def get_clean_model(
        cls,
        *,
        config_fields: bool,
        hidden_fields: bool,
        force: bool = False,
    ) -> "type[SetupModelT]":
        """Build filtered model based on json_schema_extra metadata.

        Args:
            config_fields: Include fields with json_schema_extra["config"] = True.
            hidden_fields: Include fields with json_schema_extra["hidden"] = True.
            force: Refresh dynamic schema fields by calling providers.

        Returns:
            New BaseModel subclass with filtered fields.
        """
        cache_key = (cls, config_fields, hidden_fields)
        if not force and cache_key in cls._clean_model_cache:
            return cast("type[SetupModelT]", cls._clean_model_cache[cache_key])

        clean_fields: dict[str, Any] = {}

        for name, field_info in cls.model_fields.items():
            extra = field_info.json_schema_extra or {}
            is_config = bool(extra.get("config", False)) if isinstance(extra, dict) else False
            is_hidden = bool(extra.get("hidden", False)) if isinstance(extra, dict) else False

            if is_config and not config_fields:
                continue
            if is_hidden and not hidden_fields:
                continue

            current_field_info = field_info
            current_annotation = field_info.annotation

            if force:
                if has_dynamic(field_info):
                    current_field_info = await cls._refresh_field_schema(name, field_info)

                nested_model = cls._get_base_model_type(current_annotation)
                if nested_model is not None:
                    refreshed_nested = await cls._refresh_nested_model(nested_model)
                    if refreshed_nested is not nested_model:
                        current_annotation = refreshed_nested
                        current_field_info = copy.deepcopy(current_field_info)
                        current_field_info.annotation = current_annotation

            clean_fields[name] = (current_annotation, current_field_info)

        root_extra = cls.model_config.get("json_schema_extra", {})

        m = create_model(
            f"{cls.__name__}",
            __base__=SetupModel,
            __config__=ConfigDict(
                arbitrary_types_allowed=True,
                json_schema_extra=copy.deepcopy(root_extra) if isinstance(root_extra, dict) else root_extra,
            ),
            **clean_fields,
        )

        if not force:
            cls._clean_model_cache[cache_key] = m

        return cast("type[SetupModelT]", m)

    @classmethod
    def _get_base_model_type(cls, annotation: "type | None") -> "type[BaseModel] | None":
        """Extract BaseModel type from annotation.

        Args:
            annotation: Type annotation to inspect.

        Returns:
            BaseModel subclass if found, None otherwise.
        """
        if annotation is None:
            return None

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
        args: "tuple[type, ...]",
    ) -> "type[BaseModel] | None":
        """Extract BaseModel from generic type arguments.

        Args:
            origin: Generic origin type (list, dict, Union, etc.).
            args: Type arguments.

        Returns:
            BaseModel subclass if found, None otherwise.
        """
        if origin is typing.Union or origin is types.UnionType:
            return cls._find_base_model_in_args(args)

        if origin in {list, set, frozenset} and args:
            return cls._check_base_model(args[0])

        dict_value_index = 1
        if origin is dict and len(args) > dict_value_index:
            return cls._check_base_model(args[dict_value_index])

        if origin is tuple:
            return cls._find_base_model_in_args(args, skip_ellipsis=True)

        return None

    @classmethod
    def _check_base_model(cls, arg: type) -> "type[BaseModel] | None":
        """Check if arg is a BaseModel subclass.

        Args:
            arg: Type to check.

        Returns:
            The type if it's a BaseModel subclass, None otherwise.
        """
        if isinstance(arg, type) and issubclass(arg, BaseModel):
            return arg
        return None

    @classmethod
    def _find_base_model_in_args(
        cls,
        args: "tuple[type, ...]",
        *,
        skip_ellipsis: bool = False,
    ) -> "type[BaseModel] | None":
        """Find first BaseModel in type args.

        Args:
            args: Type arguments to search.
            skip_ellipsis: Skip ellipsis in tuple types.

        Returns:
            First BaseModel subclass found, None otherwise.
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
    async def _refresh_nested_model(cls, model_cls: "type[BaseModel]") -> "type[BaseModel]":
        """Refresh dynamic fields in a nested BaseModel.

        Args:
            model_cls: Nested model class to refresh.

        Returns:
            New model class with refreshed fields, or original if no changes.
        """
        has_changes = False
        clean_fields: dict[str, Any] = {}

        for name, field_info in model_cls.model_fields.items():
            current_field_info = field_info
            current_annotation = field_info.annotation

            if has_dynamic(field_info):
                current_field_info = await cls._refresh_field_schema(name, field_info)
                has_changes = True

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

        root_extra = cls.model_config.get("json_schema_extra", {})

        return create_model(
            model_cls.__name__,
            __base__=BaseModel,
            __config__=ConfigDict(
                arbitrary_types_allowed=True,
                json_schema_extra=copy.deepcopy(root_extra) if isinstance(root_extra, dict) else root_extra,
            ),
            **clean_fields,
        )

    @classmethod
    async def _refresh_field_schema(cls, field_name: str, field_info: "FieldInfo") -> "FieldInfo":
        """Refresh field's json_schema_extra with values from dynamic providers.

        Args:
            field_name: Name of field being refreshed.
            field_info: Original FieldInfo with dynamic providers.

        Returns:
            New FieldInfo with resolved values, or original if all fetchers fail.
        """
        fetchers = get_fetchers(field_info)

        if not fetchers:
            return field_info

        result = await resolve_safe(fetchers)

        if result.errors:
            for key, error in result.errors.items():
                logger.warning(
                    "Failed to resolve '%s' for field '%s': %s",
                    key,
                    field_name,
                    error,
                )

        if not result.values:
            return field_info

        extra = field_info.json_schema_extra or {}
        new_extra = {**extra, **result.values} if isinstance(extra, dict) else result.values

        new_field_info = copy.deepcopy(field_info)
        new_field_info.json_schema_extra = new_extra
        new_field_info.metadata = [m for m in new_field_info.metadata if not isinstance(m, DynamicField)]

        return new_field_info

    def resolve_tool_references(self, registry: "RegistryStrategy") -> None:
        """Resolve all ToolReference fields recursively.

        Args:
            registry: Registry service for module discovery.
        """
        logger.info("Starting resolve_tool_references")
        self._resolve_tool_references_recursive(self, registry)
        logger.info("Finished resolve_tool_references")

    @classmethod
    def _resolve_tool_references_recursive(
        cls,
        model_instance: BaseModel,
        registry: "RegistryStrategy",
    ) -> None:
        """Recursively resolve ToolReference fields in a model.

        Args:
            model_instance: Model instance to process.
            registry: Registry service for resolution.
        """
        for field_name, field_value in model_instance.__dict__.items():
            if field_value is None:
                continue
            cls._resolve_field_value(field_name, field_value, registry)

    @classmethod
    def _resolve_field_value(
        cls,
        field_name: str,
        field_value: "BaseModel | ToolReference | list | dict",
        registry: "RegistryStrategy",
    ) -> None:
        """Resolve a single field value based on its type.

        Args:
            field_name: Name of the field.
            field_value: Value to process.
            registry: Registry service for resolution.
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
        registry: "RegistryStrategy",
    ) -> None:
        """Resolve a single ToolReference.

        Args:
            field_name: Name of the field for logging.
            tool_ref: ToolReference to resolve.
            registry: Registry service for resolution.
        """
        logger.info("Resolving ToolReference '%s' with module_id='%s'", field_name, tool_ref.config.module_id)
        try:
            tool_ref.resolve(registry)
            logger.info("Resolved ToolReference '%s' -> %s", field_name, tool_ref.module_info)
        except Exception:
            logger.exception("Failed to resolve ToolReference '%s'", field_name)

    @classmethod
    def _resolve_list_items(cls, items: list, registry: "RegistryStrategy") -> None:
        """Resolve ToolReference instances in a list.

        Args:
            items: List of items to process.
            registry: Registry service for resolution.
        """
        for item in items:
            if isinstance(item, ToolReference):
                cls._resolve_single_tool_reference("list_item", item, registry)
            elif isinstance(item, BaseModel):
                cls._resolve_tool_references_recursive(item, registry)

    @classmethod
    def _resolve_dict_values(cls, mapping: dict, registry: "RegistryStrategy") -> None:
        """Resolve ToolReference instances in dict values.

        Args:
            mapping: Dict to process.
            registry: Registry service for resolution.
        """
        for item in mapping.values():
            if isinstance(item, ToolReference):
                cls._resolve_single_tool_reference("dict_value", item, registry)
            elif isinstance(item, BaseModel):
                cls._resolve_tool_references_recursive(item, registry)

    def build_tool_cache(self) -> ToolCache:
        """Build tool cache from resolved ToolReferences, populating companion fields.

        Returns:
            ToolCache with field names as keys and ModuleInfo as values.
        """
        logger.info("Building tool cache")
        cache = ToolCache()
        self._build_tool_cache_recursive(self, cache)
        logger.info("Tool cache built: %d entries", len(cache.entries))
        return cache

    def _build_tool_cache_recursive(self, model_instance: BaseModel, cache: ToolCache) -> None:  # noqa: C901
        """Recursively build tool cache and populate companion fields.

        Args:
            model_instance: Model instance to process.
            cache: ToolCache to populate.
        """
        for field_name, field_value in model_instance.__dict__.items():
            if field_value is None:
                continue
            if isinstance(field_value, ToolReference):
                cache_field_name = f"{field_name}_cache"

                cached_info = getattr(model_instance, cache_field_name, None)
                module_info = field_value.module_info or cached_info
                if module_info:
                    if not cached_info:
                        setattr(model_instance, cache_field_name, module_info)
                    cache.add(module_info.module_id, module_info)
                    logger.debug("Added tool to cache: %s", module_info.module_id)
            elif isinstance(field_value, BaseModel):
                self._build_tool_cache_recursive(field_value, cache)
            elif isinstance(field_value, list):
                cache_field_name = f"{field_name}_cache"
                cached_infos = getattr(model_instance, cache_field_name, None) or []
                resolved_infos: list[ModuleInfo] = []

                for idx, item in enumerate(field_value):
                    if isinstance(item, ToolReference):
                        # Use resolved info or fallback to cached
                        module_info = item.module_info or (cached_infos[idx] if idx < len(cached_infos) else None)
                        if module_info:
                            resolved_infos.append(module_info)
                            cache.add(module_info.module_id, module_info)
                            logger.debug("Added tool to cache: %s", module_info.module_id)
                    elif isinstance(item, BaseModel):
                        self._build_tool_cache_recursive(item, cache)

                # Update companion field with resolved infos
                if resolved_infos:
                    setattr(model_instance, cache_field_name, resolved_infos)
