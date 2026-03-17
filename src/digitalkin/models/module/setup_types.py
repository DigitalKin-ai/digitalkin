"""Setup model types with dynamic schema resolution and tool reference support."""

import copy
import types
import typing
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, cast, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, create_model

from digitalkin.logger import logger
from digitalkin.models.module.tool_cache import ToolCache, ToolModuleInfo
from digitalkin.models.module.tool_reference import ToolReference
from digitalkin.utils.dynamic_schema import (
    DynamicField,
    get_fetchers,
    has_dynamic,
    resolve_safe,
)

if TYPE_CHECKING:
    from pydantic.fields import FieldInfo

    from digitalkin.services.communication import CommunicationStrategy
    from digitalkin.services.registry import RegistryStrategy

SetupModelT = TypeVar("SetupModelT", bound="SetupModel")


class SetupModel(BaseModel, Generic[SetupModelT]):
    """Base setup model with dynamic schema and tool cache support."""

    _clean_model_cache: ClassVar[dict[tuple[type, bool, bool], type]] = {}
    resolved_tools: dict[str, ToolModuleInfo] = Field(
        default_factory=dict,
        json_schema_extra={"ui:widget": "hidden"},
    )

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
            hidden_fields: Include fields with json_schema_extra["ui:widget"] = "hidden".
            force: Refresh dynamic schema fields by calling providers.

        Returns:
            New BaseModel subclass with filtered fields.
        """
        cache_key = (cls, config_fields, hidden_fields)
        if not force and cache_key in cls._clean_model_cache:
            return cast("type[SetupModelT]", cls._clean_model_cache[cache_key])

        clean_fields: dict[str, Any] = {}
        excluded_fields: set[str] = set()

        for name, field_info in cls.model_fields.items():
            extra = field_info.json_schema_extra or {}
            is_config = bool(extra.get("config", False)) if isinstance(extra, dict) else False
            is_hidden = (extra.get("ui:widget") == "hidden") if isinstance(extra, dict) else False

            if is_config and not config_fields:
                excluded_fields.add(name)
                continue
            if is_hidden and not hidden_fields:
                excluded_fields.add(name)
                continue

            current_field_info = field_info
            current_annotation = field_info.annotation

            if force:
                if has_dynamic(field_info):
                    current_field_info = await cls._refresh_field_schema(name, field_info)

                if (nested_model := cls._get_base_model_type(current_annotation)) is not None:
                    refreshed_nested = await cls._refresh_nested_model(nested_model)
                    if refreshed_nested is not nested_model:
                        current_annotation = refreshed_nested
                        current_field_info = copy.deepcopy(current_field_info)
                        current_field_info.annotation = current_annotation

            clean_fields[name] = (current_annotation, current_field_info)

        root_extra = cls.model_config.get("json_schema_extra", {})

        extra_bases = tuple(b for b in cls.__bases__ if b is not SetupModel)
        base: type | tuple[type, ...] = (SetupModel, *extra_bases) if extra_bases else SetupModel

        m: type[SetupModel] = create_model(  # type: ignore[assignment]
            f"{cls.__name__}",
            __base__=base,
            __config__=ConfigDict(
                arbitrary_types_allowed=True,
                json_schema_extra=copy.deepcopy(root_extra) if isinstance(root_extra, dict) else root_extra,
            ),
            **clean_fields,
        )

        cls._remove_excluded_inherited_fields(m, excluded_fields, clean_fields)

        if not force:
            cls._clean_model_cache[cache_key] = m

        return cast("type[SetupModelT]", m)

    @staticmethod
    def _remove_excluded_inherited_fields(
        model: type[BaseModel],
        excluded_fields: set[str],
        clean_fields: dict[str, Any],
    ) -> None:
        """Remove inherited fields that were excluded by the filter.

        Args:
            model: The created model class.
            excluded_fields: Field names that should be excluded.
            clean_fields: Fields explicitly included in the model.
        """
        removed = False
        for field_name in excluded_fields:
            if field_name in model.model_fields and field_name not in clean_fields:
                del model.model_fields[field_name]
                removed = True
        if removed:
            model.model_rebuild(force=True)

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
            if (result := cls._check_base_model(arg)) is not None:
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

            if (nested_model := cls._get_base_model_type(current_annotation)) is not None:
                refreshed_nested = await cls._refresh_nested_model(nested_model)
                if refreshed_nested is not nested_model:
                    current_annotation = refreshed_nested
                    current_field_info = copy.deepcopy(current_field_info)
                    current_field_info.annotation = current_annotation
                    has_changes = True

            clean_fields[name] = (current_annotation, current_field_info)

        if not has_changes:
            return model_cls

        root_extra = model_cls.model_config.get("json_schema_extra", {})

        extra_bases = tuple(b for b in model_cls.__bases__ if b is not BaseModel)
        base: type | tuple[type, ...] = (BaseModel, *extra_bases) if extra_bases else BaseModel

        return create_model(
            model_cls.__name__,
            __base__=base,
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

    async def build_tool_cache(
        self,
        registry: "RegistryStrategy | None" = None,
        communication: "CommunicationStrategy | None" = None,
    ) -> ToolCache:
        """Build tool cache, resolving uncached tools via registry.

        Walks ToolReference fields recursively. For each selected tool,
        checks resolved_tools first (cache). If missing and registry is
        available, resolves via gRPC and populates the cache.

        Args:
            registry: Registry service for resolving uncached tools.
            communication: Communication service for module schemas.

        Returns:
            ToolCache with resolved tool entries.
        """
        cache = ToolCache()
        await self._collect_tools_recursive(self, cache, registry, communication)
        logger.info("Tool cache built: %d entries", len(cache.entries))
        return cache

    async def _collect_tools_recursive(
        self,
        model_instance: BaseModel,
        cache: ToolCache,
        registry: "RegistryStrategy | None",
        communication: "CommunicationStrategy | None",
    ) -> None:
        """Recursively walk model fields to find and resolve ToolReferences.

        Args:
            model_instance: Model instance to walk.
            cache: ToolCache to populate.
            registry: Optional registry for resolving uncached tools.
            communication: Optional communication for module schemas.
        """
        for field_name, field_value in model_instance.__dict__.items():
            if field_value is None:
                continue
            if isinstance(field_value, ToolReference):
                await self._collect_from_tool_ref(field_name, field_value, cache, registry, communication)
            elif isinstance(field_value, BaseModel):
                await self._collect_tools_recursive(field_value, cache, registry, communication)
            elif isinstance(field_value, (list, dict)):
                items = field_value if isinstance(field_value, list) else field_value.values()
                for item in items:
                    if isinstance(item, ToolReference):
                        await self._collect_from_tool_ref(field_name, item, cache, registry, communication)
                    elif isinstance(item, BaseModel):
                        await self._collect_tools_recursive(item, cache, registry, communication)

    async def _collect_from_tool_ref(
        self,
        field_name: str,
        tool_ref: ToolReference,
        cache: ToolCache,
        registry: "RegistryStrategy | None",
        communication: "CommunicationStrategy | None",
    ) -> None:
        """Resolve and cache tools from a single ToolReference.

        Args:
            field_name: Field name for logging.
            tool_ref: ToolReference to process.
            cache: ToolCache to populate.
            registry: Optional registry for resolution.
            communication: Optional communication for schemas.
        """
        if not tool_ref.selected_tools:
            return

        # Resolve uncached entries via registry
        has_uncached = any(
            entry.setup_id and entry.setup_id not in self.resolved_tools for entry in tool_ref.selected_tools
        )
        if has_uncached and registry and communication:
            try:
                infos = await tool_ref.resolve(registry, communication)
                for info in infos:
                    self.resolved_tools[info.setup_id] = info
                    logger.info("Resolved tool '%s' -> module_id=%s", info.setup_id, info.module_id)
            except Exception:
                logger.exception("Failed to resolve ToolReference '%s'", field_name)

        # Add all resolved entries to cache
        for entry in tool_ref.selected_tools:
            tool_info = self.resolved_tools.get(entry.setup_id) if entry.setup_id else None
            if tool_info:
                cache.add(tool_info)
