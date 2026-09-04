"""Setup model types with dynamic schema resolution and tool reference support."""

import asyncio
import copy
import types
import typing
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, cast, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, create_model

from digitalkin.logger import logger
from digitalkin.models.module.tool_cache import ToolCache, ToolModuleInfo
from digitalkin.models.module.tool_reference import ToolReference
from digitalkin.utils.dynamic_schema import DynamicField, DynamicSchemaResolver

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from pydantic.fields import FieldInfo

    from digitalkin.services.communication import CommunicationStrategy
    from digitalkin.services.registry import RegistryStrategy

SetupModelT = TypeVar("SetupModelT", bound="SetupModel")


class SetupModel(BaseModel, Generic[SetupModelT]):
    """Base setup model with dynamic schema and tool cache support."""

    _clean_model_cache: ClassVar[dict[tuple[type, bool, bool], type]] = {}
    _CLEAN_MODEL_CACHE_MAX: ClassVar[int] = 64
    resolved_tools: dict[str, ToolModuleInfo] = Field(
        default_factory=dict,
        json_schema_extra={"ui:widget": "hidden"},
        exclude=True,
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
                if DynamicSchemaResolver.has_dynamic(field_info):
                    current_field_info = await cls._refresh_field_schema(name, field_info)

                refreshed_annotation = await cls._refresh_annotation(current_annotation)
                if refreshed_annotation is not None:
                    current_annotation = refreshed_annotation
                    current_field_info = copy.deepcopy(current_field_info)
                    current_field_info.annotation = current_annotation

            clean_fields[name] = (current_annotation, current_field_info)

        root_extra = cls.model_config.get("json_schema_extra", {})

        extra_bases = tuple(b for b in cls.__bases__ if b is not SetupModel)
        base: type | tuple[type, ...] = (SetupModel, *extra_bases) if extra_bases else SetupModel

        m: type[SetupModel] = create_model(
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
            if len(cls._clean_model_cache) >= cls._CLEAN_MODEL_CACHE_MAX:
                del cls._clean_model_cache[next(iter(cls._clean_model_cache))]
            cls._clean_model_cache[cache_key] = m

        return cast("type[SetupModelT]", m)

    @classmethod
    def clear_clean_model_cache(cls) -> None:
        """Clear the filtered model cache. Called by cache invalidation."""
        cls._clean_model_cache.clear()

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
    def _find_all_base_models_in_args(
        cls,
        args: "tuple[type, ...]",
    ) -> "list[type[BaseModel]]":
        """Find all BaseModel subclasses in type args.

        Args:
            args: Type arguments to search.

        Returns:
            List of BaseModel subclasses found.
        """
        results: list[type[BaseModel]] = []
        for arg in args:
            if arg is type(None):
                continue
            if (result := cls._check_base_model(arg)) is not None:
                results.append(result)
        return results

    @classmethod
    async def _refresh_annotation(cls, annotation: "type | None") -> "type | None":
        """Refresh dynamic fields in an annotation, handling Unions and single models.

        Args:
            annotation: Type annotation to refresh.

        Returns:
            Refreshed annotation, or None if no changes were made.
        """
        if annotation is None:
            return None

        refreshed_union = await cls._refresh_union_variants(annotation)
        if refreshed_union is not None:
            return refreshed_union

        nested_model = cls._get_base_model_type(annotation)
        if nested_model is None:
            return None

        refreshed = await cls._refresh_nested_model(nested_model)
        if refreshed is nested_model:
            return None

        return cls._rebuild_generic_annotation(annotation, nested_model, refreshed)

    @classmethod
    def _rebuild_generic_annotation(
        cls, annotation: type, original: "type[BaseModel]", refreshed: "type[BaseModel]"
    ) -> type:
        """Rebuild a generic annotation replacing the original BaseModel with the refreshed one.

        Args:
            annotation: Original generic annotation (e.g. list[MyModel]).
            original: The original BaseModel subclass found in the annotation.
            refreshed: The refreshed replacement model.

        Returns:
            Rebuilt annotation, or the refreshed model if annotation is not generic.
        """
        origin = get_origin(annotation)
        if origin is None:
            return refreshed

        args = get_args(annotation)
        new_args = tuple(refreshed if a is original else a for a in args)
        if origin in {list, set, frozenset}:
            return origin[new_args[0]]
        if origin is dict:
            return dict[new_args[0], new_args[1]]  # type: ignore[valid-type]
        if origin is tuple:
            return tuple[new_args]  # type: ignore[valid-type]
        return refreshed

    @classmethod
    async def _refresh_union_variants(
        cls,
        annotation: "type",
    ) -> "type | None":
        """Refresh dynamic fields in all BaseModel variants of a Union annotation.

        Args:
            annotation: A Union type annotation potentially containing multiple BaseModel subclasses.

        Returns:
            Rebuilt annotation with refreshed variants, or None if no changes were made.
        """
        origin = get_origin(annotation)
        if origin is not typing.Union and origin is not types.UnionType:
            return None

        args = get_args(annotation)
        models = cls._find_all_base_models_in_args(args)
        if len(models) <= 1:
            return None

        replacements: dict[type, type] = {}
        for model in models:
            refreshed = await cls._refresh_nested_model(model)
            if refreshed is not model:
                replacements[model] = refreshed

        if not replacements:
            return None

        new_args = [replacements.get(a, a) for a in args]
        rebuilt = new_args[0]
        for arg in new_args[1:]:
            rebuilt |= arg
        return rebuilt

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

            if DynamicSchemaResolver.has_dynamic(field_info):
                current_field_info = await cls._refresh_field_schema(name, field_info)
                has_changes = True

            refreshed_annotation = await cls._refresh_annotation(current_annotation)
            if refreshed_annotation is not None:
                current_annotation = refreshed_annotation
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
        fetchers = DynamicSchemaResolver.get_fetchers(field_info)

        if not fetchers:
            return field_info

        result = await DynamicSchemaResolver.resolve_safe(fetchers)

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
        """Build tool cache, resolving tools via registry.

        ``resolved_tools`` is a within-build dedup cache, not a cross-request
        store: when a registry is available it is cleared first so a stale or
        empty entry can never be served — every build re-resolves. Without a
        registry the existing entries are kept (degraded/embedded path).

        Args:
            registry: Registry service for resolving uncached tools.
            communication: Communication service for module schemas.

        Returns:
            ToolCache with resolved tool entries.
        """
        if registry and communication:
            self.resolved_tools.clear()
        cache = ToolCache()
        await self._collect_tools_recursive(self, cache, registry, communication)
        counts = " ".join(f"{sid}={len(info.tools)}" for sid, info in cache.entries.items())
        logger.info("Tool cache built: %d entries [%s]", len(cache.entries), counts)
        return cache

    async def _collect_tools_recursive(  # noqa: C901
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
        # Gather across ToolReferences so multiple refs don't serialise their RPCs.
        tool_ref_tasks: list[Awaitable[None]] = []
        nested_models: list[BaseModel] = []
        for field_name, field_value in model_instance.__dict__.items():
            if field_value is None:
                continue
            if isinstance(field_value, ToolReference):
                tool_ref_tasks.append(
                    self._collect_from_tool_ref(field_name, field_value, cache, registry, communication)
                )
            elif isinstance(field_value, BaseModel):
                nested_models.append(field_value)
            elif isinstance(field_value, (list, dict)):
                items = field_value if isinstance(field_value, list) else field_value.values()
                for item in items:
                    if isinstance(item, ToolReference):
                        tool_ref_tasks.append(
                            self._collect_from_tool_ref(field_name, item, cache, registry, communication)
                        )
                    elif isinstance(item, BaseModel):
                        nested_models.append(item)
        if tool_ref_tasks:
            await asyncio.gather(*tool_ref_tasks)
        if nested_models:
            await asyncio.gather(
                *(self._collect_tools_recursive(m, cache, registry, communication) for m in nested_models),
            )

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

        has_uncached = any(
            entry.setup_id and entry.setup_id not in self.resolved_tools for entry in tool_ref.selected_tools
        )
        if has_uncached and registry and communication:
            try:
                infos = await tool_ref.resolve(registry, communication, trim=False)
                for info in infos:
                    self.resolved_tools[info.setup_id] = info
                    logger.debug("Resolved tool '%s' -> module_id=%s", info.setup_id, info.module_id)
            except Exception:
                logger.exception("Failed to resolve ToolReference '%s'", field_name)

        missing: list[str] = []
        for entry in tool_ref.selected_tools:
            tool_info = self.resolved_tools.get(entry.setup_id) if entry.setup_id else None
            if tool_info is None:
                if entry.setup_id:
                    missing.append(entry.setup_id)
                continue
            cache.add(tool_info)

        if missing:
            logger.warning(
                "ToolReference '%s' has %d unresolved setup_id(s): %s "
                "(each has an upstream 'Tool resolve failed' log with the reason)",
                field_name,
                len(missing),
                missing,
            )
