"""Tool reference types for module configuration."""

import asyncio
import logging
from typing import Annotated

from pydantic import AfterValidator, BaseModel, Field, PlainSerializer, model_validator
from pydantic.annotated_handlers import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

from digitalkin.logger import logger
from digitalkin.models.module.tool_cache import ToolModuleInfo
from digitalkin.models.settings.module import get_module_settings
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.registry import RegistryStrategy


class ToolSelection(BaseModel):
    """Single tool selection with trigger filtering."""

    setup_id: str = Field(description="Setup ID of the selected tool.")
    triggers: dict[str, bool] = Field(min_length=1, max_length=100, description="Trigger protocols with enabled state.")


class ToolReference(BaseModel):
    """Tool selection containing setup IDs and trigger filters."""

    selected_tools: list[ToolSelection] = Field(
        default_factory=list, description="Selected tools with trigger filters."
    )

    @model_validator(mode="before")
    @classmethod
    def _drop_blank_selections(cls, data: object) -> object:
        """Drop (and log) tool selections with an empty setup_id from raw list input.

        react-jsonschema-form sends selections as a list; a placeholder row with no
        ``setupId`` would otherwise become ``setup_id=""`` and hit ``get_setup("")``.

        Args:
            data: Raw validation input — a list of selection dicts from the frontend, or a dict.

        Returns:
            ``{"selected_tools": [...]}`` with blanks removed for list input; data unchanged otherwise.
        """
        if not isinstance(data, list):
            return data
        kept: list[object] = []
        for e in data:
            if isinstance(e, dict):
                sid = (e.get("setup_id") or e.get("setupId") or "").strip()
                if sid:
                    kept.append({"setup_id": sid, "triggers": e.get("triggers", {})})
                    continue
            elif isinstance(e, ToolSelection):
                if e.setup_id.strip():
                    kept.append(e)
                    continue
            else:
                kept.append(e)
                continue
            logger.info("tool_reference_input: dropped incomplete tool selection (empty setup_id): %r", e)
        return {"selected_tools": kept}

    async def resolve(
        self,
        registry: RegistryStrategy,
        communication: CommunicationStrategy,
        *,
        trim: bool = True,
    ) -> list[ToolModuleInfo]:
        """Resolve selected tools using the registry.

        Each tool resolution is bounded by ``DIGITALKIN_MODULE_TOOL_RESOLVE_TIMEOUT``
        (default 10s). Inputs that fail to resolve are logged as WARNING with the
        ``setup_id`` and a ``reason=...`` field; the returned list contains only
        successful ``ToolModuleInfo``s. The caller can correlate against
        ``self.selected_tools`` by ``setup_id``.

        Args:
            registry: Registry service for module discovery.
            communication: Communication service for module schemas.
            trim: When True, each result is trimmed (on a copy) to the entry's enabled
                triggers. When False, the full module catalog is returned — used to
                populate the shared per-``setup_id`` cache so agents sharing a
                ``setup_id`` with disjoint triggers keep the full catalog; per-agent
                filtering then happens at the consumer.

        Returns:
            List of resolved ``ToolModuleInfo``. Failed resolutions are logged and omitted.
        """
        timeout = get_module_settings().tool_resolve_timeout

        async def _bounded(entry: ToolSelection) -> ToolModuleInfo | None:
            try:
                tool_info = await asyncio.wait_for(
                    ToolReference._resolve_single(entry, registry, communication),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Tool resolve failed: setup_id=%s reason=resolve_timeout timeout_s=%.1f",
                    entry.setup_id,
                    timeout,
                )
                return None
            except Exception:
                logger.exception(
                    "Tool resolve failed: setup_id=%s reason=resolve_exception",
                    entry.setup_id,
                )
                return None
            if tool_info is None or not trim:
                return tool_info
            enabled = {name for name, on in entry.triggers.items() if on}
            if not enabled:
                return tool_info
            return tool_info.model_copy(
                update={"tools": [t for t in tool_info.tools if t.name in enabled]},
            )

        results = await asyncio.gather(*(_bounded(e) for e in self.selected_tools if e.setup_id.strip()))
        return [r for r in results if r is not None]

    @staticmethod
    async def _resolve_single(
        entry: "ToolSelection",
        registry: RegistryStrategy,
        communication: CommunicationStrategy,
    ) -> ToolModuleInfo | None:
        """Resolve a single tool selection; emit one structured audit line per call.

        Every failure path logs a ``WARNING`` with a ``reason=...`` field
        (``setup_not_found``, ``module_not_discovered``, ``schema_fetch_failed``)
        so callers don't need to re-derive the cause. Successful resolutions
        emit ``[perf] tool_resolve`` at DEBUG with input/output counts.
        Post-filter results of zero functions emit a second ``WARNING`` naming
        the structural cause (``module_exposes_no_triggers``,
        ``all_user_triggers_unknown``, or ``post_filter_empty``) and the
        selection is dropped — a module whose advertised triggers share nothing
        with the setup's enabled triggers must never reach the tool cache.

        Args:
            entry: Tool selection to resolve.
            registry: Registry service for module discovery.
            communication: Communication service for module schemas.

        Returns:
            ToolModuleInfo on success; ``None`` on registry miss or when no
            enabled trigger matches the module's advertised triggers.
        """
        setup = await registry.get_setup(entry.setup_id)
        if not setup or not setup.module_id:
            logger.warning(
                "Tool resolve failed: setup_id=%s reason=setup_not_found",
                entry.setup_id,
            )
            return None
        info = await registry.discover_by_id(setup.module_id)
        if not info:
            logger.warning(
                "Tool resolve failed: setup_id=%s tool_name=%s module_id=%s reason=module_not_discovered",
                entry.setup_id,
                setup.name,
                setup.module_id,
            )
            return None

        try:
            tool_info = await ToolModuleInfo.from_module_info(
                info,
                entry.setup_id,
                setup.name,
                communication,
            )
        except Exception:
            logger.exception(
                "Tool resolve failed: setup_id=%s tool_name=%s reason=schema_fetch_failed",
                entry.setup_id,
                setup.name,
            )
            return None

        available = {t.name for t in tool_info.tools}
        enabled_triggers = {name for name, enabled in entry.triggers.items() if enabled}

        if enabled_triggers and (unknown := enabled_triggers - available):
            logger.warning(
                "Tool '%s' enables triggers the module does not expose: %s (available: %s)",
                entry.setup_id,
                sorted(unknown),
                sorted(available),
            )

        post_count = len(available & enabled_triggers) if enabled_triggers else len(tool_info.tools)
        logger.debug(
            "[perf] tool_resolve: setup_id=%s slug=%s "
            "user_triggers_enabled=%d user_triggers_total=%d "
            "module_available=%d post_filter=%d",
            entry.setup_id,
            tool_info.slug,
            len(enabled_triggers),
            len(entry.triggers),
            len(available),
            post_count,
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "tool_resolve detail: setup_id=%s user_triggers=%s available=%s",
                entry.setup_id,
                dict(entry.triggers),
                sorted(available),
            )

        if post_count == 0:
            if not available:
                reason = "module_exposes_no_triggers"
            elif enabled_triggers and not (enabled_triggers & available):
                reason = "all_user_triggers_unknown"
            else:
                reason = "post_filter_empty"
            # TODO(validate): remove marker once drop-on-zero is validated in prod
            logger.warning(
                "[VALIDATE DROP0] Tool resolved with 0 functions, dropped: "
                "setup_id=%s slug=%s reason=%s user_enabled=%d module_available=%d",
                entry.setup_id,
                tool_info.slug,
                reason,
                len(enabled_triggers),
                len(available),
            )
            return None

        return tool_info


class _ToolReferenceInputSchema:
    """Custom JSON schema generator with configurable maxItems and ui:options."""

    def __init__(
        self,
        setup_ids: list[str] | None,
        module_ids: list[str] | None,
        tag_ids: list[str] | None,
        categories: list[str] | None,
        max_tools: int = 0,
        min_tools: int = 0,
    ) -> None:
        self.setup_ids = setup_ids or []
        self.module_ids = module_ids
        self.tag_ids = tag_ids or []
        self.max_tools = max_tools
        self.min_tools = min_tools
        self.categories = categories or []

    def __get_pydantic_json_schema__(
        self,
        _schema: CoreSchema,
        _handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        """Generate JSON schema as array for UI, hiding ToolReference complexity.

        Returns:
            JSON schema as array with ui:widget toolSelect.
        """
        json_schema: dict[str, object] = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "setupId": {"type": "string"},
                    "triggers": {
                        "type": "object",
                        "additionalProperties": {"type": "boolean"},
                        "minProperties": 1,
                        "maxProperties": 100,
                    },
                },
                "required": ["setupId", "triggers"],
            },
        }
        if self.max_tools > 0:
            json_schema["maxItems"] = self.max_tools
        if self.min_tools > 0:
            json_schema["minItems"] = self.min_tools
        json_schema["ui:widget"] = "toolSelect"
        json_schema["ui:options"] = {
            "setupIds": self.setup_ids or [],
            "tagIds": self.tag_ids or [],
            "categories": self.categories or [],
            "moduleIds": self.module_ids or [],
            "showModules": self.module_ids is not None,
        }
        return json_schema


def tool_reference_input(
    setup_ids: list[str] | None = None,
    module_ids: list[str] | None = None,
    tag_ids: list[str] | None = None,
    categories: list[str] | None = None,
    max_tools: int = 0,
    min_tools: int = 0,
) -> type[ToolReference]:
    """Create ToolReferenceInput type with schema options and validation.

    Args:
        setup_ids: Setup IDs for the user to choose from.
        module_ids: Module IDs for the user to choose from.
        tag_ids: Tag IDs for the user to choose from.
        categories: Categories for the user to choose from.
        max_tools: Maximum tools allowed. 0 for unlimited.
        min_tools: Minimum tools required. 0 for no minimum.

    Returns:
        Annotated type for use in Pydantic models.
    """

    def validate_tools_count(v: ToolReference) -> ToolReference:
        """Validate selected_tools count against min/max constraints.

        Returns:
            The validated ToolReference.

        Raises:
            ValueError: If count is below min_tools or above max_tools.
        """
        count = len(v.selected_tools)
        if min_tools > 0 and count < min_tools:
            msg = f"At least {min_tools} tools required, got {count}"
            raise ValueError(msg)
        if max_tools > 0 and count > max_tools:
            msg = f"At most {max_tools} tools allowed, got {count}"
            raise ValueError(msg)
        return v

    def serialize_to_list(v: ToolReference) -> list[dict[str, object]]:
        """Serialize ToolReference as list of dicts for frontend compatibility.

        Returns:
            List of tool selection dicts with id and subtools.
        """
        return [{"setupId": t.setup_id, "triggers": t.triggers} for t in v.selected_tools]

    schema = _ToolReferenceInputSchema(
        setup_ids=setup_ids or [],
        module_ids=module_ids,
        tag_ids=tag_ids or [],
        categories=categories or [],
        max_tools=max_tools,
        min_tools=min_tools,
    )

    # When min_tools > 0, omit the default so the field is required — an empty
    # ToolReference can never satisfy the constraint, and Pydantic v2 ignores
    # validate_default inside Annotated/Field, so the only reliable way to
    # prevent silent acceptance of the default is to not have one.
    field = Field() if min_tools > 0 else Field(default_factory=ToolReference)

    return Annotated[  # type: ignore[return-value]  # Returns Annotated type, not ToolReference directly
        ToolReference,
        AfterValidator(validate_tools_count),
        PlainSerializer(serialize_to_list, return_type=list[dict[str, object]]),
        schema,
        field,
    ]
