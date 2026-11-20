"""Tests for the Registry Toolkit managers — Tools / Services / Kins via ``manage_*`` dispatch."""

import asyncio
import datetime as dt
import json
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import AsyncMock, Mock

import pytest
from agno.exceptions import RunCancelledException
from agno.models.message import Message
from agno.models.response import ModelResponse
from pydantic import ValidationError

from digitalkin.community.agno.safe_litellm import SafeLiteLLM
from digitalkin.community.agno.toolkits import KinsManager, ServicesManager, ToolsManager
from digitalkin.community.agno.toolkits.registry.action import (
    ChangeVisibilityAction,
    DeleteAction,
    GetAction,
    ListVersionsAction,
    SearchAction,
    SetVersionAction,
    UpdateAction,
)
from digitalkin.community.agno.toolkits.registry.services.action import (
    AppendServiceAction,
    CreateServiceAction,
    GenerateServiceAction,
    GetServiceAction,
    LoadServiceAction,
    PatchServiceAction,
    RemoveServiceAction,
    SearchStructureServiceAction,
    ServiceSection,
    SetServiceVersionAction,
    StructureServiceAction,
    UpdateServiceAction,
    UpdateStructureServiceAction,
)
from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.models.services.registry import (
    ModuleInfo,
    RegistryModuleType,
    RegistrySetupStatus,
    RegistrySortBy,
    RegistryVisibility,
    SetupInfo,
    SetupSummary,
)
from digitalkin.models.services.storage import Visibility
from digitalkin.services.registry import DefaultRegistry
from digitalkin.services.registry.exceptions import RegistryServiceError
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.services.setup.setup_strategy import SetupData, SetupVersionData

# setup_id, module_id, module_name, module_type, status, version, content
_SEED = [
    ("setups:duda", "modules:duda", "tool-duda", RegistryModuleType.TOOL_MODULE, "1.0.0", {"secret": "MUST-NOT-LEAK"}),
    ("setups:nikita", "modules:nikita", "service-nikita", RegistryModuleType.SERVICE, "1.0.0", {"branding": True}),
    ("setups:isaac", "modules:isaac", "archetype-isaac", RegistryModuleType.ARCHETYPE, "2.0.0", {"agent": "x"}),
]
_NAMES = {"setups:duda": "Duda Builder", "setups:nikita": "Nikita", "setups:isaac": "Isaac"}
_TAGS = {"setups:duda": ["Web", "builder"], "setups:nikita": ["branding"], "setups:isaac": ["agent"]}
_DOCS = {
    # At the proto cap: SetupSummary.documentation is max_len 300.
    "setups:duda": "Builds websites. " + "x" * 283,
    "setups:nikita": "Branding service",
    "setups:isaac": "Multi-agent kin",
}


def _stores() -> tuple[DefaultSetup, DefaultRegistry]:
    """A setup + registry pair seeded with one resolvable setup of each object type.

    The two stores are kept consistent: every id readable via ``get_setup`` has its
    backing module registered, so a manager's type gate can resolve each id's kind.
    ``local`` (the id ``DefaultSetup`` mints on create) is registered as a SERVICE, since
    ``create`` only exists on ``services_manager``.
    """
    setup, registry = DefaultSetup(), DefaultRegistry("", "", "")
    now = dt.datetime.now(dt.timezone.utc)
    registry._modules["local"] = ModuleInfo(
        module_id="local", module_type=RegistryModuleType.SERVICE, module_name="local"
    )
    for setup_id, module_id, module_name, module_type, version, content in _SEED:
        setup.setups[setup_id] = SetupData(
            id=setup_id,
            name=_NAMES[setup_id],
            organisation_id="org",
            owner_id="owner",
            module_id=module_id,
            status=RegistrySetupStatus.READY,
            visibility=Visibility.PRIVATE,
            current_setup_version=SetupVersionData(
                id=f"{setup_id}:v", setup_id=setup_id, version=version, content=content, creation_date=now
            ),
        )
        registry._modules[module_id] = ModuleInfo(module_id=module_id, module_type=module_type, module_name=module_name)
        registry.add_setup(
            SetupInfo(
                setup_id=setup_id,
                name=_NAMES[setup_id],
                documentation=_DOCS[setup_id],
                status=RegistrySetupStatus.READY
                if module_type is not RegistryModuleType.ARCHETYPE
                else RegistrySetupStatus.CONFIGURATION_SUCCEEDED,
                module_id=module_id,
                module_name=module_name,
                module_type=module_type,
                setup_version=version,
                visibility=RegistryVisibility.PRIVATE,
                tags=_TAGS[setup_id],
                config=content,
            )
        )
    return setup, registry


def _env(raw: str) -> dict[str, Any]:
    return json.loads(raw)


class TestExposedSurface:
    def test_each_manager_exposes_exactly_one_tool(self) -> None:
        # Each manager registers exactly one agno Function (async entrypoint → async_functions).
        setup, reg = _stores()
        assert set(ToolsManager(setup, reg).async_functions) == {"tools_manager"}
        assert set(ServicesManager(setup, reg).async_functions) == {"services_manager"}
        assert set(KinsManager(setup, reg).async_functions) == {"kins_manager"}

    def test_services_schema_exposes_the_large_service_fields(self) -> None:
        """DEV-938: generate, append items, paged load; the model reads these from the schema."""
        schema = json.dumps(ServicesManager(*_stores()).async_functions["services_manager"].parameters)
        defs = json.loads(schema)["$defs"]

        assert "GenerateServiceAction" in defs
        assert defs["AppendServiceAction"]["properties"]["items"]["anyOf"][0]["type"] == "array"
        assert {"offset", "limit"} <= set(defs["LoadServiceAction"]["properties"])
        assert "generate" in ServicesManager(*_stores()).async_functions["services_manager"].description  # type: ignore[operator]

    def test_services_schema_exposes_the_size_aware_reads(self) -> None:
        """DEV-949: get's structure flag, structure paging and search_structure reach the model."""
        function = ServicesManager(*_stores()).async_functions["services_manager"]
        defs = json.loads(json.dumps(function.parameters))["$defs"]

        assert "SearchStructureServiceAction" in defs
        assert {"prefix", "offset", "limit"} <= set(defs["StructureServiceAction"]["properties"])
        assert "structure" in defs["GetServiceAction"]["properties"]
        assert "search_structure" in function.description

    def test_every_write_states_its_scope(self) -> None:
        """The model reads, for each write, which part of the service it changes and which it keeps."""
        defs = json.loads(json.dumps(ServicesManager(*_stores()).async_functions["services_manager"].parameters))[
            "$defs"
        ]
        writes = [
            "CreateServiceAction",
            "UpdateServiceAction",
            "PatchServiceAction",
            "AppendServiceAction",
            "RemoveServiceAction",
            "UpdateStructureServiceAction",
            "GenerateServiceAction",
            "DeleteAction",
            "ChangeVisibilityAction",
            "SetServiceVersionAction",
        ]

        assert [name for name in writes if "Scope:" not in defs[name]["description"]] == []
        assert "only the structure map" in defs["UpdateStructureServiceAction"]["description"]

    def test_tool_schema_exposes_the_action_union_without_validate_call(self) -> None:
        # The explicit schema keeps the discriminated union for the LLM; skip_entrypoint_processing
        # means Agno does NOT wrap validate_call — we validate in _run instead.
        fn = ServicesManager(*_stores()).async_functions["services_manager"]
        assert fn.skip_entrypoint_processing is True
        assert "action" in (fn.parameters or {}).get("properties", {})


class TestInvalidActionIsCleanEnvelope:
    """A bad LLM argument must be a clean fail envelope, not a raised ValidationError.

    Agno wraps normal tools in ``validate_call`` and logs any raise as an error traceback. These
    managers skip that and validate in ``_run``, so an out-of-range ``limit`` or a missing field —
    the model's mistake — comes back as an envelope the model reads and self-corrects from.
    """

    async def test_out_of_range_limit_is_a_fail_envelope(self) -> None:
        # A raw dict, exactly as Agno passes the model's arguments to the entrypoint.
        env = _env(await ServicesManager(*_stores()).services_manager({"action": "search", "query": "x", "limit": 101}))
        assert env["metadata"]["success"] is False
        assert env["metadata"]["tool"] == "services_manager"
        assert "limit" in env["error"]

    async def test_below_range_limit_is_a_fail_envelope(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager({"action": "search", "query": "rdf", "limit": 0}))
        assert env["metadata"]["success"] is False
        assert "limit" in env["error"]

    async def test_missing_required_field_is_a_fail_envelope(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager({"action": "get"}))  # setup_id missing
        assert env["metadata"]["success"] is False
        assert "setup_id" in env["error"]

    async def test_valid_raw_dict_payload_dispatches(self) -> None:
        # The happy path still works from a raw dict, proving validation runs and then dispatches.
        env = _env(await KinsManager(*_stores()).kins_manager({"action": "search", "query": "", "limit": 5}))
        assert env["metadata"]["success"] is True
        assert "setups" in env["output"]

    async def test_stringified_action_still_dispatches(self) -> None:
        # Regression: some models serialise the nested action as a JSON string (the discriminated
        # union schema triggers it). It must be parsed and dispatched, not rejected as "invalid".
        payload = json.dumps({"action": "search", "query": "rdf", "limit": 5})
        env = _env(await ToolsManager(*_stores()).tools_manager(payload))
        assert env["metadata"]["success"] is True
        assert "setups" in env["output"]


class TestSearchFiltersByType:
    """Each manager's ``search`` returns only setups of its own ``module_type``."""

    async def test_tools_search(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager(SearchAction(query="")))
        assert env["metadata"]["tool"] == "search"
        assert [s["setup_id"] for s in env["output"]["setups"]] == ["setups:duda"]

    async def test_services_search(self) -> None:
        env = _env(await ServicesManager(*_stores()).services_manager(SearchAction(query="")))
        assert [s["setup_id"] for s in env["output"]["setups"]] == ["setups:nikita"]

    async def test_kins_search(self) -> None:
        env = _env(await KinsManager(*_stores()).kins_manager(SearchAction(query="")))
        assert [s["setup_id"] for s in env["output"]["setups"]] == ["setups:isaac"]

    async def test_search_never_leaks_config(self) -> None:
        raw = await ToolsManager(*_stores()).tools_manager(SearchAction(query="duda"))
        assert "config" not in raw
        assert "MUST-NOT-LEAK" not in raw

    async def test_search_passes_documentation_through_whole(self) -> None:
        """The proto caps setup documentation at 300 characters, so the row no longer trims it."""
        env = _env(await ToolsManager(*_stores()).tools_manager(SearchAction(query="duda")))
        assert env["output"]["setups"][0]["documentation"] == _DOCS["setups:duda"]


class TestSearchLimit:
    """``limit`` spans the full service page size, and the probe row never exceeds it."""

    async def test_service_ceiling_is_accepted(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager(SearchAction(query="", limit=100)))
        assert env["metadata"]["success"] is True

    async def test_probe_row_is_clamped_at_the_ceiling(self) -> None:
        """At the ceiling there is no room for the +1 truncation probe, so the request is clamped."""
        seen: dict[str, int] = {}

        class _Recording(DefaultRegistry):
            async def search_setups(self, *_args: object, **kwargs: object) -> list:
                seen["limit"] = kwargs["limit"]  # type: ignore[assignment]
                return []

        reg = _Recording("", "", "")
        await ToolsManager(DefaultSetup(), reg).tools_manager(SearchAction(query="", limit=100))
        assert seen["limit"] == 100
        await ToolsManager(DefaultSetup(), reg).tools_manager(SearchAction(query="", limit=99))
        assert seen["limit"] == 100  # 99 + 1 probe row

    def test_above_the_ceiling_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SearchAction(query="", limit=101)


class TestSearchStatusFilter:
    """``unspecified`` has no proto name and the registry refuses ``validating``: both fail before the call."""

    @pytest.mark.parametrize("status", ["unspecified", "validating"])
    async def test_unfilterable_status_is_a_fail_envelope(self, status: str) -> None:
        registry = DefaultRegistry("", "", "")
        registry.search_setups = AsyncMock()  # type: ignore[method-assign]

        env = _env(
            await ToolsManager(DefaultSetup(), registry).tools_manager({
                "action": "search",
                "statuses": ["ready", status],
            })
        )

        assert env["metadata"]["success"] is False
        assert "cannot be used as a search status filter" in env["error"]
        registry.search_setups.assert_not_awaited()

    async def test_filterable_statuses_reach_the_registry(self) -> None:
        registry = DefaultRegistry("", "", "")
        registry.search_setups = AsyncMock(return_value=[])  # type: ignore[method-assign]

        env = _env(
            await ToolsManager(DefaultSetup(), registry).tools_manager({
                "action": "search",
                "statuses": ["failed", "configuration_failed"],
            })
        )

        assert env["metadata"]["success"] is True
        assert registry.search_setups.await_args.kwargs["statuses"] == [
            RegistrySetupStatus.FAILED,
            RegistrySetupStatus.CONFIGURATION_FAILED,
        ]


class TestGetAndLoad:
    async def test_get_returns_the_matching_object_type(self) -> None:
        env = _env(await ServicesManager(*_stores()).services_manager(GetServiceAction(setup_id="setups:nikita")))
        assert env["metadata"]["tool"] == "get"
        assert env["output"]["id"] == "setups:nikita"

    async def test_load_service_returns_json_content(self) -> None:
        env = _env(await ServicesManager(*_stores()).services_manager(LoadServiceAction(setup_id="setups:nikita")))
        assert env["metadata"]["tool"] == "load"
        assert env["output"] == {"branding": True}


class TestServiceCreateAndLoad:
    async def test_create_service(self) -> None:
        env = _env(
            await ServicesManager(*_stores()).services_manager(
                CreateServiceAction(
                    name="Nikita",
                    content={"branding": True},
                    structure={"branding": "whether the kin applies house branding"},
                )
            )
        )
        assert env["metadata"]["tool"] == "create"
        assert not env["output"]["path"]
        assert env["output"]["content_chars"] == len('{"branding": true}')

    async def test_create_then_load_round_trips(self) -> None:
        svc = ServicesManager(*_stores())
        created = _env(
            await svc.services_manager(
                CreateServiceAction(
                    name="Nikita", content={"branding": True}, structure={"branding": "house branding toggle"}
                )
            )
        )
        env = _env(await svc.services_manager(LoadServiceAction(setup_id=created["output"]["setup_id"])))
        assert env["output"] == {"branding": True}


class TestCrudRoundTrip:
    """update / change_visibility / delete route through the setup service on the same type."""

    async def test_update_visibility_delete(self) -> None:
        svc = ServicesManager(*_stores())
        created = _env(
            await svc.services_manager(CreateServiceAction(name="X", content={"a": 1}, structure={"a": "the a knob"}))
        )
        setup_id = created["output"]["setup_id"]

        updated = _env(
            await svc.services_manager(
                UpdateServiceAction(setup_id=setup_id, name="renamed", content={"a": 2}, structure={})
            )
        )
        assert updated["metadata"]["tool"] == "update"
        assert _env(await svc.services_manager(GetServiceAction(setup_id=setup_id)))["output"]["name"] == "renamed"

        shared = _env(await svc.services_manager(ChangeVisibilityAction(setup_id=setup_id, visibility="internal")))
        assert shared["output"]["visibility"] == "internal"

        deleted = _env(await svc.services_manager(DeleteAction(setup_id=setup_id)))
        assert deleted["output"] is True


class TestServiceContentIsFreeForm:
    """A service's backing module runs no server: update must not dial it for a config schema."""

    async def test_service_update_never_fetches_a_module_schema(self) -> None:
        ctx = SimpleNamespace(get_module_config_schema=AsyncMock(return_value={}), callbacks=SimpleNamespace())
        mgr = ServicesManager(*_stores(), context=ctx)  # type: ignore[arg-type]
        env = _env(
            await mgr.services_manager(
                UpdateServiceAction(setup_id="setups:nikita", name="x", content={"free": 1}, structure={})
            )
        )
        assert env["metadata"]["success"] is True
        ctx.get_module_config_schema.assert_not_awaited()


class TestContentValidation:
    """Update validates ``content`` against the module's config schema before writing (best-effort).

    With a context exposing the archetype's config schema, a missing/wrong field is refused with a
    correctable message; with no context wired, validation is skipped.
    """

    _SCHEMA: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"model": {"type": "string"}},
        "required": ["model"],
    }

    def _ctx(self) -> SimpleNamespace:
        return SimpleNamespace(
            get_module_config_schema=AsyncMock(return_value=self._SCHEMA), callbacks=SimpleNamespace()
        )

    async def test_update_missing_required_content_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx())  # type: ignore[arg-type]
        env = _env(await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"other": 1})))
        assert env["metadata"]["success"] is False
        assert "model" in env["error"]

    async def test_update_wrong_typed_content_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx())  # type: ignore[arg-type]
        env = _env(await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"model": 42})))
        assert env["metadata"]["success"] is False
        assert "model" in env["error"]

    async def test_update_valid_content_passes(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx())  # type: ignore[arg-type]
        env = _env(await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"model": "opus"})))
        assert env["metadata"]["success"] is True

    async def test_update_without_context_skips_validation(self) -> None:
        env = _env(
            await KinsManager(*_stores()).kins_manager(
                UpdateAction(setup_id="setups:isaac", name="x", content={"anything": 1})
            )
        )
        assert env["metadata"]["success"] is True

    # An object-typed root key must reject a non-object.
    _SCHEMA_OBJECT: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"knowledge": {"type": "object", "properties": {"docs": {"type": "string"}}}},
        "required": ["knowledge"],
    }
    _SCHEMA_REF: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"knowledge": {"$ref": "#/$defs/Knowledge"}},
        "required": ["knowledge"],
        "$defs": {"Knowledge": {"type": "object", "properties": {"docs": {"type": "string"}}}},
    }

    def _ctx_for(self, schema: dict[str, Any]) -> SimpleNamespace:
        return SimpleNamespace(get_module_config_schema=AsyncMock(return_value=schema), callbacks=SimpleNamespace())

    async def test_update_object_key_given_a_list_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_OBJECT))  # type: ignore[arg-type]
        env = _env(await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"knowledge": []})))
        assert env["metadata"]["success"] is False
        assert "knowledge" in env["error"]
        assert "dictionary" in env["error"]

    async def test_update_object_via_ref_given_a_list_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_REF))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(
                UpdateAction(setup_id="setups:isaac", name="x", content={"knowledge": ["not", "an", "object"]})
            )
        )
        assert env["metadata"]["success"] is False
        assert "knowledge" in env["error"]
        assert "dictionary" in env["error"]

    async def test_update_object_via_ref_given_an_object_passes(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_REF))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(
                UpdateAction(setup_id="setups:isaac", name="x", content={"knowledge": {"docs": "hello"}})
            )
        )
        assert env["metadata"]["success"] is True

    # An undeclared key must be refused, not persisted.
    async def test_update_undeclared_key_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx())  # type: ignore[arg-type]  # _SCHEMA: {model}
        env = _env(
            await mgr.kins_manager(
                UpdateAction(setup_id="setups:isaac", name="x", content={"model": "opus", "qa_test_injected": 1})
            )
        )
        assert env["metadata"]["success"] is False
        assert "qa_test_injected" in env["error"]

    # Array elements are typed, not just the container.
    _SCHEMA_ARRAY: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"rules": {"type": "array", "items": {"type": "string"}}},
        "required": ["rules"],
    }

    async def test_update_wrong_typed_array_element_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_ARRAY))  # type: ignore[arg-type]
        env = _env(await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"rules": [12345]})))
        assert env["metadata"]["success"] is False
        assert "rules.0" in env["error"]

    # A closed enum rejects an out-of-vocabulary value.
    _SCHEMA_ENUM: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"identity_type": {"enum": ["guided", "autonomous"]}},
        "required": ["identity_type"],
    }

    async def test_update_out_of_enum_value_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_ENUM))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(
                UpdateAction(setup_id="setups:isaac", name="x", content={"identity_type": "not_a_valid_enum_value"})
            )
        )
        assert env["metadata"]["success"] is False
        assert "identity_type" in env["error"]

    # Null is refused on a typed non-nullable field, but tolerated on a nullable one.
    _SCHEMA_NUMBER: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"price_multiplier": {"type": "number"}},
    }
    _SCHEMA_NULLABLE: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"note": {"anyOf": [{"type": "string"}, {"type": "null"}]}},
    }

    async def test_update_null_on_typed_field_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_NUMBER))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"price_multiplier": None}))
        )
        assert env["metadata"]["success"] is False
        assert "price_multiplier" in env["error"]

    async def test_update_null_on_declared_nullable_field_passes(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_NULLABLE))  # type: ignore[arg-type]
        env = _env(await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"note": None})))
        assert env["metadata"]["success"] is True

    # A number field rejects a coercible string/bool, keeps ints.
    async def test_update_string_coerced_to_number_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_NUMBER))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"price_multiplier": "2.5"}))
        )
        assert env["metadata"]["success"] is False
        assert "price_multiplier" in env["error"]

    async def test_update_bool_coerced_to_number_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_NUMBER))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"price_multiplier": True}))
        )
        assert env["metadata"]["success"] is False
        assert "price_multiplier" in env["error"]

    async def test_update_integer_for_number_field_passes(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_NUMBER))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"price_multiplier": 2}))
        )
        assert env["metadata"]["success"] is True

    # Control characters (NUL, ANSI escape) in a string field are refused.
    _SCHEMA_STRING: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
    }

    async def test_update_control_char_in_string_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_STRING))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"name": "Litmus\x00evil"}))
        )
        assert env["metadata"]["success"] is False
        assert "name" in env["error"]

    async def test_update_newline_in_string_passes(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_STRING))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"name": "line1\nline2"}))
        )
        assert env["metadata"]["success"] is True

    # A declared minItems refuses an empty structural array.
    _SCHEMA_MINITEMS: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"rules": {"type": "array", "items": {"type": "string"}, "minItems": 1}},
        "required": ["rules"],
    }

    async def test_update_empty_array_below_minitems_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_MINITEMS))  # type: ignore[arg-type]
        env = _env(await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"rules": []})))
        assert env["metadata"]["success"] is False
        assert "rules" in env["error"]

    # additionalProperties types a mapping's values (triggers).
    _SCHEMA_MAPPING: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {"triggers": {"type": "object", "additionalProperties": {"type": "boolean"}}},
        "required": ["triggers"],
    }

    async def test_update_wrong_typed_mapping_value_is_refused(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_MAPPING))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(
                UpdateAction(setup_id="setups:isaac", name="x", content={"triggers": {"read_json": "yes_please"}})
            )
        )
        assert env["metadata"]["success"] is False
        assert "triggers.read_json" in env["error"]

    async def test_update_correctly_typed_mapping_value_passes(self) -> None:
        mgr = KinsManager(*_stores(), context=self._ctx_for(self._SCHEMA_MAPPING))  # type: ignore[arg-type]
        env = _env(
            await mgr.kins_manager(
                UpdateAction(setup_id="setups:isaac", name="x", content={"triggers": {"read_json": True}})
            )
        )
        assert env["metadata"]["success"] is True


class TestActionNameHardening:
    """The action's own ``name`` (outside ``content``) also rejects control characters.

    Without this the name bypasses the content validator, reaching persistence where a NUL byte or
    ANSI escape is silently stripped — altering the value with no error to the caller.
    """

    async def test_update_name_with_control_char_is_refused(self) -> None:
        env = _env(
            await KinsManager(*_stores()).kins_manager({
                "action": "update",
                "setup_id": "setups:isaac",
                "name": "Bad\x00name",
                "content": {"x": 1},
            })
        )
        assert env["metadata"]["success"] is False
        assert "name" in env["error"]

    async def test_service_create_name_with_ansi_escape_is_refused(self) -> None:
        env = _env(
            await ServicesManager(*_stores()).services_manager({
                "action": "create",
                "name": "svc\x1b[31m",
                "content": {"a": 1},
            })
        )
        assert env["metadata"]["success"] is False
        assert "name" in env["error"]

    async def test_update_clean_name_still_passes(self) -> None:
        env = _env(
            await KinsManager(*_stores()).kins_manager({
                "action": "update",
                "setup_id": "setups:isaac",
                "name": "Clean Name",
                "content": {"x": 1},
            })
        )
        assert env["metadata"]["success"] is True


class TestTypeIsolation:
    """An id resolves regardless of kind, so every id-targeting action gates the type."""

    async def test_kins_manager_refuses_to_get_a_tool(self) -> None:
        """A Kin manager handed a tool setup id returns a fail, not the tool."""
        env = _env(await KinsManager(*_stores()).kins_manager(GetAction(setup_id="setups:duda")))
        assert env["metadata"]["success"] is False
        assert "kind mismatch" in env["error"]

    async def test_tools_manager_refuses_to_get_a_kin(self) -> None:
        """A Tool manager handed a Kin setup id does not return the full agent."""
        env = _env(await ToolsManager(*_stores()).tools_manager(GetAction(setup_id="setups:isaac")))
        assert env["metadata"]["success"] is False
        assert "kind mismatch" in env["error"]
        assert "agent" not in env["error"]

    async def test_services_manager_refuses_to_load_a_tool(self) -> None:
        """``load`` on a tool id is refused and the tool config never leaks."""
        raw = await ServicesManager(*_stores()).services_manager(LoadServiceAction(setup_id="setups:duda"))
        env = _env(raw)
        assert env["metadata"]["success"] is False
        assert "MUST-NOT-LEAK" not in raw

    async def test_tools_manager_cannot_delete_a_service(self) -> None:
        """Cross-type delete is refused before the destructive call, leaving the id intact."""
        setup, reg = _stores()
        env = _env(await ToolsManager(setup, reg).tools_manager(DeleteAction(setup_id="setups:nikita")))
        assert env["metadata"]["success"] is False
        assert "setups:nikita" in setup.setups


class TestDeletedResourceIsFrozen:
    """A deleted id is no longer resolvable, so writes on it are refused."""

    async def test_update_after_delete_is_refused(self) -> None:
        svc = ServicesManager(*_stores())
        assert _env(await svc.services_manager(DeleteAction(setup_id="setups:nikita")))["output"] is True
        env = _env(
            await svc.services_manager(
                UpdateServiceAction(setup_id="setups:nikita", name="zombie", content={"k": "v"}, structure={})
            )
        )
        assert env["metadata"]["success"] is False
        assert env["metadata"]["tool"] == "update"


class TestVisibilityVocabulary:
    """Visibility reads back in the vocabulary the caller writes."""

    async def test_change_visibility_echoes_input_form(self) -> None:
        env = _env(
            await ServicesManager(*_stores()).services_manager(
                ChangeVisibilityAction(setup_id="setups:nikita", visibility="internal")
            )
        )
        assert env["output"]["visibility"] == "internal"

    async def test_change_visibility_returns_reread_state_not_write_snapshot(self) -> None:
        """The response reflects the committed re-read, not change_visibility's pre-write snapshot."""
        setup, reg = _stores()
        base = setup.setups["setups:nikita"]
        stale = base.model_copy(deep=True)  # what change_visibility echoes (pre-write snapshot)
        stale.current_setup_version.version = "1.0.0"
        fresh = base.model_copy(deep=True, update={"visibility": Visibility.INTERNAL})  # committed state
        fresh.current_setup_version.version = "1.0.1"  # a concurrent update bumped the version

        setup.change_visibility = AsyncMock(return_value=stale)  # type: ignore[method-assign]
        setup.get_setup = AsyncMock(return_value=fresh)  # type: ignore[method-assign]

        env = _env(
            await ServicesManager(setup, reg).services_manager(
                ChangeVisibilityAction(setup_id="setups:nikita", visibility="internal")
            )
        )
        assert env["output"]["current_setup_version"]["version"] == "1.0.1"  # re-read, not the 1.0.0 snapshot
        assert env["output"]["visibility"] == "internal"


class TestInvalidation:
    async def test_write_invalidates_but_read_does_not(self) -> None:
        invalidate = Mock()
        context = SimpleNamespace(callbacks=SimpleNamespace(invalidate_setup=invalidate))
        svc = ServicesManager(*_stores(), context=context)  # type: ignore[arg-type]
        await svc.services_manager(CreateServiceAction(name="X", content={"a": 1}, structure={"a": "the a knob"}))
        await svc.services_manager(SearchAction(query=""))
        invalidate.assert_called_once()


class TestDegradation:
    async def test_search_permission_denied_is_distinct(self) -> None:
        class _Denied(DefaultRegistry):
            async def search_setups(self, *_args: object, **_kwargs: object) -> list:
                msg = "denied"
                raise PermissionDeniedError(msg)

        env = _env(await ToolsManager(DefaultSetup(), _Denied("", "", "")).tools_manager(SearchAction(query="")))
        assert env["error"] == "permission denied: search"

    async def test_setup_error_lands_in_fail_envelope(self) -> None:
        setup, reg = _stores()

        def _boom(_: dict[str, Any]) -> bool:  # raises on call, before the dispatcher's await
            msg = "boom"
            raise SetupServiceError(msg)

        setup.delete_setup = _boom  # type: ignore[method-assign]
        env = _env(await ServicesManager(setup, reg).services_manager(DeleteAction(setup_id="setups:nikita")))
        assert env["metadata"]["success"] is False
        assert env["metadata"]["tool"] == "delete"


class TestQaContractFixes:
    """Client-side contract fixes from the services_manager campaign."""

    def test_visibility_rejects_unspecified(self) -> None:
        """The exposed visibility no longer offers 'unspecified' (rejected by the backend)."""
        with pytest.raises(ValidationError):
            ChangeVisibilityAction(setup_id="s", visibility="unspecified")  # type: ignore[arg-type]

    def test_create_rejects_empty_content(self) -> None:
        """An empty content is rejected client-side (schema declares minProperties: 1)."""
        with pytest.raises(ValidationError):
            CreateServiceAction(name="x", content={}, structure={})

    def test_update_rejects_empty_content(self) -> None:
        """Same non-empty content contract on update."""
        with pytest.raises(ValidationError):
            UpdateAction(setup_id="s", name="n", content={})

    def test_get_has_no_version_field(self) -> None:
        """The ignored/deprecated 'version' field is gone from get."""
        assert "version" not in GetAction.model_fields


class TestSearchFilterSurface:
    """``search`` exposes the registry's whole filter surface, minus the type boundary."""

    @staticmethod
    def _recording() -> tuple[DefaultRegistry, dict[str, Any]]:
        """A registry that captures the kwargs the toolkit forwards."""
        seen: dict[str, Any] = {}

        class _Recording(DefaultRegistry):
            async def search_setups(self, **kwargs: Any) -> list:
                seen.update(kwargs)
                return []

        return _Recording("", "", ""), seen

    def test_every_registry_filter_is_llm_visible(self) -> None:
        exposed = set(SearchAction.model_json_schema()["properties"])
        assert exposed == {
            "action",
            "query",
            "setup_ids",
            "module_ids",
            "statuses",
            "visibilities",
            "tags",
            "sort_by",
            "descending",
            "limit",
            "offset",
        }

    async def test_filters_are_forwarded_verbatim(self) -> None:
        registry, seen = self._recording()
        await ToolsManager(DefaultSetup(), registry).tools_manager(
            SearchAction(
                query="q",
                setup_ids=["setups:a"],
                module_ids=["modules:b"],
                statuses=[RegistrySetupStatus.FAILED],
                visibilities=[RegistryVisibility.PUBLIC],
                tags=["rag"],
                sort_by=RegistrySortBy.NAME,
                descending=True,
                offset=20,
            )
        )
        assert seen["query"] == "q"
        assert seen["setup_ids"] == ["setups:a"]
        assert seen["module_ids"] == ["modules:b"]
        assert seen["statuses"] == [RegistrySetupStatus.FAILED]
        assert seen["visibilities"] == [RegistryVisibility.PUBLIC]
        assert seen["tags"] == ["rag"]
        assert seen["sort_by"] is RegistrySortBy.NAME
        assert seen["descending"] is True
        assert seen["offset"] == 20

    async def test_module_type_stays_pinned_to_the_manager(self) -> None:
        """The type boundary is not a caller-settable filter — it is the manager's identity."""
        registry, seen = self._recording()
        await KinsManager(DefaultSetup(), registry).kins_manager(SearchAction(query=""))
        assert seen["module_types"] == [RegistryModuleType.ARCHETYPE]

    async def test_status_defaults_to_the_invocable_pair(self) -> None:
        registry, seen = self._recording()
        await ToolsManager(DefaultSetup(), registry).tools_manager(SearchAction(query=""))
        assert seen["statuses"] == [RegistrySetupStatus.READY, RegistrySetupStatus.CONFIGURATION_SUCCEEDED]

    async def test_an_explicit_status_overrides_the_default(self) -> None:
        """Otherwise a broken setup would be unreachable through this surface."""
        registry, seen = self._recording()
        await ToolsManager(DefaultSetup(), registry).tools_manager(
            SearchAction(query="", statuses=[RegistrySetupStatus.FAILED])
        )
        assert seen["statuses"] == [RegistrySetupStatus.FAILED]

    async def test_tag_filter_narrows_the_result(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager(SearchAction(query="", tags=["web"])))
        assert [s["setup_id"] for s in env["output"]["setups"]] == ["setups:duda"]

    async def test_a_non_matching_tag_returns_nothing(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager(SearchAction(query="", tags=["nope"])))
        assert env["output"]["setups"] == []

    async def test_offset_pages_past_the_only_match(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager(SearchAction(query="", offset=1)))
        assert env["output"]["setups"] == []
        assert env["output"]["offset"] == 1

    def test_a_negative_offset_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SearchAction(query="", offset=-1)

    def test_an_unknown_enum_value_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SearchAction(query="", sort_by="sideways")


class TestSearchRowsCarryFilterableFields:
    """A filter is unusable if its values never appear in a result row."""

    async def test_rows_expose_tags_visibility_and_status(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager(SearchAction(query="")))
        row = env["output"]["setups"][0]
        assert row["tags"] == ["Web", "builder"]
        assert row["visibility"] == "private"
        assert row["status"] == "ready"

    async def test_rows_still_never_leak_config(self) -> None:
        raw = await ToolsManager(*_stores()).tools_manager(SearchAction(query=""))
        assert "MUST-NOT-LEAK" not in raw


class TestVersionHistory:
    """``update`` cuts versions; ``list_versions`` + ``set_version`` make them reachable."""

    @staticmethod
    async def _kin() -> tuple[KinsManager, str]:
        """A kins manager over one archetype setup whose content is ``{"tone": "good"}``."""
        setup, registry = DefaultSetup(), DefaultRegistry("", "", "")
        created = await setup.create_setup({"name": "isaac", "content": {"tone": "good"}})
        registry._modules["local"] = ModuleInfo(module_id="local", module_type=RegistryModuleType.ARCHETYPE)
        return KinsManager(setup, registry), created.id

    @staticmethod
    async def _content(manager: KinsManager, setup_id: str) -> Any:
        env = _env(await manager.kins_manager(GetAction(setup_id=setup_id)))
        return env["output"]["current_setup_version"]["content"]

    @staticmethod
    async def _documentation(manager: KinsManager, setup_id: str) -> Any:
        env = _env(await manager.kins_manager(GetAction(setup_id=setup_id)))
        return env["output"]["current_setup_version"]["documentation"]

    async def test_update_carries_documentation_over_when_omitted(self) -> None:
        """A content-only update must not blank the text the instance is searchable by."""
        setup, registry = DefaultSetup(), DefaultRegistry("", "", "")
        created = await setup.create_setup({
            "name": "isaac",
            "content": {"tone": "good"},
            "documentation": "the house voice",
        })
        registry._modules["local"] = ModuleInfo(module_id="local", module_type=RegistryModuleType.ARCHETYPE)
        manager = KinsManager(setup, registry)

        await manager.kins_manager(UpdateAction(setup_id=created.id, name="isaac", content={"tone": "loud"}))
        assert await self._documentation(manager, created.id) == "the house voice"

    async def test_update_replaces_documentation_when_given(self) -> None:
        manager, setup_id = await self._kin()
        await manager.kins_manager(
            UpdateAction(setup_id=setup_id, name="isaac", content={"tone": "loud"}, documentation="new text")
        )
        assert await self._documentation(manager, setup_id) == "new text"

    async def test_update_clears_documentation_on_an_explicit_empty_string(self) -> None:
        """Empty string is a deliberate clear, distinct from omitting the field."""
        setup, registry = DefaultSetup(), DefaultRegistry("", "", "")
        created = await setup.create_setup({
            "name": "isaac",
            "content": {"tone": "good"},
            "documentation": "the house voice",
        })
        registry._modules["local"] = ModuleInfo(module_id="local", module_type=RegistryModuleType.ARCHETYPE)
        manager = KinsManager(setup, registry)

        await manager.kins_manager(
            UpdateAction(setup_id=created.id, name="isaac", content={"tone": "loud"}, documentation="")
        )
        assert not await self._documentation(manager, created.id)

    async def test_update_activates_the_new_version_by_default(self) -> None:
        manager, setup_id = await self._kin()
        await manager.kins_manager(UpdateAction(setup_id=setup_id, name="isaac", content={"tone": "loud"}))
        assert await self._content(manager, setup_id) == {"tone": "loud"}

    async def test_update_can_stage_without_activating(self) -> None:
        manager, setup_id = await self._kin()
        await manager.kins_manager(
            UpdateAction(setup_id=setup_id, name="isaac", content={"tone": "loud"}, set_as_current=False)
        )
        assert await self._content(manager, setup_id) == {"tone": "good"}
        env = _env(await manager.kins_manager(ListVersionsAction(setup_id=setup_id)))
        assert env["output"]["total_count"] == 2

    async def test_list_versions_is_most_recent_first_and_flags_the_live_one(self) -> None:
        manager, setup_id = await self._kin()
        await manager.kins_manager(UpdateAction(setup_id=setup_id, name="isaac", content={"tone": "loud"}))
        env = _env(await manager.kins_manager(ListVersionsAction(setup_id=setup_id)))

        versions = env["output"]["versions"]
        assert [v["is_current"] for v in versions] == [True, False]
        assert env["output"]["current_setup_version_id"] == versions[0]["setup_version_id"]
        assert env["output"]["total_count"] == 2

    async def test_list_versions_never_returns_configuration_payloads(self) -> None:
        """History rows are metadata: dumping every past config would blow up the context window."""
        manager, setup_id = await self._kin()
        raw = await manager.kins_manager(ListVersionsAction(setup_id=setup_id))
        assert "content" not in _env(raw)["output"]["versions"][0]
        assert "tone" not in raw

    async def test_list_versions_paginates(self) -> None:
        manager, setup_id = await self._kin()
        for tone in ("a", "b"):
            await manager.kins_manager(UpdateAction(setup_id=setup_id, name="isaac", content={"tone": tone}))
        env = _env(await manager.kins_manager(ListVersionsAction(setup_id=setup_id, limit=1, offset=2)))
        assert env["output"]["returned"] == 1
        assert env["output"]["total_count"] == 3
        assert env["output"]["offset"] == 2

    async def test_set_version_undoes_a_bad_update(self) -> None:
        manager, setup_id = await self._kin()
        await manager.kins_manager(UpdateAction(setup_id=setup_id, name="isaac", content={"tone": "BROKEN"}))
        env = _env(await manager.kins_manager(ListVersionsAction(setup_id=setup_id)))
        previous = next(v for v in env["output"]["versions"] if not v["is_current"])

        await manager.kins_manager(SetVersionAction(setup_id=setup_id, setup_version_id=previous["setup_version_id"]))
        assert await self._content(manager, setup_id) == {"tone": "good"}

    async def test_a_rollback_can_itself_be_rolled_forward(self) -> None:
        """set_version creates nothing, so returning to the newer version is another set_version."""
        manager, setup_id = await self._kin()
        await manager.kins_manager(UpdateAction(setup_id=setup_id, name="isaac", content={"tone": "new"}))
        env = _env(await manager.kins_manager(ListVersionsAction(setup_id=setup_id)))
        newer, older = env["output"]["versions"]

        await manager.kins_manager(SetVersionAction(setup_id=setup_id, setup_version_id=older["setup_version_id"]))
        await manager.kins_manager(SetVersionAction(setup_id=setup_id, setup_version_id=newer["setup_version_id"]))

        assert await self._content(manager, setup_id) == {"tone": "new"}
        assert _env(await manager.kins_manager(ListVersionsAction(setup_id=setup_id)))["output"]["total_count"] == 2

    async def test_set_version_refuses_a_version_from_another_setup(self) -> None:
        manager, mine = await self._kin()
        env = _env(await manager.kins_manager(SetVersionAction(setup_id=mine, setup_version_id="setups:other:v")))
        assert env["metadata"]["success"] is False

    async def test_set_version_marks_itself_as_a_write(self) -> None:
        """``writes`` drives the servicer setup-cache invalidation; without it a rollback is invisible."""
        assert SetVersionAction.writes is True
        assert ListVersionsAction.writes is False


class TestVersionActionsRespectTheTypeBoundary:
    """Version history is as type-scoped as the setup it belongs to."""

    async def test_list_versions_refuses_a_foreign_kind(self) -> None:
        env = _env(await KinsManager(*_stores()).kins_manager(ListVersionsAction(setup_id="setups:duda")))
        assert env["metadata"]["success"] is False
        assert "kind mismatch" in json.dumps(env)

    async def test_set_version_refuses_a_foreign_kind(self) -> None:
        env = _env(
            await ToolsManager(*_stores()).tools_manager(
                SetVersionAction(setup_id="setups:isaac", setup_version_id="setups:isaac:v")
            )
        )
        assert env["metadata"]["success"] is False
        assert "kind mismatch" in json.dumps(env)

    def test_both_actions_are_on_every_manager(self) -> None:
        setup, registry = _stores()
        for manager, tool in (
            (ToolsManager(setup, registry), "tools_manager"),
            (ServicesManager(setup, registry), "services_manager"),
            (KinsManager(setup, registry), "kins_manager"),
        ):
            schema = json.dumps(manager.async_functions[tool].parameters)
            assert "list_versions" in schema
            assert "set_version" in schema


class TestFlattenedCalls:
    """Models routinely flatten a nested union instead of nesting it; that must still dispatch.

    The tool schema declares one ``action`` property holding the union, but models send
    ``{"action": "search", "query": ""}`` at least as often as the nested form. Because the
    ``Function`` is registered with ``skip_entrypoint_processing``, agno splats those arguments
    straight onto the entrypoint — so anything it does not absorb dies as a ``TypeError`` inside
    agno, before validation can turn it into a correctable message.
    """

    async def test_the_flattened_form_dispatches(self) -> None:
        env = _env(await KinsManager(*_stores()).kins_manager(action="search", query="", limit=5))
        assert env["metadata"]["success"] is True
        assert env["metadata"]["tool"] == "search"

    async def test_the_flattened_form_matches_the_nested_one(self) -> None:
        flat = _env(await ToolsManager(*_stores()).tools_manager(action="search", query="duda"))
        nested = _env(await ToolsManager(*_stores()).tools_manager({"action": "search", "query": "duda"}))
        assert flat["output"] == nested["output"]

    async def test_a_bare_discriminator_needs_no_fields(self) -> None:
        env = _env(await KinsManager(*_stores()).kins_manager(action="search"))
        assert env["metadata"]["success"] is True

    async def test_the_inner_object_may_arrive_as_a_json_string(self) -> None:
        env = _env(await KinsManager(*_stores()).kins_manager('{"action": "search", "query": ""}'))
        assert env["metadata"]["success"] is True

    async def test_flattened_fields_reach_the_action(self) -> None:
        """Re-nesting must carry the values, not merely stop the TypeError."""
        seen: dict[str, Any] = {}

        class _Recording(DefaultRegistry):
            async def search_setups(self, **kwargs: Any) -> list:
                seen.update(kwargs)
                return []

        await ToolsManager(DefaultSetup(), _Recording("", "", "")).tools_manager(
            action="search", query="hello", limit=7, offset=3
        )
        assert seen["query"] == "hello"
        assert seen["offset"] == 3

    async def test_flattening_works_for_every_manager(self) -> None:
        setup, registry = _stores()
        for manager, call in (
            (ToolsManager(setup, registry), "tools_manager"),
            (ServicesManager(setup, registry), "services_manager"),
            (KinsManager(setup, registry), "kins_manager"),
        ):
            env = _env(await getattr(manager, call)(action="search", query=""))
            assert env["metadata"]["success"] is True, call

    async def test_flattening_works_for_the_new_version_actions(self) -> None:
        env = _env(await KinsManager(*_stores()).kins_manager(action="list_versions", setup_id="setups:isaac"))
        assert env["metadata"]["success"] is True
        assert env["output"]["total_count"] == 1

    async def test_an_invalid_flattened_field_still_fails_cleanly(self) -> None:
        """Absorbing the arguments must not turn a bad value into a silent success."""
        env = _env(await KinsManager(*_stores()).kins_manager(action="search", limit=999))
        assert env["metadata"]["success"] is False
        assert "limit" in env["error"]

    async def test_an_unknown_flattened_field_is_ignored_like_a_nested_one(self) -> None:
        """Extras are dropped, not refused — the action models keep pydantic's default policy.

        Worth pinning: re-nesting must not make the flattened path stricter than the nested one,
        or a call the model can already make today would start failing depending on how it framed
        the arguments.
        """
        setup, registry = _stores()
        flat = _env(await KinsManager(setup, registry).kins_manager(action="search", nonsense=1))
        nested = _env(await KinsManager(setup, registry).kins_manager({"action": "search", "nonsense": 1}))
        assert flat["metadata"]["success"] is True
        assert nested["metadata"]["success"] is True


class TestOrphanedSetups:
    """A setup whose backing module cannot be resolved must still be removable."""

    @staticmethod
    async def _orphan() -> tuple[DefaultSetup, DefaultRegistry, str]:
        """A setup whose ``module_id`` the registry has never heard of."""
        setup, registry = DefaultSetup(), DefaultRegistry("", "", "")
        created = await setup.create_setup({"name": "orphan", "content": {"a": 1}})
        return setup, registry, created.id

    async def test_delete_removes_an_orphan(self) -> None:
        """Otherwise the record is unremovable by anyone, forever."""
        setup, registry, setup_id = await self._orphan()
        env = _env(await ServicesManager(setup, registry).services_manager(DeleteAction(setup_id=setup_id)))
        assert env["metadata"]["success"] is True
        assert setup_id not in setup.setups

    async def test_reads_still_refuse_an_orphan(self) -> None:
        """Only delete is widened: an unknowable kind must not become a cross-type read."""
        setup, registry, setup_id = await self._orphan()
        manager = ServicesManager(setup, registry)
        for action in (GetAction(setup_id=setup_id), ListVersionsAction(setup_id=setup_id)):
            env = _env(await manager.services_manager(action))
            assert env["metadata"]["success"] is False

    async def test_a_registry_outage_does_not_authorise_a_delete(self) -> None:
        """The widening keys on NOT-FOUND, not on 'the registry call failed'.

        A transient outage must not destroy a healthy setup whose type simply could not be
        read at that moment — including one belonging to another manager.
        """
        setup, _registry, setup_id = await self._orphan()

        class _Unreachable(DefaultRegistry):
            async def discover_by_id(self, module_id: str) -> ModuleInfo:
                msg = "registry unreachable"
                raise RegistryServiceError(msg)

        env = _env(
            await ServicesManager(setup, _Unreachable("", "", "")).services_manager(DeleteAction(setup_id=setup_id))
        )
        assert env["metadata"]["success"] is False
        assert setup_id in setup.setups

    async def test_a_resolvable_setup_of_another_kind_is_still_refused(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager(DeleteAction(setup_id="setups:isaac")))
        assert env["metadata"]["success"] is False
        assert "kind mismatch" in json.dumps(env)


class TestAuthoredStructure:
    """The agent writes the key map; the SDK stores it verbatim and judges nothing."""

    async def test_create_stores_the_authored_summaries(self) -> None:
        setup, registry = _stores()
        svc = ServicesManager(setup, registry)
        authored = {"llm.provider": "which backend routes the call", "region": "where it runs"}

        env = _env(
            await svc.services_manager(
                CreateServiceAction(
                    name="N",
                    content={"llm": {"provider": "litellm"}, "region": "eu-west"},
                    structure=authored,
                )
            )
        )

        assert setup.setups[env["output"]["setup_id"]].current_setup_version.structure == authored

    async def test_a_map_is_stored_unjudged(self) -> None:
        """An entry naming a key the content lacks is still stored — we do not vet the map."""
        setup, registry = _stores()
        svc = ServicesManager(setup, registry)
        authored = {"region": "where it runs", "llm.provider": "not in this document"}

        env = _env(
            await svc.services_manager(CreateServiceAction(name="N", content={"region": "eu-west"}, structure=authored))
        )

        assert setup.setups[env["output"]["setup_id"]].current_setup_version.structure == authored

    async def test_update_replaces_the_map_with_the_one_supplied(self) -> None:
        setup, registry = _stores()
        svc = ServicesManager(setup, registry)
        created = _env(
            await svc.services_manager(
                CreateServiceAction(name="N", content={"a": "one"}, structure={"a": "the a knob"})
            )
        )

        setup_id = created["output"]["setup_id"]
        await svc.services_manager(
            UpdateServiceAction(
                setup_id=setup_id,
                name="N",
                content={"a": "two", "b": "new"},
                structure={"a": "the a knob", "b": "the b knob"},
            )
        )

        assert setup.setups[setup_id].current_setup_version.structure == {
            "a": "the a knob",
            "b": "the b knob",
        }

    def test_update_requires_a_structure(self) -> None:
        """Like create: an update rewrites the whole content, so it must describe it."""
        with pytest.raises(ValidationError, match="structure"):
            UpdateServiceAction(setup_id="setups:x", name="N", content={"a": "two"})  # type: ignore[call-arg]

    async def test_a_raw_update_without_a_structure_fails_cleanly(self) -> None:
        env = _env(
            await ServicesManager(*_stores()).services_manager({
                "action": "update",
                "setup_id": "setups:nikita",
                "name": "N",
                "content": {"a": "two"},
            })
        )

        assert env["metadata"]["success"] is False
        assert "structure" in env["error"]

    def test_create_requires_a_structure(self) -> None:
        with pytest.raises(ValidationError):
            CreateServiceAction(name="x", content={"a": 1})  # type: ignore[call-arg]

    def test_structure_is_a_services_only_field(self) -> None:
        """It describes a service's configuration; kins and tools never see it."""
        assert "structure" in UpdateServiceAction.model_fields
        assert "structure" not in UpdateAction.model_fields


class TestScopedRead:
    """search shows the shape, load reads one key of it — the two-step read."""

    @staticmethod
    async def _service() -> tuple[ServicesManager, str]:
        """A services manager over one setup with a nested configuration and an authored map."""
        svc = ServicesManager(*_stores())
        created = _env(
            await svc.services_manager(
                CreateServiceAction(
                    name="N",
                    content={"llm": {"provider": "litellm", "model": "gpt-4o"}, "region": "eu-west"},
                    structure={"llm.model": "which model answers", "region": "where it runs"},
                )
            )
        )
        return svc, created["output"]["setup_id"]

    async def test_load_with_a_key_returns_only_that_key(self) -> None:
        """The read is narrowed at the setup service, so the response carries just the key."""
        svc, setup_id = await self._service()

        env = _env(await svc.services_manager(LoadServiceAction(setup_id=setup_id, key="llm.model")))

        assert env["output"] == {"llm.model": "gpt-4o"}
        assert "litellm" not in json.dumps(env["output"])

    async def test_load_without_a_key_returns_the_whole_document(self) -> None:
        svc, setup_id = await self._service()

        env = _env(await svc.services_manager(LoadServiceAction(setup_id=setup_id)))

        assert env["output"] == {"llm": {"provider": "litellm", "model": "gpt-4o"}, "region": "eu-west"}

    async def test_load_with_an_unknown_key_fails_with_not_found(self) -> None:
        """The backend answers NOT_FOUND for a key the configuration does not have.

        The declaration says so too: an agent told a bad key returns everything would guard
        against an over-read and never handle the failure it actually receives.
        """
        svc, setup_id = await self._service()

        envelope = json.loads(await svc.services_manager(LoadServiceAction(setup_id=setup_id, key="llm.nope")))

        assert envelope["metadata"]["success"] is False
        assert "no path llm.nope" in envelope["error"]
        assert "output" not in envelope

    @pytest.mark.regression
    def test_the_declaration_matches_the_not_found(self) -> None:
        """Regression (QA D6): the load contract promised the whole document on a bad key."""
        schema = json.dumps(LoadServiceAction.model_json_schema())

        assert "not found" in schema
        assert "returns the whole document" not in schema

    @staticmethod
    async def _service_indexed(structure: dict[str, str]) -> tuple[ServicesManager, str]:
        """A services manager whose one setup carries ``structure``, indexed by the registry search too."""
        setup, registry = _stores()
        created = await setup.create_setup({
            "name": "N",
            "content": {"llm": {"model": "gpt-4o"}},
            "structure": structure,
        })

        class _WithMap(DefaultRegistry):
            async def search_setups(self, *args: Any, **kwargs: Any) -> list[SetupSummary]:
                return [
                    SetupSummary(
                        setup_id=created.id, name="N", module_type=RegistryModuleType.SERVICE, structure=structure
                    )
                ]

        indexed = _WithMap("", "", "")
        indexed._modules = registry._modules
        return ServicesManager(setup, indexed), created.id

    @pytest.mark.regression
    async def test_get_serves_the_structure_the_structure_action_serves(self) -> None:
        """Regression (QA D1): ``get`` showed ``{}`` over a 7-key map; the setup version now carries it."""
        svc, setup_id = await self._service_indexed({"llm.model": "which model answers"})

        got = _env(await svc.services_manager(GetServiceAction(setup_id=setup_id)))
        shape = _env(await svc.services_manager(StructureServiceAction(setup_id=setup_id)))

        assert got["output"]["current_setup_version"]["structure"] == {"llm.model": "which model answers"}
        assert got["output"]["current_setup_version"]["structure"] == shape["output"]

    @pytest.mark.regression
    async def test_get_keeps_the_key_when_no_map_is_indexed(self) -> None:
        """Regression (QA D1 replay): omitting the key moved D7's KeyError from search to get."""
        svc, setup_id = await self._service_indexed({})

        env = _env(await svc.services_manager(GetServiceAction(setup_id=setup_id)))

        assert env["output"]["current_setup_version"]["structure"] == {}

    @pytest.mark.regression
    async def test_set_version_serves_the_structure(self) -> None:
        """Regression (QA D1): the version a set_version activates serves its map too."""
        svc, setup_id = await self._service_indexed({"llm.model": "which model answers"})
        versions = _env(await svc.services_manager(ListVersionsAction(setup_id=setup_id)))["output"]["versions"]

        await svc.services_manager(
            SetServiceVersionAction(setup_id=setup_id, setup_version_id=versions[0]["setup_version_id"])
        )

        env = _env(await svc.services_manager(StructureServiceAction(setup_id=setup_id)))
        assert env["output"] == {"llm.model": "which model answers"}

    async def test_change_visibility_serves_the_structure(self) -> None:
        svc, setup_id = await self._service_indexed({"llm.model": "which model answers"})

        env = _env(await svc.services_manager(ChangeVisibilityAction(setup_id=setup_id, visibility="internal")))

        assert env["output"]["current_setup_version"]["structure"] == {"llm.model": "which model answers"}

    async def test_a_tools_get_carries_no_structure(self) -> None:
        """Tools have no map, so their reads never show the key — nor pay the search call."""
        setup, registry = _stores()
        registry.search_setups = AsyncMock()  # type: ignore[method-assign]

        env = _env(await ToolsManager(setup, registry).tools_manager(GetAction(setup_id="setups:duda")))

        assert "structure" not in env["output"]["current_setup_version"]
        registry.search_setups.assert_not_awaited()

    async def test_structure_returns_the_stored_map_and_no_content(self) -> None:
        setup, registry = _stores()
        created = await setup.create_setup({
            "name": "N",
            "content": {"llm": {"model": "gpt-4o"}},
            "structure": {"llm.model": "which model answers"},
        })

        class _WithMap(DefaultRegistry):
            async def search_setups(self, *args: Any, **kwargs: Any) -> list[SetupSummary]:
                return [
                    SetupSummary(
                        setup_id=created.id,
                        name="N",
                        module_type=RegistryModuleType.SERVICE,
                        structure=created.current_setup_version.structure,
                    )
                ]

        registry_with_map = _WithMap("", "", "")
        registry_with_map._modules = registry._modules
        raw = await ServicesManager(setup, registry_with_map).services_manager(
            StructureServiceAction(setup_id=created.id)
        )

        assert _env(raw)["output"] == {"llm.model": "which model answers"}
        assert "gpt-4o" not in raw

    async def test_structure_reads_the_version_not_the_search_index(self) -> None:
        """The map comes from the current version, so a search summary that lags or carries none does not matter.

        DefaultRegistry mirrors no map, which is the legacy/backfill-pending case on the wire (DEV-949).
        """
        svc, setup_id = await self._service()

        assert _env(await svc.services_manager(StructureServiceAction(setup_id=setup_id)))["output"] == {
            "llm.model": "which model answers",
            "region": "where it runs",
        }

    async def test_structure_refuses_another_object_type(self) -> None:
        """The type gate applies to the shape read as much as to the content read."""
        raw = await ServicesManager(*_stores()).services_manager(StructureServiceAction(setup_id="setups:duda"))

        assert json.loads(raw)["metadata"]["success"] is False
        assert "not a service setup" in json.loads(raw)["error"]


class TestStructurePaging:
    """``structure`` pages a large map, narrows it to a branch, and refuses it whole over the limit (DEV-949)."""

    @staticmethod
    async def _service(structure: dict[str, str] | None) -> tuple[ServicesManager, str]:
        setup, registry = _stores()
        created = await setup.create_setup({"name": "N", "content": {"a": 1}, "structure": structure})
        return ServicesManager(setup, registry), created.id

    @staticmethod
    async def _structure(manager: ServicesManager, setup_id: str, **fields: Any) -> dict[str, Any]:
        return _env(await manager.services_manager(StructureServiceAction(setup_id=setup_id, **fields)))

    @staticmethod
    def _big_map() -> dict[str, str]:
        return {f"items[{index}].name": "x" * 40 for index in reversed(range(100))}

    async def test_a_large_map_without_paging_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_STRUCTURE_MAX_CHARS", "1000")
        manager, setup_id = await self._service(self._big_map())

        env = await self._structure(manager, setup_id)

        assert env["metadata"]["success"] is False
        assert "100 entries" in env["error"]
        assert "offset and limit" in env["error"]
        assert "search_structure" in env["error"]

    async def test_pages_entries_sorted_by_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_STRUCTURE_MAX_CHARS", "1000")
        manager, setup_id = await self._service({"b": "second", "a": "first", "c": "third"})

        output = (await self._structure(manager, setup_id, offset=0, limit=2))["output"]

        assert output == {"entries": {"a": "first", "b": "second"}, "offset": 0, "total": 3, "next_offset": 2}
        assert list(output["entries"]) == ["a", "b"]

    async def test_a_page_stays_within_the_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_STRUCTURE_MAX_CHARS", "1000")
        manager, setup_id = await self._service(self._big_map())

        output = (await self._structure(manager, setup_id, offset=0))["output"]

        assert 0 < len(output["entries"]) < 100
        assert len(json.dumps(output["entries"])) <= 1000
        assert output["total"] == 100
        assert output["next_offset"] == len(output["entries"])

    async def test_prefix_keeps_one_branch(self) -> None:
        manager, setup_id = await self._service({"a": "1", "a.b": "2", "a[0]": "3", "ab": "4", 'a["x.y"]': "5"})

        output = (await self._structure(manager, setup_id, prefix="a"))["output"]

        assert output == {"a": "1", "a.b": "2", "a[0]": "3", 'a["x.y"]': "5"}

    async def test_prefix_pages_its_branch(self) -> None:
        manager, setup_id = await self._service({"a.c": "1", "a.b": "2", "z": "3"})

        output = (await self._structure(manager, setup_id, prefix="a", offset=1))["output"]

        assert output == {"entries": {"a.c": "1"}, "offset": 1, "total": 2, "next_offset": None}

    async def test_prefix_matching_nothing_answers_an_empty_map(self) -> None:
        manager, setup_id = await self._service({"a": "1"})

        assert (await self._structure(manager, setup_id, prefix="zz"))["output"] == {}

    async def test_a_version_without_a_map_answers_an_empty_map(self) -> None:
        manager, setup_id = await self._service(None)

        assert (await self._structure(manager, setup_id))["output"] == {}
        assert (await self._structure(manager, setup_id, offset=0))["output"] == {
            "entries": {},
            "offset": 0,
            "total": 0,
            "next_offset": None,
        }

    async def test_a_small_map_is_unchanged(self) -> None:
        manager, setup_id = await self._service({"b": "2", "a": "1"})

        assert list((await self._structure(manager, setup_id))["output"].items()) == [("b", "2"), ("a", "1")]


class TestSearchStructure:
    """``search_structure`` finds map entries by the words of their path or description, paged (DEV-949)."""

    _MAP: ClassVar[dict[str, str]] = {
        "catalog.products[0].sku": "P001 drill",
        "catalog.products[1].sku": "P002 hammer",
        "catalog.title": "Shop name",
        "pricing.currency": "Currency of every price, EUR",
    }

    @staticmethod
    async def _search(query: str, **fields: Any) -> dict[str, Any]:
        setup, registry = _stores()
        created = await setup.create_setup({"name": "N", "content": {"a": 1}, "structure": TestSearchStructure._MAP})
        manager = ServicesManager(setup, registry)
        return _env(
            await manager.services_manager(SearchStructureServiceAction(setup_id=created.id, query=query, **fields))
        )

    async def test_finds_an_entry_by_a_word_of_its_path(self) -> None:
        output = (await self._search("currency"))["output"]

        assert output == {
            "entries": {"pricing.currency": "Currency of every price, EUR"},
            "offset": 0,
            "total": 1,
            "next_offset": None,
        }

    async def test_finds_an_entry_by_a_word_of_its_description(self) -> None:
        assert (await self._search("hammer"))["output"]["entries"] == {"catalog.products[1].sku": "P002 hammer"}

    async def test_every_word_must_match(self) -> None:
        assert list((await self._search("catalog sku P001"))["output"]["entries"]) == ["catalog.products[0].sku"]

    async def test_is_case_insensitive(self) -> None:
        assert list((await self._search("SHOP"))["output"]["entries"]) == ["catalog.title"]

    async def test_results_are_paged_and_sorted_by_path(self) -> None:
        output = (await self._search("catalog", offset=1, limit=1))["output"]

        assert output == {
            "entries": {"catalog.products[1].sku": "P002 hammer"},
            "offset": 1,
            "total": 3,
            "next_offset": 2,
        }

    async def test_no_match_is_an_empty_page(self) -> None:
        assert (await self._search("nothing-like-this"))["output"] == {
            "entries": {},
            "offset": 0,
            "total": 0,
            "next_offset": None,
        }

    async def test_a_query_without_a_word_is_refused(self) -> None:
        """A blank query would match every entry; the model sends JSON, so it is refused as an invalid action."""
        raw = await ServicesManager(*_stores()).services_manager(
            action={"action": "search_structure", "setup_id": "setups:x", "query": "   "}  # type: ignore[arg-type]
        )

        assert json.loads(raw)["metadata"]["success"] is False
        assert "query" in json.loads(raw)["error"]

    async def test_refuses_a_tool_setup(self) -> None:
        raw = await ServicesManager(*_stores()).services_manager(
            SearchStructureServiceAction(setup_id="setups:duda", query="x")
        )

        assert json.loads(raw)["metadata"]["success"] is False
        assert "not a service setup" in json.loads(raw)["error"]


class TestGetStructure:
    """``get`` returns a small map, refuses a large one, and drops it on ``structure=false`` (DEV-949)."""

    @staticmethod
    async def _get(stored: dict[str, str] | None, **fields: Any) -> dict[str, Any]:
        setup, registry = _stores()
        created = await setup.create_setup({"name": "N", "content": {"a": 1}, "structure": stored})
        manager = ServicesManager(setup, registry)
        return _env(await manager.services_manager(GetServiceAction(setup_id=created.id, **fields)))

    @staticmethod
    def _big_map() -> dict[str, str]:
        return {f"items[{index}].name": "x" * 40 for index in range(100)}

    async def test_a_small_map_is_returned_as_before(self) -> None:
        version = (await self._get({"a": "what a is"}))["output"]["current_setup_version"]

        assert version["structure"] == {"a": "what a is"}
        assert "structure_entries" not in version

    async def test_a_large_map_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_STRUCTURE_MAX_CHARS", "1000")

        env = await self._get(self._big_map())

        assert env["metadata"]["success"] is False
        assert "100 entries" in env["error"]
        assert "structure=false" in env["error"]

    async def test_structure_false_drops_the_map_and_counts_it(self) -> None:
        version = (await self._get({"a": "what a is"}, structure=False))["output"]["current_setup_version"]

        assert "structure" not in version
        assert "content" not in version
        assert version["structure_entries"] == 1

    async def test_structure_false_on_a_large_map_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_STRUCTURE_MAX_CHARS", "1000")

        env = await self._get(self._big_map(), structure=False)

        assert env["output"]["current_setup_version"]["structure_entries"] == 100
        assert "x" * 40 not in json.dumps(env)

    async def test_a_version_without_a_map_counts_zero(self) -> None:
        version = (await self._get(None, structure=False))["output"]["current_setup_version"]

        assert version["structure_entries"] == 0


class TestSearchRowStructure:
    """A ``search`` row shows a small map, and a text in place of a map too large to show (DEV-949)."""

    @staticmethod
    async def _rows(structure: dict[str, str]) -> list[dict[str, Any]]:
        setup, registry = _stores()

        class _WithMap(DefaultRegistry):
            async def search_setups(self, *args: Any, **kwargs: Any) -> list[SetupSummary]:
                return [
                    SetupSummary(
                        setup_id="setups:big",
                        name="Big",
                        module_type=RegistryModuleType.SERVICE,
                        setup_version="1.0.0",
                        structure=structure,
                    )
                ]

        indexed = _WithMap("", "", "")
        indexed._modules = registry._modules
        env = _env(await ServicesManager(setup, indexed).services_manager(SearchAction(query="big")))
        return env["output"]["setups"]

    async def test_a_small_map_is_shown(self) -> None:
        assert (await self._rows({"a": "what a is"}))[0]["structure"] == {"a": "what a is"}

    async def test_a_large_map_is_replaced_by_a_text(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_STRUCTURE_MAX_CHARS", "1000")

        row = (await self._rows({f"items[{index}].name": "x" * 40 for index in range(100)}))[0]

        assert isinstance(row["structure"], str)
        assert "100 entries" in row["structure"]
        assert "structure action" in row["structure"]
        assert "x" * 40 not in json.dumps(row)


class TestSearchRowsCarryTheStructure:
    """Discovery hands the shape over with the row, so choosing a scope costs no extra call."""

    @staticmethod
    def _registry_returning(structure: dict[str, str]) -> DefaultRegistry:
        """A registry whose single search hit carries ``structure``."""

        class _WithMap(DefaultRegistry):
            async def search_setups(self, *args: Any, **kwargs: Any) -> list[SetupSummary]:
                return [
                    SetupSummary(
                        setup_id="setups:nikita",
                        name="Nikita",
                        module_type=RegistryModuleType.SERVICE,
                        setup_version="1.0.0",
                        structure=structure,
                    )
                ]

        return _WithMap("", "", "")

    async def test_a_row_carries_the_stored_map(self) -> None:
        setup, _ = _stores()
        registry = self._registry_returning({"llm.model": "which model answers"})

        env = _env(await ServicesManager(setup, registry).services_manager(SearchAction(query="")))

        assert env["output"]["setups"][0]["structure"] == {"llm.model": "which model answers"}

    @pytest.mark.regression
    async def test_a_service_row_without_a_stored_map_renders_it_empty(self) -> None:
        """Regression (QA D7): every service row carries the key, so iterating rows cannot KeyError."""
        setup, _ = _stores()

        env = _env(await ServicesManager(setup, self._registry_returning({})).services_manager(SearchAction(query="")))

        assert env["output"]["setups"][0]["structure"] == {}

    @pytest.mark.regression
    async def test_a_row_never_carries_configuration_values(self) -> None:
        """Regression: a value-derived map on a search row would leak exactly what SetupSummary forbids.

        ``DefaultRegistry`` holds each setup's ``config``; deriving the map from it put
        secrets into search results. The map must come from what was authored, never from
        the values.
        """
        raw = await ToolsManager(*_stores()).tools_manager(SearchAction(query="duda"))

        assert "MUST-NOT-LEAK" not in raw
        assert "structure" not in json.loads(raw)["output"]["setups"][0]


@pytest.mark.validation
class TestStructureFieldValidation:
    """Schema-level contract of the fields the model fills in."""

    def test_create_rejects_a_non_string_summary(self) -> None:
        with pytest.raises(ValidationError):
            CreateServiceAction(name="n", content={"a": 1}, structure={"a": {"nested": "object"}})  # type: ignore[dict-item]

    def test_load_key_defaults_to_the_whole_document(self) -> None:
        assert LoadServiceAction(setup_id="s").key is None

    def test_load_rejects_a_non_string_key(self) -> None:
        with pytest.raises(ValidationError):
            LoadServiceAction(setup_id="s", key=["llm.model"])  # type: ignore[arg-type]

    def test_structure_action_takes_a_setup_id_and_paging(self) -> None:
        assert set(StructureServiceAction.model_fields) - {"action"} == {"setup_id", "prefix", "offset", "limit"}

    def test_structure_is_a_read_not_a_write(self) -> None:
        """``writes`` drives cache invalidation; a shape read must not trigger it."""
        assert StructureServiceAction.writes is False


@pytest.mark.validation
class TestDocumentationCap:
    """The action schema carries the protocol's 300-character cap, so the agent sees it up front."""

    def test_create_rejects_documentation_over_the_cap(self) -> None:
        with pytest.raises(ValidationError, match="at most 300 characters"):
            CreateServiceAction(name="n", content={"a": 1}, structure={}, documentation="x" * 301)

    def test_update_rejects_documentation_over_the_cap(self) -> None:
        with pytest.raises(ValidationError, match="at most 300 characters"):
            UpdateAction(setup_id="s", name="n", content={"a": 1}, documentation="x" * 301)

    def test_documentation_at_the_cap_is_accepted(self) -> None:
        assert (
            len(CreateServiceAction(name="n", content={"a": 1}, structure={}, documentation="x" * 300).documentation)
            == 300
        )

    def test_the_cap_is_declared_in_the_schema(self) -> None:
        assert CreateServiceAction.model_json_schema()["properties"]["documentation"]["maxLength"] == 300

    async def test_a_raw_call_over_the_cap_fails_cleanly(self) -> None:
        raw = await ServicesManager(*_stores()).services_manager({
            "action": "create",
            "name": "n",
            "content": {"a": 1},
            "structure": {},
            "documentation": "x" * 301,
        })

        envelope = json.loads(raw)
        assert envelope["metadata"]["success"] is False
        assert "documentation" in envelope["error"]


@pytest.mark.edge_case
class TestScopedReadBoundaries:
    """Boundaries of the two-step read that the happy path does not reach."""

    @staticmethod
    async def _service(content: dict[str, Any], structure: dict[str, str]) -> tuple[ServicesManager, str]:
        """A services manager over one setup with the given content and authored map."""
        svc = ServicesManager(*_stores())
        created = _env(await svc.services_manager(CreateServiceAction(name="N", content=content, structure=structure)))
        return svc, created["output"]["setup_id"]

    async def test_a_key_naming_a_section_returns_the_whole_section(self) -> None:
        """Containers stay fetchable, so an author may scope at section level."""
        svc, setup_id = await self._service({"llm": {"model": "gpt-4o", "temp": 0.2}}, {"llm": "model routing"})

        env = _env(await svc.services_manager(LoadServiceAction(setup_id=setup_id, key="llm")))

        assert env["output"] == {"llm": {"model": "gpt-4o", "temp": 0.2}}

    async def test_a_quoted_key_survives_the_round_trip(self) -> None:
        """The one real interop risk: a dotted key must not be split naively."""
        svc, setup_id = await self._service({"limits": {"max.tokens": 8000}}, {'limits["max.tokens"]': "ceiling"})

        env = _env(await svc.services_manager(LoadServiceAction(setup_id=setup_id, key='limits["max.tokens"]')))

        assert env["output"] == {'limits["max.tokens"]': 8000}

    async def test_a_malformed_key_fails_instead_of_returning_everything(self) -> None:
        """An unparseable path is a caller bug, so it is refused rather than read.

        Distinct from a merely *unresolvable* key, which the wire answers with NOT_FOUND.
        Here the path cannot be decoded at all.
        """
        svc, setup_id = await self._service({"a": 1}, {"a": "the a knob"})

        envelope = json.loads(await svc.services_manager(LoadServiceAction(setup_id=setup_id, key='a["')))

        assert envelope["metadata"]["success"] is False
        assert "malformed path" in envelope["error"]
        # A fail envelope carries no output key at all — asserting on its contents would
        # pass no matter what the action did.
        assert "output" not in envelope

    async def test_a_key_resolving_to_null_is_not_confused_with_a_miss(self) -> None:
        """A stored null is a real value; only an absent path is an error."""
        svc, setup_id = await self._service({"proxy": None}, {"proxy": "upstream proxy, unset by default"})

        env = _env(await svc.services_manager(LoadServiceAction(setup_id=setup_id, key="proxy")))

        assert env["metadata"]["success"] is True
        assert env["output"] == {"proxy": None}

    async def test_an_empty_map_is_stored_as_empty(self) -> None:
        """Nothing is derived on the agent's behalf: an empty map stays empty.

        Asserted on the store: the create response is a receipt, and a later ``get`` cannot show
        the map (GetSetupResponse has no structure field, and both strategies mirror that).
        """
        setup, registry = _stores()
        svc = ServicesManager(setup, registry)

        created = _env(await svc.services_manager(CreateServiceAction(name="N", content={"a": "one"}, structure={})))

        assert setup.setups[created["output"]["setup_id"]].current_setup_version.structure == {}


class TestConfigurationIsNotRewritten:
    """A service's own data must survive the envelope untouched.

    ``_jsonable`` rewrites ``visibility`` from the proto spelling (``VISIBILITY_INTERNAL``)
    to the caller's (``internal``) so a round-trip matches. That is right for a setup, and
    wrong for a *configuration*: ``load`` and ``structure`` return the service's own JSON,
    where a key called ``visibility`` belongs to the service and means whatever it says.
    The rewrite is confined to the model branch for that reason; these pin it.
    """

    @staticmethod
    async def _service_with(content: dict[str, Any], structure: dict[str, str]) -> tuple[ServicesManager, str]:
        """A services manager over one setup carrying the given configuration."""
        svc = ServicesManager(*_stores())
        created = _env(await svc.services_manager(CreateServiceAction(name="N", content=content, structure=structure)))
        return svc, created["output"]["setup_id"]

    async def test_load_returns_a_visibility_key_verbatim(self) -> None:
        content = {"visibility": "VISIBILITY_PRIVATE", "region": "eu"}
        svc, setup_id = await self._service_with(content, {"visibility": "who may call the upstream"})

        env = _env(await svc.services_manager(LoadServiceAction(setup_id=setup_id)))

        assert env["output"]["visibility"] == "VISIBILITY_PRIVATE"

    async def test_a_scoped_load_returns_a_visibility_key_verbatim(self) -> None:
        content = {"visibility": "VISIBILITY_INTERNAL"}
        svc, setup_id = await self._service_with(content, {"visibility": "who may call the upstream"})

        env = _env(await svc.services_manager(LoadServiceAction(setup_id=setup_id, key="visibility")))

        assert env["output"] == {"visibility": "VISIBILITY_INTERNAL"}

    async def test_a_setup_visibility_is_still_normalised(self) -> None:
        """The rewrite the configuration must escape is the one a setup still needs."""
        svc, setup_id = await self._service_with({"a": 1}, {"a": "knob"})
        await svc.services_manager(ChangeVisibilityAction(setup_id=setup_id, visibility="internal"))

        env = _env(await svc.services_manager(GetServiceAction(setup_id=setup_id)))

        assert env["output"]["visibility"] == "internal"


class TestPatchAndAppend:
    """``patch`` changes one existing element, ``append`` adds one; both cut a new version."""

    _BASE_MAP: ClassVar[dict[str, str]] = {"llm.model": "which model answers", "tools": "what it may call"}

    @classmethod
    async def _service(cls) -> tuple[ServicesManager, str]:
        """A services manager over one service with a key map and documentation."""
        manager = ServicesManager(*_stores())
        created = _env(
            await manager.services_manager(
                CreateServiceAction(
                    name="Brand",
                    content={"llm": {"model": "small"}, "tools": ["search"]},
                    documentation="the house voice",
                    structure=cls._BASE_MAP,
                )
            )
        )
        return manager, created["output"]["setup_id"]

    @staticmethod
    async def _current(manager: ServicesManager, setup_id: str) -> dict[str, Any]:
        version = _env(await manager.services_manager(GetServiceAction(setup_id=setup_id)))["output"]
        version = version["current_setup_version"]
        version["content"] = _env(await manager.services_manager(LoadServiceAction(setup_id=setup_id)))["output"]
        return version

    @staticmethod
    async def _version_count(manager: ServicesManager, setup_id: str) -> int:
        return _env(await manager.services_manager(ListVersionsAction(setup_id=setup_id)))["output"]["total_count"]

    async def test_patch_changes_one_key_and_keeps_the_rest(self) -> None:
        manager, setup_id = await self._service()

        env = _env(
            await manager.services_manager(
                PatchServiceAction(
                    setup_id=setup_id, path="llm.model", value="large", structure={"llm.model": "which model answers"}
                )
            )
        )

        assert env["metadata"]["success"] is True
        assert (await self._current(manager, setup_id))["content"] == {"llm": {"model": "large"}, "tools": ["search"]}
        assert await self._version_count(manager, setup_id) == 2

    async def test_patch_refuses_an_absent_path_and_cuts_nothing(self) -> None:
        """Patch never creates, so a mistyped key cannot slip a stray entry into the service."""
        manager, setup_id = await self._service()

        env = _env(
            await manager.services_manager(
                PatchServiceAction(setup_id=setup_id, path="llm.modle", value="x", structure={})
            )
        )

        assert env["metadata"]["success"] is False
        assert "append" in env["error"]
        assert await self._version_count(manager, setup_id) == 1

    async def test_append_adds_a_new_key_and_its_entry(self) -> None:
        manager, setup_id = await self._service()

        await manager.services_manager(
            AppendServiceAction(
                setup_id=setup_id, path="llm.temperature", value=0.2, structure={"llm.temperature": "how creative"}
            )
        )

        current = await self._current(manager, setup_id)
        assert current["content"]["llm"] == {"model": "small", "temperature": 0.2}
        assert current["structure"] == {**self._BASE_MAP, "llm.temperature": "how creative"}

    async def test_append_pushes_onto_a_list_and_describes_the_new_item(self) -> None:
        manager, setup_id = await self._service()

        await manager.services_manager(
            AppendServiceAction(
                setup_id=setup_id, path="tools", value="fetch", structure={"tools[1]": "the page fetcher"}
            )
        )

        current = await self._current(manager, setup_id)
        assert current["content"]["tools"] == ["search", "fetch"]
        assert current["structure"] == {**self._BASE_MAP, "tools[1]": "the page fetcher"}

    async def test_append_refuses_an_existing_value_and_cuts_nothing(self) -> None:
        manager, setup_id = await self._service()

        env = _env(
            await manager.services_manager(AppendServiceAction(setup_id=setup_id, path="llm", value="x", structure={}))
        )

        assert env["metadata"]["success"] is False
        assert "patch" in env["error"]
        assert await self._version_count(manager, setup_id) == 1

    async def test_a_patched_section_is_described_again(self) -> None:
        """Replacing a section removes its keys, so their entries go and the call's own come in."""
        manager, setup_id = await self._service()

        await manager.services_manager(
            PatchServiceAction(
                setup_id=setup_id,
                path="llm",
                value={"name": "large", "temperature": 0.2},
                structure={"llm.name": "which model answers", "llm.temperature": "how creative"},
            )
        )

        assert (await self._current(manager, setup_id))["structure"] == {
            "tools": "what it may call",
            "llm.name": "which model answers",
            "llm.temperature": "how creative",
        }

    async def test_an_empty_map_leaves_the_edited_element_undescribed(self) -> None:
        """The call describes what it writes, as on create: an empty map means no entry for it."""
        manager, setup_id = await self._service()

        await manager.services_manager(
            PatchServiceAction(setup_id=setup_id, path="llm.model", value="large", structure={})
        )

        assert (await self._current(manager, setup_id))["structure"] == {"tools": "what it may call"}

    async def test_entries_outside_the_edited_element_are_kept(self) -> None:
        manager, setup_id = await self._service()

        await manager.services_manager(
            AppendServiceAction(setup_id=setup_id, path="region", value="eu", structure={"region": "where it runs"})
        )

        assert (await self._current(manager, setup_id))["structure"] == {**self._BASE_MAP, "region": "where it runs"}

    async def test_an_entry_guessing_the_wrong_list_index_is_refused_and_names_the_real_one(self) -> None:
        """A push lands at the end; an entry keyed elsewhere would describe another item."""
        manager, setup_id = await self._service()

        env = _env(
            await manager.services_manager(
                AppendServiceAction(
                    setup_id=setup_id, path="tools", value="fetch", structure={"tools[0]": "the page fetcher"}
                )
            )
        )

        assert env["metadata"]["success"] is False
        assert "tools[1]" in env["error"]
        assert await self._version_count(manager, setup_id) == 1

    async def test_an_entry_naming_a_missing_key_is_refused(self) -> None:
        manager, setup_id = await self._service()

        env = _env(
            await manager.services_manager(
                PatchServiceAction(
                    setup_id=setup_id, path="llm", value={"name": "large"}, structure={"llm.model": "stale"}
                )
            )
        )

        assert env["metadata"]["success"] is False
        assert "llm.model" in env["error"]
        assert await self._version_count(manager, setup_id) == 1

    @pytest.mark.parametrize("action_class", [PatchServiceAction, AppendServiceAction])
    def test_a_structure_is_required(self, action_class: type[PatchServiceAction]) -> None:
        """Like create: an action that can add or remove keys must say what it writes."""
        with pytest.raises(ValidationError, match="structure"):
            action_class(setup_id="setups:x", path="a", value=1)  # type: ignore[call-arg]

    @pytest.mark.parametrize("action", ["patch", "append"])
    async def test_a_raw_call_without_a_structure_fails_cleanly(self, action: str) -> None:
        manager, setup_id = await self._service()

        env = _env(
            await manager.services_manager({"action": action, "setup_id": setup_id, "path": "region", "value": "eu"})
        )

        assert env["metadata"]["success"] is False
        assert "structure" in env["error"]

    @pytest.mark.parametrize(
        "action",
        [
            PatchServiceAction(setup_id="", path="llm.model", value="large", structure={}),
            AppendServiceAction(setup_id="", path="region", value="eu", structure={}),
        ],
    )
    async def test_the_name_and_documentation_carry_over(self, action: PatchServiceAction) -> None:
        """Unlike update, the configuration and its documentation never have to be resent."""
        manager, setup_id = await self._service()

        await manager.services_manager(action.model_copy(update={"setup_id": setup_id}))

        env = _env(await manager.services_manager(GetServiceAction(setup_id=setup_id)))
        assert env["output"]["name"] == "Brand"
        assert env["output"]["current_setup_version"]["documentation"] == "the house voice"

    async def test_an_edit_can_start_from_an_older_version(self) -> None:
        manager, setup_id = await self._service()
        first = (await self._current(manager, setup_id))["id"]
        await manager.services_manager(
            UpdateServiceAction(
                setup_id=setup_id, name="Brand", content={"llm": {"model": "BROKEN"}}, structure={"llm": "broken"}
            )
        )

        await manager.services_manager(
            AppendServiceAction(
                setup_id=setup_id,
                setup_version_id=first,
                path="region",
                value="eu",
                structure={"region": "where it runs"},
            )
        )

        current = await self._current(manager, setup_id)
        assert current["content"] == {"llm": {"model": "small"}, "tools": ["search"], "region": "eu"}
        assert current["structure"] == {**self._BASE_MAP, "region": "where it runs"}
        assert await self._version_count(manager, setup_id) == 3

    async def test_a_version_of_another_setup_is_refused(self) -> None:
        manager, setup_id = await self._service()
        other = _env(
            await manager.services_manager(
                CreateServiceAction(name="Other", content={"llm": {"model": "x"}}, structure={})
            )
        )
        foreign = other["output"]["setup_version_id"]

        env = _env(
            await manager.services_manager(
                PatchServiceAction(
                    setup_id=setup_id, setup_version_id=foreign, path="llm.model", value="x", structure={}
                )
            )
        )

        assert env["metadata"]["success"] is False
        assert "is not a version of" in env["error"]
        assert await self._version_count(manager, setup_id) == 1

    async def test_an_edit_can_be_staged_without_activating(self) -> None:
        manager, setup_id = await self._service()

        await manager.services_manager(
            PatchServiceAction(setup_id=setup_id, path="llm.model", value="large", structure={}, set_as_current=False)
        )

        assert (await self._current(manager, setup_id))["content"]["llm"]["model"] == "small"
        assert await self._version_count(manager, setup_id) == 2

    async def test_a_tool_setup_is_refused(self) -> None:
        env = _env(
            await ServicesManager(*_stores()).services_manager(
                PatchServiceAction(setup_id="setups:duda", path="secret", value="x", structure={})
            )
        )

        assert env["metadata"]["success"] is False
        assert "kind mismatch" in env["error"]

    async def test_a_key_persistence_would_mangle_is_refused(self) -> None:
        manager, setup_id = await self._service()

        env = _env(
            await manager.services_manager(
                AppendServiceAction(setup_id=setup_id, path="mood\U0001f525", value=1, structure={})
            )
        )

        assert env["metadata"]["success"] is False
        assert await self._version_count(manager, setup_id) == 1

    async def test_a_raw_flattened_call_dispatches(self) -> None:
        """Exactly what the model sends: the action's fields as siblings of the discriminator."""
        manager, setup_id = await self._service()

        env = _env(
            await manager.services_manager(
                "patch", setup_id=setup_id, path="llm.model", value="large", structure={"llm.model": "the model"}
            )
        )

        assert env["metadata"]["success"] is True
        assert (await self._current(manager, setup_id))["content"]["llm"]["model"] == "large"

    async def test_a_null_value_is_written_not_dropped(self) -> None:
        manager, setup_id = await self._service()

        await manager.services_manager({
            "action": "patch",
            "setup_id": setup_id,
            "path": "llm.model",
            "value": None,
            "structure": {},
        })

        assert (await self._current(manager, setup_id))["content"]["llm"] == {"model": None}


class TestPatchAndAppendSurface:
    """``patch`` and ``append`` are cache-invalidating writes, and exist on services only."""

    def test_both_actions_are_writes(self) -> None:
        """``writes`` drives the setup-cache invalidation; without it running jobs keep the old config."""
        assert PatchServiceAction.writes is True
        assert AppendServiceAction.writes is True

    async def test_both_actions_are_services_only(self) -> None:
        setup, registry = _stores()
        for action in ("patch", "append"):
            env = _env(
                await KinsManager(setup, registry).kins_manager({
                    "action": action,
                    "setup_id": "setups:isaac",
                    "path": "agent",
                    "value": "y",
                    "structure": {},
                })
            )
            assert env["metadata"]["success"] is False


class TestRemoveAction:
    """``remove`` deletes one element and keeps the map describing what is left."""

    _BASE_MAP: ClassVar[dict[str, str]] = {
        "llm.model": "which model answers",
        "tools[1]": "the fetcher",
        "tools[2]": "the browser",
    }

    @classmethod
    async def _service(cls) -> tuple[ServicesManager, str]:
        manager = ServicesManager(*_stores())
        created = _env(
            await manager.services_manager(
                CreateServiceAction(
                    name="Brand",
                    content={"llm": {"model": "small"}, "tools": ["search", "fetch", "browse"]},
                    documentation="the house voice",
                    structure=cls._BASE_MAP,
                )
            )
        )
        return manager, created["output"]["setup_id"]

    @staticmethod
    async def _current(manager: ServicesManager, setup_id: str) -> dict[str, Any]:
        version = _env(await manager.services_manager(GetServiceAction(setup_id=setup_id)))["output"]
        version = version["current_setup_version"]
        version["content"] = _env(await manager.services_manager(LoadServiceAction(setup_id=setup_id)))["output"]
        return version

    @staticmethod
    async def _version_count(manager: ServicesManager, setup_id: str) -> int:
        return _env(await manager.services_manager(ListVersionsAction(setup_id=setup_id)))["output"]["total_count"]

    async def test_removes_a_key_and_its_entry(self) -> None:
        manager, setup_id = await self._service()

        env = _env(await manager.services_manager(RemoveServiceAction(setup_id=setup_id, path="llm.model")))

        assert env["metadata"]["success"] is True
        current = await self._current(manager, setup_id)
        assert current["content"] == {"llm": {}, "tools": ["search", "fetch", "browse"]}
        assert current["structure"] == {"tools[1]": "the fetcher", "tools[2]": "the browser"}
        assert await self._version_count(manager, setup_id) == 2

    async def test_removing_a_list_item_renumbers_the_later_entries(self) -> None:
        """The browser moved from tools[2] to tools[1]; its entry must follow it, not vanish or lie."""
        manager, setup_id = await self._service()

        await manager.services_manager(RemoveServiceAction(setup_id=setup_id, path="tools[1]"))

        current = await self._current(manager, setup_id)
        assert current["content"]["tools"] == ["search", "browse"]
        assert current["structure"] == {"llm.model": "which model answers", "tools[1]": "the browser"}

    async def test_an_absent_path_is_refused_and_cuts_nothing(self) -> None:
        manager, setup_id = await self._service()

        env = _env(await manager.services_manager(RemoveServiceAction(setup_id=setup_id, path="llm.modle")))

        assert env["metadata"]["success"] is False
        assert "not found" in env["error"]
        assert await self._version_count(manager, setup_id) == 1

    async def test_an_empty_path_fails_cleanly(self) -> None:
        manager, setup_id = await self._service()

        env = _env(await manager.services_manager({"action": "remove", "setup_id": setup_id, "path": ""}))

        assert env["metadata"]["success"] is False
        assert "path" in env["error"]

    async def test_the_name_and_documentation_carry_over(self) -> None:
        manager, setup_id = await self._service()

        await manager.services_manager(RemoveServiceAction(setup_id=setup_id, path="tools[0]"))

        env = _env(await manager.services_manager(GetServiceAction(setup_id=setup_id)))
        assert env["output"]["name"] == "Brand"
        assert env["output"]["current_setup_version"]["documentation"] == "the house voice"

    async def test_a_removal_can_start_from_an_older_version(self) -> None:
        manager, setup_id = await self._service()
        first = (await self._current(manager, setup_id))["id"]
        await manager.services_manager(
            UpdateServiceAction(setup_id=setup_id, name="Brand", content={"llm": {"model": "BROKEN"}}, structure={})
        )

        await manager.services_manager(RemoveServiceAction(setup_id=setup_id, setup_version_id=first, path="tools[0]"))

        current = await self._current(manager, setup_id)
        assert current["content"] == {"llm": {"model": "small"}, "tools": ["fetch", "browse"]}
        assert current["structure"] == {
            "llm.model": "which model answers",
            "tools[0]": "the fetcher",
            "tools[1]": "the browser",
        }

    async def test_a_removal_can_be_staged_without_activating(self) -> None:
        manager, setup_id = await self._service()

        await manager.services_manager(RemoveServiceAction(setup_id=setup_id, path="llm", set_as_current=False))

        assert "llm" in (await self._current(manager, setup_id))["content"]
        assert await self._version_count(manager, setup_id) == 2

    async def test_a_tool_setup_is_refused(self) -> None:
        env = _env(
            await ServicesManager(*_stores()).services_manager(
                RemoveServiceAction(setup_id="setups:duda", path="secret")
            )
        )

        assert env["metadata"]["success"] is False
        assert "kind mismatch" in env["error"]

    async def test_a_raw_flattened_call_dispatches(self) -> None:
        manager, setup_id = await self._service()

        env = _env(await manager.services_manager("remove", setup_id=setup_id, path="llm.model"))

        assert env["metadata"]["success"] is True
        assert (await self._current(manager, setup_id))["content"]["llm"] == {}

    def test_it_takes_no_value_and_no_map(self) -> None:
        """Nothing is written, so there is nothing to describe: the map is updated from the removal."""
        assert "value" not in RemoveServiceAction.model_fields
        assert "structure" not in RemoveServiceAction.model_fields

    def test_it_is_a_write(self) -> None:
        assert RemoveServiceAction.writes is True

    async def test_it_is_services_only(self) -> None:
        env = _env(
            await KinsManager(*_stores()).kins_manager({
                "action": "remove",
                "setup_id": "setups:isaac",
                "path": "agent",
            })
        )

        assert env["metadata"]["success"] is False


class TestDottedListIndex:
    """A list index joined with "." (``items.0.title``, as Gemini writes it) is used as ``items[0].title``.

    The backend resolves only the bracket form: a dotted entry stored by create named nothing, and patch,
    append and remove refused the dotted path as not found (campaign r1, D-DOTIDX).
    """

    _CONTENT: ClassVar[dict[str, Any]] = {"items": [{"title": "a"}, {"title": "b"}]}

    @classmethod
    async def _service(cls, structure: dict[str, str]) -> tuple[ServicesManager, str]:
        manager = ServicesManager(*_stores())
        created = await manager.services_manager(
            CreateServiceAction(name="Catalog", content=cls._CONTENT, structure=structure)
        )
        return manager, _env(created)["output"]["setup_id"]

    @staticmethod
    async def _read(manager: ServicesManager, setup_id: str) -> tuple[dict[str, Any], dict[str, str]]:
        content = _env(await manager.services_manager(LoadServiceAction(setup_id=setup_id)))["output"]
        structure = _env(await manager.services_manager(StructureServiceAction(setup_id=setup_id)))["output"]
        return content, structure

    @pytest.mark.regression
    async def test_create_stores_the_bracket_form(self) -> None:
        manager, setup_id = await self._service({"items": "the items", "items.0.title": "first title"})

        assert (await self._read(manager, setup_id))[1] == {"items": "the items", "items[0].title": "first title"}

    @pytest.mark.regression
    async def test_update_stores_the_bracket_form(self) -> None:
        manager, setup_id = await self._service({})

        await manager.services_manager(
            UpdateServiceAction(
                setup_id=setup_id, name="Catalog", content=self._CONTENT, structure={"items.1.title": "second"}
            )
        )

        assert (await self._read(manager, setup_id))[1] == {"items[1].title": "second"}

    @pytest.mark.regression
    async def test_patch_takes_a_dotted_index(self) -> None:
        manager, setup_id = await self._service({})

        env = _env(
            await manager.services_manager(
                PatchServiceAction(
                    setup_id=setup_id, path="items.1.title", value="B", structure={"items.1.title": "second title"}
                )
            )
        )

        content, structure = await self._read(manager, setup_id)
        assert env["output"]["path"] == "items[1].title"
        assert content["items"][1] == {"title": "B"}
        assert structure == {"items[1].title": "second title"}

    async def test_append_takes_a_dotted_index_in_its_structure(self) -> None:
        manager, setup_id = await self._service({})

        env = _env(
            await manager.services_manager(
                AppendServiceAction(
                    setup_id=setup_id, path="items", value={"title": "c"}, structure={"items.2": "third item"}
                )
            )
        )

        assert env["output"]["path"] == "items[2]"
        assert (await self._read(manager, setup_id))[1] == {"items[2]": "third item"}

    @pytest.mark.regression
    async def test_remove_takes_a_dotted_index(self) -> None:
        manager, setup_id = await self._service({})

        env = _env(await manager.services_manager(RemoveServiceAction(setup_id=setup_id, path="items.0")))

        assert env["output"]["path"] == "items[0]"
        assert (await self._read(manager, setup_id))[0]["items"] == [{"title": "b"}]

    async def test_update_structure_takes_a_dotted_index(self) -> None:
        manager, setup_id = await self._service({})

        await manager.services_manager(
            UpdateStructureServiceAction(setup_id=setup_id, entries={"items.0.title": "first title"})
        )

        assert (await self._read(manager, setup_id))[1] == {"items[0].title": "first title"}


class TestWriteReceipts:
    """A service write answers a short receipt, never the configuration (DEV-937).

    Echoing the whole service after every write made a long edit loop fill the model's context.
    """

    _BIG = "x" * 5000

    @staticmethod
    async def _service() -> tuple[ServicesManager, str]:
        manager = ServicesManager(*_stores())
        created = _env(
            await manager.services_manager(
                CreateServiceAction(name="Catalog", content={"products": ["first"]}, structure={})
            )
        )
        return manager, created["output"]["setup_id"]

    @staticmethod
    async def _current(manager: ServicesManager, setup_id: str) -> dict[str, Any]:
        version = _env(await manager.services_manager(GetServiceAction(setup_id=setup_id)))["output"]
        version = version["current_setup_version"]
        version["content"] = _env(await manager.services_manager(LoadServiceAction(setup_id=setup_id)))["output"]
        return version

    async def test_create_answers_a_receipt(self) -> None:
        manager = ServicesManager(*_stores())
        content = {"products": [self._BIG]}

        raw = await manager.services_manager(CreateServiceAction(name="Catalog", content=content, structure={}))

        output = _env(raw)["output"]
        current = await self._current(manager, output["setup_id"])
        assert output == {
            "setup_id": output["setup_id"],
            "setup_version_id": current["id"],
            "version": current["version"],
            "path": "",
            "content_chars": len(json.dumps(content, ensure_ascii=False)),
        }
        assert self._BIG not in raw

    @pytest.mark.parametrize(
        ("action", "path"),
        [
            (PatchServiceAction(setup_id="", path="products[0]", value=_BIG, structure={}), "products[0]"),
            (AppendServiceAction(setup_id="", path="products", value=_BIG, structure={}), "products[1]"),
            (AppendServiceAction(setup_id="", path="notes", value=_BIG, structure={}), "notes"),
        ],
    )
    async def test_an_edit_answers_the_path_it_wrote(self, action: PatchServiceAction, path: str) -> None:
        """An item pushed onto a list is named by the index it landed at, for the next edit to use."""
        manager, setup_id = await self._service()

        raw = await manager.services_manager(action.model_copy(update={"setup_id": setup_id}))

        output = _env(raw)["output"]
        current = await self._current(manager, setup_id)
        assert output == {
            "setup_id": setup_id,
            "setup_version_id": current["id"],
            "version": current["version"],
            "path": path,
            "content_chars": len(json.dumps(current["content"], ensure_ascii=False)),
        }
        assert self._BIG not in raw

    async def test_remove_answers_a_receipt(self) -> None:
        manager, setup_id = await self._service()

        output = _env(await manager.services_manager(RemoveServiceAction(setup_id=setup_id, path="products[0]")))[
            "output"
        ]

        assert output["path"] == "products[0]"
        assert output["content_chars"] == len(json.dumps({"products": []}))

    async def test_update_answers_a_receipt(self) -> None:
        manager, setup_id = await self._service()
        content = {"products": [self._BIG, self._BIG]}

        raw = await manager.services_manager(
            UpdateServiceAction(setup_id=setup_id, name="Catalog", content=content, structure={})
        )

        output = _env(raw)["output"]
        assert output["setup_version_id"] == (await self._current(manager, setup_id))["id"]
        assert output["content_chars"] == len(json.dumps(content, ensure_ascii=False))
        assert self._BIG not in raw

    @pytest.mark.regression
    async def test_set_version_answers_a_receipt(self) -> None:
        """set_version echoed the whole configuration of the version it activated (campaign r1, D-SETVERSION-FULL)."""
        manager, setup_id = await self._service()
        first = await self._current(manager, setup_id)
        await manager.services_manager(
            UpdateServiceAction(setup_id=setup_id, name="Catalog", content={"products": [self._BIG]}, structure={})
        )

        raw = await manager.services_manager({
            "action": "set_version",
            "setup_id": setup_id,
            "setup_version_id": first["id"],
        })

        assert _env(raw)["output"] == {
            "setup_id": setup_id,
            "setup_version_id": first["id"],
            "version": first["version"],
            "path": "",
            "content_chars": len(json.dumps(first["content"], ensure_ascii=False)),
        }
        assert (await self._current(manager, setup_id))["id"] == first["id"]

    async def test_a_kin_set_version_still_answers_the_setup(self) -> None:
        """Only service writes answer receipts; the kins and tools managers keep their answers (DEV-937)."""
        manager = KinsManager(*_stores())

        env = _env(
            await manager.kins_manager(SetVersionAction(setup_id="setups:isaac", setup_version_id="setups:isaac:v"))
        )

        assert env["output"]["current_setup_version"]["content"] == {"agent": "x"}

    async def test_a_tool_update_still_answers_the_setup(self) -> None:
        """Receipts are a services change: tools and kins keep their full answer."""
        output = _env(
            await KinsManager(*_stores()).kins_manager(
                UpdateAction(setup_id="setups:isaac", name="Isaac", content={"agent": "y"})
            )
        )["output"]

        assert output["current_setup_version"]["content"] == {"agent": "y"}


class TestServiceWriteLock:
    """Parallel writes on one service run one after the other, each from the other's result (DEV-938).

    The store yields between the read and the write, as the gRPC setup service does, so without the lock
    every parallel edit starts from the same base and only the last one survives.
    """

    @staticmethod
    async def _yielding_service(content: dict[str, Any]) -> tuple[ServicesManager, DefaultSetup, str]:
        setup, registry = _stores()
        manager = ServicesManager(setup, registry)
        created = _env(
            await manager.services_manager(CreateServiceAction(name="Catalog", content=content, structure={}))
        )
        write = setup.update_setup

        async def slow_write(data: dict[str, Any]) -> SetupData:
            await asyncio.sleep(0)
            return await write(data)

        setup.update_setup = slow_write  # type: ignore[method-assign]
        return manager, setup, created["output"]["setup_id"]

    async def test_parallel_appends_on_one_service_all_land(self) -> None:
        manager, setup, setup_id = await self._yielding_service({"products": []})

        envelopes = await asyncio.gather(
            *(
                manager.services_manager(
                    AppendServiceAction(setup_id=setup_id, path="products", value={"id": index}, structure={})
                )
                for index in range(5)
            )
        )

        assert all(_env(raw)["output"]["path"].startswith("products[") for raw in envelopes)
        products = setup.setups[setup_id].current_setup_version.content["products"]
        assert sorted(item["id"] for item in products) == [0, 1, 2, 3, 4]

    async def test_parallel_patch_and_remove_keep_both(self) -> None:
        manager, setup, setup_id = await self._yielding_service({"a": 1, "b": 2})

        await asyncio.gather(
            manager.services_manager(PatchServiceAction(setup_id=setup_id, path="a", value=10, structure={})),
            manager.services_manager(RemoveServiceAction(setup_id=setup_id, path="b")),
        )

        assert setup.setups[setup_id].current_setup_version.content == {"a": 10}

    async def test_a_waiting_write_logs_the_validate_marker(self, caplog: pytest.LogCaptureFixture) -> None:
        manager, _, setup_id = await self._yielding_service({"products": []})

        with caplog.at_level("INFO"):
            await asyncio.gather(
                *(
                    manager.services_manager(
                        AppendServiceAction(setup_id=setup_id, path="products", value=index, structure={})
                    )
                    for index in range(2)
                )
            )

        assert "[VALIDATE SERVICE-WRITE-LOCK]" in caplog.text


class TestBatchAndTextAppend:
    """``append`` pushes several list items in one call, and adds text at the end of a text (DEV-938)."""

    @staticmethod
    async def _service(content: dict[str, Any], structure: dict[str, str]) -> tuple[ServicesManager, DefaultSetup, str]:
        setup, registry = _stores()
        manager = ServicesManager(setup, registry)
        created = _env(
            await manager.services_manager(CreateServiceAction(name="Catalog", content=content, structure=structure))
        )
        return manager, setup, created["output"]["setup_id"]

    async def test_items_push_every_item(self) -> None:
        manager, setup, setup_id = await self._service({"products": [0, 1, 2]}, {})

        env = _env(
            await manager.services_manager(
                AppendServiceAction(setup_id=setup_id, path="products", items=list(range(3, 23)), structure={})
            )
        )

        assert setup.setups[setup_id].current_setup_version.content["products"] == list(range(23))
        assert env["output"]["path"] == "products[22]"

    async def test_items_create_a_missing_list(self) -> None:
        manager, setup, setup_id = await self._service({"title": "x"}, {})

        await manager.services_manager(
            AppendServiceAction(setup_id=setup_id, path="catalog.products", items=[{"id": 1}, {"id": 2}], structure={})
        )

        assert setup.setups[setup_id].current_setup_version.content["catalog"] == {"products": [{"id": 1}, {"id": 2}]}

    async def test_items_map_entry_names_the_list_and_keeps_the_others(self) -> None:
        manager, setup, setup_id = await self._service({"products": [{"id": 0}]}, {"products[0]": "the first one"})

        await manager.services_manager(
            AppendServiceAction(
                setup_id=setup_id,
                path="products",
                items=[{"id": 1}],
                structure={"products": "catalog, each product {id}", "products[1].id": "its id"},
            )
        )

        assert setup.setups[setup_id].current_setup_version.structure == {
            "products[0]": "the first one",
            "products": "catalog, each product {id}",
            "products[1].id": "its id",
        }

    async def test_items_structure_outside_the_list_is_refused(self) -> None:
        manager, setup, setup_id = await self._service({"products": [], "other": 1}, {})

        env = _env(
            await manager.services_manager(
                AppendServiceAction(setup_id=setup_id, path="products", items=[1], structure={"other": "elsewhere"})
            )
        )

        assert env["metadata"]["success"] is False
        assert setup.setups[setup_id].current_setup_version.content["products"] == []

    async def test_items_on_a_non_list_is_refused(self) -> None:
        manager, _, setup_id = await self._service({"title": "x"}, {})

        env = _env(
            await manager.services_manager(
                AppendServiceAction(setup_id=setup_id, path="title", items=[1], structure={})
            )
        )

        assert env["metadata"]["success"] is False
        assert "patch" in env["error"]

    @pytest.mark.parametrize(
        "fields",
        [{"value": 1, "items": [1]}, {}, {"items": []}],
        ids=["both", "neither", "no items"],
    )
    async def test_value_xor_items(self, fields: dict[str, Any]) -> None:
        manager, setup, setup_id = await self._service({"products": []}, {})

        env = _env(
            await manager.services_manager({
                "action": "append",
                "setup_id": setup_id,
                "path": "products",
                "structure": {},
                **fields,
            })
        )

        assert env["metadata"]["success"] is False
        assert "items" in env["error"]
        assert setup.setups[setup_id].current_setup_version.content == {"products": []}

    async def test_a_null_value_is_still_one_value(self) -> None:
        manager, setup, setup_id = await self._service({"products": []}, {})

        await manager.services_manager({
            "action": "append",
            "setup_id": setup_id,
            "path": "products",
            "value": None,
            "structure": {},
        })

        assert setup.setups[setup_id].current_setup_version.content == {"products": [None]}

    async def test_text_value_on_a_text_is_concatenated(self) -> None:
        manager, setup, setup_id = await self._service({"notes": "abc"}, {"notes": "free notes"})

        env = _env(
            await manager.services_manager(
                AppendServiceAction(setup_id=setup_id, path="notes", value="def", structure={})
            )
        )

        version = setup.setups[setup_id].current_setup_version
        assert version.content == {"notes": "abcdef"}
        assert version.structure == {"notes": "free notes"}
        assert env["output"]["path"] == "notes"

    async def test_non_text_value_on_a_text_is_refused(self) -> None:
        manager, _, setup_id = await self._service({"notes": "abc"}, {})

        env = _env(
            await manager.services_manager(AppendServiceAction(setup_id=setup_id, path="notes", value=1, structure={}))
        )

        assert env["metadata"]["success"] is False
        assert "patch" in env["error"]


class TestPagedLoad:
    """``load`` reads a large list, text or object page by page, and refuses a huge part unpaged (DEV-938, DEV-949)."""

    @staticmethod
    async def _service(content: dict[str, Any]) -> tuple[ServicesManager, str]:
        manager = ServicesManager(*_stores())
        created = _env(await manager.services_manager(CreateServiceAction(name="Big", content=content, structure={})))
        return manager, created["output"]["setup_id"]

    @staticmethod
    async def _load(manager: ServicesManager, setup_id: str, **fields: Any) -> dict[str, Any]:
        return _env(await manager.services_manager(LoadServiceAction(setup_id=setup_id, **fields)))

    async def test_a_list_page(self) -> None:
        manager, setup_id = await self._service({"products": list(range(50))})

        env = await self._load(manager, setup_id, key="products", offset=10, limit=5)

        assert env["output"] == {"page": [10, 11, 12, 13, 14], "offset": 10, "total": 50, "next_offset": 15}

    async def test_the_last_page_has_no_next(self) -> None:
        manager, setup_id = await self._service({"products": list(range(50))})

        env = await self._load(manager, setup_id, key="products", offset=48, limit=5)

        assert env["output"] == {"page": [48, 49], "offset": 48, "total": 50, "next_offset": None}

    async def test_an_offset_past_the_end_is_an_empty_page(self) -> None:
        manager, setup_id = await self._service({"products": list(range(3))})

        env = await self._load(manager, setup_id, key="products", offset=10, limit=5)

        assert env["output"] == {"page": [], "offset": 10, "total": 3, "next_offset": None}

    async def test_a_text_page(self) -> None:
        text = "".join(str(index % 10) for index in range(100))
        manager, setup_id = await self._service({"notes": text})

        output = (await self._load(manager, setup_id, key="notes", offset=90, limit=20))["output"]

        # The page opens on the last characters before the offset asked (a fifth of this tiny page).
        assert (output["page"], output["offset"], output["total"], output["next_offset"]) == (text[85:], 85, 100, None)
        assert output["part"].startswith("characters 85-100 of 100; its first 5 characters repeat")

    async def test_a_scalar_cannot_be_paged(self) -> None:
        manager, setup_id = await self._service({"title": 3})

        env = await self._load(manager, setup_id, key="title", offset=0, limit=5)

        assert env["metadata"]["success"] is False
        assert "a list, a text or an object" in env["error"]

    async def test_an_object_pages_by_its_keys(self) -> None:
        manager, setup_id = await self._service({"catalog": {"b": 1, "a": 2, "c": 3}})

        env = await self._load(manager, setup_id, key="catalog", offset=1, limit=1)

        assert env["output"] == {"page": {"a": 2}, "offset": 1, "total": 3, "next_offset": 2}

    async def test_the_whole_configuration_pages_by_its_keys(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "1000")
        manager, setup_id = await self._service({f"sku{index:04d}": "w" * 50 for index in range(200)})

        output = (await self._load(manager, setup_id, offset=0))["output"]

        assert 0 < len(output["page"]) < 200
        assert len(json.dumps(output["page"])) <= 1000
        assert next(iter(output["page"])) == "sku0000"
        assert output["total"] == 200
        assert output["next_offset"] == len(output["page"])

    async def test_a_large_list_without_paging_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "1000")
        manager, setup_id = await self._service({"products": ["x" * 98] * 30})

        env = await self._load(manager, setup_id, key="products")

        assert env["metadata"]["success"] is False
        assert "products is a list of 30 items" in env["error"]
        assert "offset and limit" in env["error"]
        assert "x" * 98 not in json.dumps(env)

    async def test_a_large_text_without_paging_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "1000")
        manager, setup_id = await self._service({"notes": "y" * 5000})

        env = await self._load(manager, setup_id, key="notes")

        assert env["metadata"]["success"] is False
        assert "notes is a text of 5000 characters" in env["error"]

    async def test_a_large_configuration_without_paging_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "1000")
        manager, setup_id = await self._service({"products": ["z" * 100] * 20, "title": "Shop", "meta": {"a": 1}})

        env = await self._load(manager, setup_id)

        assert env["metadata"]["success"] is False
        assert "The configuration is an object of 3 keys" in env["error"]
        assert "narrower key" in env["error"]

    async def test_a_large_nested_object_without_paging_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "1000")
        manager, setup_id = await self._service({"shop": {"products": ["z" * 100] * 20, "title": "Shop"}})

        env = await self._load(manager, setup_id, key="shop")

        assert env["metadata"]["success"] is False
        assert "shop is an object of 2 keys" in env["error"]

    async def test_a_page_of_one_oversized_value_names_its_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Paging an object never hands back one value larger than a page: it names the key to load (review #1)."""
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "1000")
        manager, setup_id = await self._service({"catalog": {"products": ["x" * 98] * 30}, "meta": 1})

        env = await self._load(manager, setup_id, offset=0)

        assert env["metadata"]["success"] is False
        assert "load key catalog" in env["error"]
        assert "offset 1" in env["error"]
        assert "x" * 98 not in json.dumps(env)

    async def test_a_page_of_one_oversized_item_names_its_index(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "1000")
        manager, setup_id = await self._service({"products": [{"a": "x" * 2000}, 1]})

        env = await self._load(manager, setup_id, key="products", offset=0)

        assert env["metadata"]["success"] is False
        assert "load key products[0]" in env["error"]

    async def test_a_nested_list_is_paged(self) -> None:
        """The setup service answers a keyed read as ``{key: part}``, whatever the key's depth."""
        manager, setup_id = await self._service({"catalog": {"products": list(range(50))}})

        env = await self._load(manager, setup_id, key="catalog.products", offset=10, limit=5)

        assert env["output"] == {"page": [10, 11, 12, 13, 14], "offset": 10, "total": 50, "next_offset": 15}

    async def test_a_bracket_quoted_text_is_paged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_TEXT_OVERLAP_CHARS", "0")
        manager, setup_id = await self._service({"limits": {"max.tokens": "abcdef"}})

        output = (await self._load(manager, setup_id, key='limits["max.tokens"]', offset=2))["output"]

        assert (output["page"], output["offset"], output["total"], output["next_offset"]) == ("cdef", 2, 6, None)

    async def test_a_limit_never_exceeds_one_page(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "1000")
        manager, setup_id = await self._service({"products": ["x" * 98] * 30, "notes": "y" * 5000})

        listed = (await self._load(manager, setup_id, key="products", offset=0, limit=30))["output"]
        text = (await self._load(manager, setup_id, key="notes", offset=0, limit=5000))["output"]

        assert len(json.dumps(listed["page"])) <= 1000
        assert listed["next_offset"] == len(listed["page"])
        assert 0 < len(json.dumps(text["page"])) <= 1000

    async def test_a_small_load_is_unchanged(self) -> None:
        manager, setup_id = await self._service({"products": [1, 2], "title": "Shop"})

        assert (await self._load(manager, setup_id))["output"] == {"products": [1, 2], "title": "Shop"}
        assert (await self._load(manager, setup_id, key="products"))["output"] == {"products": [1, 2]}

    async def test_service_get_has_no_content(self) -> None:
        manager, setup_id = await self._service({"products": ["secret-item"] * 3})

        raw = await manager.services_manager(GetServiceAction(setup_id=setup_id))

        version = _env(raw)["output"]["current_setup_version"]
        assert "content" not in version
        assert version["content_chars"] == len(json.dumps({"products": ["secret-item"] * 3}))
        assert "secret-item" not in raw

    async def test_tool_and_kin_get_keep_the_content(self) -> None:
        setup, registry = _stores()

        tool = _env(await ToolsManager(setup, registry).tools_manager(GetAction(setup_id="setups:duda")))
        kin = _env(await KinsManager(setup, registry).kins_manager(GetAction(setup_id="setups:isaac")))

        assert tool["output"]["current_setup_version"]["content"] == {"secret": "MUST-NOT-LEAK"}
        assert kin["output"]["current_setup_version"]["content"] == {"agent": "x"}


class TestTextPages:
    """A long text is paged at natural boundaries; each page opens on the end of the previous one (DEV-949)."""

    _TEXT: ClassVar[str] = "".join(f"Sentence number {index} talks about the catalog. " for index in range(400))

    @staticmethod
    async def _read(monkeypatch: pytest.MonkeyPatch, text: str, offset: int, overlap: str = "100") -> dict[str, Any]:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "1000")
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_TEXT_OVERLAP_CHARS", overlap)
        setup, registry = _stores()
        created = await setup.create_setup({"name": "N", "content": {"notes": text}, "structure": {}})
        manager = ServicesManager(setup, registry)
        env = await manager.services_manager(LoadServiceAction(setup_id=created.id, key="notes", offset=offset))
        return _env(env)["output"]

    async def _page(self, monkeypatch: pytest.MonkeyPatch, offset: int, overlap: str = "100") -> dict[str, Any]:
        return await self._read(monkeypatch, self._TEXT, offset, overlap)

    async def test_a_page_ends_on_a_sentence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        page = (await self._page(monkeypatch, 0))["page"]

        assert page.rstrip().endswith("catalog.")
        assert len(json.dumps(page)) <= 1000

    async def test_the_next_page_opens_on_the_end_of_this_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = await self._page(monkeypatch, 0)
        second = await self._page(monkeypatch, first["next_offset"])
        repeated = first["next_offset"] - second["offset"]

        assert first["next_offset"] == len(first["page"])
        assert 0 < repeated <= 100
        assert first["page"].endswith(second["page"][:repeated])
        assert f"its first {repeated} characters repeat the end of the previous page" in second["part"]
        assert len(json.dumps(second["page"])) <= 1000

    async def test_part_says_which_characters_and_what_follows(self, monkeypatch: pytest.MonkeyPatch) -> None:
        output = await self._page(monkeypatch, 0)

        assert output["part"].startswith(f"characters 0-{len(output['page'])} of {len(self._TEXT)}")
        assert f"next page starts at {output['next_offset']}" in output["part"]

    async def test_following_next_offset_reads_the_whole_text(self, monkeypatch: pytest.MonkeyPatch) -> None:
        read, offset = "", 0
        while offset is not None:
            output = await self._page(monkeypatch, offset)
            read = read[: output["offset"]] + output["page"]
            offset = output["next_offset"]

        assert read == self._TEXT
        assert "last page" in output["part"]

    async def test_without_overlap_a_page_starts_where_asked(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = await self._page(monkeypatch, 0, overlap="0")
        second = await self._page(monkeypatch, first["next_offset"], overlap="0")

        assert second["offset"] == first["next_offset"]

    async def test_an_offset_past_the_end_is_an_empty_last_page(self, monkeypatch: pytest.MonkeyPatch) -> None:
        output = await self._page(monkeypatch, len(self._TEXT) + 5)

        assert (output["page"], output["next_offset"]) == ("", None)

    async def test_pages_overlap_even_at_a_long_paragraph_boundary(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The splitter alone repeats only the blank line when a paragraph is longer than the overlap (Litmus D3)."""
        paragraph = "This fairly long sentence describes one precise maintenance step of the box. " * 4
        text = "\n\n".join([paragraph] * 6)
        first = await self._read(monkeypatch, text, 0)
        second = await self._read(monkeypatch, text, first["next_offset"])

        assert 20 < first["next_offset"] - second["offset"] <= 100

    async def test_pages_never_shrink_before_a_boundary(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A page ending on a paragraph followed by one too long to fit must not make the next pages shrink."""
        text = "A short closing paragraph of a few words.\n\n" + " ".join(["longword"] * 600)
        offset, pages = 0, 0
        while offset is not None:
            output = await self._read(monkeypatch, text, offset)
            assert output["next_offset"] is None or output["next_offset"] > offset
            offset, pages = output["next_offset"], pages + 1

        assert pages <= 8

    async def test_the_page_estimate_matches_the_pages_read(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = await self._page(monkeypatch, 0)
        pages, offset = 0, first["next_offset"]
        while offset is not None:
            offset = (await self._page(monkeypatch, offset))["next_offset"]
            pages += 1
        estimate = int(first["part"].split("about ")[1].split(" more")[0])

        assert abs(estimate - pages) <= 1

    @pytest.mark.regression
    async def test_a_short_page_does_not_inflate_the_estimate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A page ending early on a paragraph said "about 2254 more pages" for a few (campaign r1, D-PAGES)."""
        text = "A short closing paragraph of a few words.\n\n" + " ".join(["longword"] * 600)
        first = await self._read(monkeypatch, text, 0)
        pages, offset = 0, first["next_offset"]
        while offset is not None:
            offset = (await self._read(monkeypatch, text, offset))["next_offset"]
            pages += 1
        estimate = int(first["part"].split("about ")[1].split(" more")[0])

        assert abs(estimate - pages) <= 1


class _Writer:
    """A stand-in writer model: streams one reply per section, keyed by its instructions (``write <key>``).

    A reply is a text, streamed in two pieces, or an exception to raise. It records every request, the
    peak number of calls in flight, and how many calls were cancelled. It only streams: a writer called
    without streaming fails.
    """

    def __init__(self, replies: dict[str, list[Any]], delay: float = 0.0) -> None:
        self.replies = replies
        self.delay = delay
        self.requests: list[list[Message]] = []
        self.in_flight = 0
        self.peak = 0
        self.cancelled = 0

    async def ainvoke_stream(self, messages: list[Message], assistant_message: Message) -> AsyncIterator[ModelResponse]:
        assert assistant_message.role == "assistant"
        self.requests.append(messages)
        key = str(messages[1].content).rsplit("\n", 1)[-1].removeprefix("write ")
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        try:
            await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        finally:
            self.in_flight -= 1
        reply = self.replies[key].pop(0)
        if isinstance(reply, BaseException):
            raise reply
        half = len(reply) // 2
        yield ModelResponse(content=reply[:half])
        yield ModelResponse(reasoning_content="not part of the section")
        yield ModelResponse(content=reply[half:])


class TestGenerate:
    """``generate`` writes every section with its own model call and saves them as one version (DEV-938)."""

    @staticmethod
    async def _service(
        writer: _Writer | None, content: dict[str, Any] | None = None
    ) -> tuple[ServicesManager, DefaultSetup, str]:
        setup, registry = _stores()
        manager = ServicesManager(setup, registry)
        if writer is not None:
            manager.bind_writer(writer)  # type: ignore[arg-type]
        created = _env(
            await manager.services_manager(
                CreateServiceAction(name="Shop", content=content or {"title": "x"}, structure={"title": "the name"})
            )
        )
        return manager, setup, created["output"]["setup_id"]

    @staticmethod
    def _section(path: str, key: str, summary: str = "") -> ServiceSection:
        return ServiceSection(path=path, instructions=f"write {key}", summary=summary or f"about {path}")

    @staticmethod
    def _products(start: int, count: int) -> str:
        return json.dumps([{"id": index} for index in range(start, start + count)])

    async def test_sections_are_merged_into_one_version(self) -> None:
        writer = _Writer({
            "first": [self._products(0, 12)],
            "second": [self._products(12, 12)],
            "intro": ['"Welcome"'],
        })
        manager, setup, setup_id = await self._service(writer, {"title": "x", "products": []})

        env = _env(
            await manager.services_manager(
                GenerateServiceAction(
                    setup_id=setup_id,
                    brief="a shop",
                    sections=[
                        self._section("products", "first", "catalog, each {id}"),
                        self._section("products", "second", "catalog, each {id}"),
                        self._section("intro", "intro", "the welcome text"),
                    ],
                )
            )
        )

        version = setup.setups[setup_id].current_setup_version
        assert sorted(item["id"] for item in version.content["products"]) == list(range(24))
        assert version.content["intro"] == "Welcome"
        assert version.structure == {"title": "the name", "products": "catalog, each {id}", "intro": "the welcome text"}
        assert _env(await manager.services_manager(ListVersionsAction(setup_id=setup_id)))["output"]["total_count"] == 2
        output = env["output"]
        assert output["setup_version_id"] == version.id
        assert output["sections"][0] == {
            "section": 0,
            "path": "products",
            "chars": len(self._products(0, 12)),
            "items": 12,
        }
        assert output["failed"] == []
        assert "Welcome" not in json.dumps(output)

    async def test_a_described_list_keeps_its_entry(self) -> None:
        """A retry pushing one slice onto a list must not replace the whole list's description by the slice's."""
        writer = _Writer({"slice": [self._products(0, 2)]})
        setup, registry = _stores()
        manager = ServicesManager(setup, registry)
        manager.bind_writer(writer)  # type: ignore[arg-type]
        setup_id = _env(
            await manager.services_manager(
                CreateServiceAction(name="Shop", content={"products": []}, structure={"products": "the catalog"})
            )
        )["output"]["setup_id"]

        await manager.services_manager(
            GenerateServiceAction(
                setup_id=setup_id, brief="b", sections=[self._section("products", "slice", "products P441 to P450")]
            )
        )

        assert setup.setups[setup_id].current_setup_version.structure == {"products": "the catalog"}

    async def test_500_items_are_all_pushed(self) -> None:
        writer = _Writer({f"part{index:02d}": [self._products(index * 12, 12)] for index in range(42)})
        manager, setup, setup_id = await self._service(writer, {"products": []})

        await manager.services_manager(
            GenerateServiceAction(
                setup_id=setup_id,
                brief="a shop",
                sections=[self._section("products", f"part{index:02d}") for index in range(42)],
            )
        )

        assert len(setup.setups[setup_id].current_setup_version.content["products"]) == 504

    async def test_an_existing_non_list_is_replaced(self) -> None:
        writer = _Writer({"title": ['"Better title"']})
        manager, setup, setup_id = await self._service(writer)

        await manager.services_manager(
            GenerateServiceAction(setup_id=setup_id, brief="b", sections=[self._section("title", "title")])
        )

        assert setup.setups[setup_id].current_setup_version.content == {"title": "Better title"}

    async def test_a_list_reply_on_a_missing_path_creates_the_list(self) -> None:
        writer = _Writer({"items": [self._products(0, 3)]})
        manager, setup, setup_id = await self._service(writer)

        await manager.services_manager(
            GenerateServiceAction(setup_id=setup_id, brief="b", sections=[self._section("catalog.products", "items")])
        )

        assert setup.setups[setup_id].current_setup_version.content["catalog"] == {
            "products": [{"id": 0}, {"id": 1}, {"id": 2}]
        }

    async def test_invalid_json_is_retried_once(self) -> None:
        writer = _Writer({"intro": ["Sure! here it is", '"Welcome"']})
        manager, setup, setup_id = await self._service(writer)

        await manager.services_manager(
            GenerateServiceAction(setup_id=setup_id, brief="b", sections=[self._section("intro", "intro")])
        )

        assert setup.setups[setup_id].current_setup_version.content["intro"] == "Welcome"
        assert len(writer.requests) == 2
        assert "not valid JSON" in str(writer.requests[1][-1].content)

    async def test_a_code_fence_is_unwrapped(self) -> None:
        writer = _Writer({"intro": ['```json\n{"a": 1}\n```']})
        manager, setup, setup_id = await self._service(writer)

        await manager.services_manager(
            GenerateServiceAction(setup_id=setup_id, brief="b", sections=[self._section("intro", "intro")])
        )

        assert setup.setups[setup_id].current_setup_version.content["intro"] == {"a": 1}

    async def test_a_failed_section_is_reported_and_the_others_saved(self, caplog: pytest.LogCaptureFixture) -> None:
        writer = _Writer({
            "slow": [TimeoutError("cut by the proxy")],
            "chatty": ["no json", "still no json"],
            "intro": ['"Welcome"'],
        })
        manager, setup, setup_id = await self._service(writer)

        with caplog.at_level("WARNING"):
            output = _env(
                await manager.services_manager(
                    GenerateServiceAction(
                        setup_id=setup_id,
                        brief="b",
                        sections=[
                            self._section("a", "slow"),
                            self._section("b", "chatty"),
                            self._section("intro", "intro"),
                        ],
                    )
                )
            )["output"]

        assert setup.setups[setup_id].current_setup_version.content == {"title": "x", "intro": "Welcome"}
        assert [item["path"] for item in output["failed"]] == ["a", "b"]
        assert [item["section"] for item in output["failed"]] == [0, 1]
        assert [item["instructions"] for item in output["failed"]] == ["write slow", "write chatty"]
        assert "cut by the proxy" in output["failed"][0]["reason"]
        assert "JSON" in output["failed"][1]["reason"]
        assert "split" in output["hint"].lower()
        assert "write slow" in caplog.text
        assert "write chatty" in caplog.text

    async def test_failed_sections_on_one_list_are_told_apart(self) -> None:
        """Sections filling one list share a path; the receipt names which ones failed, to redo only those."""
        writer = _Writer({"p1": [self._products(0, 2)], "p2": ["x", "y"], "p3": [self._products(4, 2)]})
        manager, _, setup_id = await self._service(writer, {"products": []})

        output = _env(
            await manager.services_manager(
                GenerateServiceAction(
                    setup_id=setup_id,
                    brief="b",
                    sections=[self._section("products", key) for key in ("p1", "p2", "p3")],
                )
            )
        )["output"]

        assert output["failed"] == [
            {"section": 1, "path": "products", "instructions": "write p2", "reason": output["failed"][0]["reason"]}
        ]
        assert [item["section"] for item in output["sections"]] == [0, 2]

    async def test_all_sections_failed_saves_nothing(self) -> None:
        writer = _Writer({"slow": [TimeoutError("cut")]})
        manager, _setup, setup_id = await self._service(writer)

        env = _env(
            await manager.services_manager(
                GenerateServiceAction(setup_id=setup_id, brief="b", sections=[self._section("a", "slow")])
            )
        )

        assert env["metadata"]["success"] is False
        assert "cut" in env["error"]
        assert _env(await manager.services_manager(ListVersionsAction(setup_id=setup_id)))["output"]["total_count"] == 1

    async def test_concurrency_is_bounded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_WRITER_CONCURRENCY", "2")
        writer = _Writer({f"part{index}": ["1"] for index in range(6)}, delay=0.01)
        manager, _, setup_id = await self._service(writer, {"numbers": []})

        await manager.services_manager(
            GenerateServiceAction(
                setup_id=setup_id, brief="b", sections=[self._section("numbers", f"part{index}") for index in range(6)]
            )
        )

        assert manager._ctx_data.writer.peak == 2  # type: ignore[union-attr]  # bind_writer keeps a copy
        assert len(writer.requests) == 6

    async def test_no_writer_is_refused(self) -> None:
        manager, _, setup_id = await self._service(None)

        env = _env(
            await manager.services_manager(
                GenerateServiceAction(setup_id=setup_id, brief="b", sections=[self._section("a", "a")])
            )
        )

        assert env["metadata"]["success"] is False
        assert "model" in env["error"]

    async def test_cancel_stops_pending_writers(self) -> None:
        writer = _Writer({f"part{index}": ["1"] for index in range(3)}, delay=10)
        manager, setup, setup_id = await self._service(writer)
        before = setup.setups[setup_id].current_setup_version.id

        task = asyncio.ensure_future(
            manager.services_manager(
                GenerateServiceAction(
                    setup_id=setup_id,
                    brief="b",
                    sections=[self._section(f"k{index}", f"part{index}") for index in range(3)],
                )
            )
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert manager._ctx_data.writer.cancelled == 3  # type: ignore[union-attr]  # bind_writer keeps a copy
        assert setup.setups[setup_id].current_setup_version.id == before

    async def test_a_mission_cancelled_mid_run_saves_nothing(self) -> None:
        """A section refused because the mission was cancelled stops the whole write, not just that section."""
        writer = _Writer({
            "intro": ['"Welcome"'],
            "late": [RunCancelledException("task cancelled before model request")],
        })
        manager, setup, setup_id = await self._service(writer)
        before = setup.setups[setup_id].current_setup_version.id

        env = _env(
            await manager.services_manager(
                GenerateServiceAction(
                    setup_id=setup_id, brief="b", sections=[self._section("intro", "intro"), self._section("x", "late")]
                )
            )
        )

        assert env["metadata"]["success"] is False
        assert setup.setups[setup_id].current_setup_version.id == before

    async def test_merge_uses_the_version_current_at_save_time(self) -> None:
        writer = _Writer({"items": [self._products(0, 2)]}, delay=0.05)
        manager, setup, setup_id = await self._service(writer, {"products": []})

        generating = asyncio.ensure_future(
            manager.services_manager(
                GenerateServiceAction(setup_id=setup_id, brief="b", sections=[self._section("products", "items")])
            )
        )
        await asyncio.sleep(0)
        await manager.services_manager(
            AppendServiceAction(setup_id=setup_id, path="products", value={"id": 99}, structure={})
        )
        await generating

        assert sorted(item["id"] for item in setup.setups[setup_id].current_setup_version.content["products"]) == [
            0,
            1,
            99,
        ]

    async def test_writer_prompt_carries_brief_outline_and_instructions(self) -> None:
        writer = _Writer({"intro": ['"Welcome"'], "rules": ['"Be nice"']})
        manager, _, setup_id = await self._service(writer)

        await manager.services_manager(
            GenerateServiceAction(
                setup_id=setup_id,
                brief="A shop for owls",
                sections=[
                    self._section("intro", "intro", "the welcome"),
                    self._section("rules", "rules", "house rules"),
                ],
            )
        )

        prompt = str(writer.requests[0][1].content)
        assert "A shop for owls" in prompt
        assert "- intro: the welcome" in prompt
        assert "- rules: house rules" in prompt
        assert "`intro`" in prompt
        assert "write intro" in prompt
        assert "JSON" in str(writer.requests[0][0].content)


class TestWriterModel:
    """The generate writer is a copy of the agent's model: same request settings, no history compactor.

    Its reasoning is the agent's, untouched: the writer serves any kin's model, reasoning or not.
    """

    def test_the_writer_keeps_the_agent_request_settings(self) -> None:
        model = SafeLiteLLM(id="claude-sonnet-5", request_params={"num_retries": 0}, compactor=Mock())
        manager = ServicesManager(Mock(), Mock())

        manager.bind_writer(model)

        writer = manager._ctx_data.writer
        assert isinstance(writer, SafeLiteLLM)
        assert writer is not model
        assert writer.compactor is None
        assert writer.request_params == {"num_retries": 0}


class TestEscapedTextIsRefused:
    r"""A text a model escaped twice (``\\n``, ``\\u00e9`` written out) is refused before any write."""

    @staticmethod
    async def _call(manager: ServicesManager, action: dict[str, Any]) -> dict[str, Any]:
        return _env(await manager.services_manager(action=action))  # type: ignore[arg-type]

    @staticmethod
    async def _service() -> tuple[ServicesManager, str]:
        manager = ServicesManager(*_stores())
        created = _env(
            await manager.services_manager(
                CreateServiceAction(name="N", content={"note": "x", "items": []}, structure={})
            )
        )
        return manager, created["output"]["setup_id"]

    async def test_append_refuses_an_escaped_text(self) -> None:
        manager, setup_id = await self._service()

        env = await self._call(
            manager,
            {
                "action": "append",
                "setup_id": setup_id,
                "path": "note",
                "value": "\\n\\nChapitre 2 : c\\u00e2blage",
                "structure": {},
            },
        )

        assert env["metadata"]["success"] is False
        assert "escape sequences" in env["error"]

    async def test_append_refuses_an_escaped_item(self) -> None:
        manager, setup_id = await self._service()

        env = await self._call(
            manager,
            {"action": "append", "setup_id": setup_id, "path": "items", "items": ["ok", "caf\\u00e9"], "structure": {}},
        )

        assert env["metadata"]["success"] is False
        assert "escape sequences" in env["error"]

    async def test_patch_refuses_an_escaped_text(self) -> None:
        manager, setup_id = await self._service()

        env = await self._call(
            manager,
            {"action": "patch", "setup_id": setup_id, "path": "note", "value": "a\\nb\\nc", "structure": {}},
        )

        assert env["metadata"]["success"] is False
        assert "escape sequences" in env["error"]

    async def test_create_refuses_escaped_content(self) -> None:
        env = await self._call(
            ServicesManager(*_stores()),
            {"action": "create", "name": "N", "content": {"notice": "c\\u00e2blage"}, "structure": {}},
        )

        assert env["metadata"]["success"] is False
        assert "'notice'" in env["error"]

    async def test_a_real_text_is_appended(self) -> None:
        manager, setup_id = await self._service()

        env = await self._call(
            manager,
            {"action": "append", "setup_id": setup_id, "path": "note", "value": "\nDeuxième ligne", "structure": {}},
        )

        assert env["metadata"]["success"] is True

    async def test_a_generated_escaped_section_fails(self) -> None:
        writer = _Writer({"intro": ['"Chapitre 1 \\\\u00e9t\\\\u00e9"'], "title": ['"Clean title"']})
        manager, setup, setup_id = await TestGenerate._service(writer)

        output = _env(
            await manager.services_manager(
                GenerateServiceAction(
                    setup_id=setup_id,
                    brief="b",
                    sections=[TestGenerate._section("intro", "intro"), TestGenerate._section("title", "title")],
                )
            )
        )["output"]

        assert [item["path"] for item in output["failed"]] == ["intro"]
        assert "escape sequences" in output["failed"][0]["reason"]
        assert setup.setups[setup_id].current_setup_version.content["title"] == "Clean title"

    def test_value_is_typed_in_the_schema(self) -> None:
        """An untyped ``value`` left the model free to send an escaped copy of a text."""
        value = AppendServiceAction.model_json_schema()["properties"]["value"]

        assert {"string", "number", "boolean", "array", "object", "null"} <= {
            branch.get("type") for branch in value["anyOf"]
        }


class TestUpdateStructure:
    """``update_structure`` merges map entries into a new version, leaving the content untouched."""

    @staticmethod
    async def _service() -> tuple[ServicesManager, DefaultSetup, str]:
        setup, registry = _stores()
        created = await setup.create_setup({
            "name": "Shop",
            "content": {"products": [{"id": "P1"}, {"id": "P2"}], "limits": {"max.tokens": 3}},
            "structure": {"products": "Products P441 to P450", "products[1]": "old P2"},
            "documentation": "doc",
        })
        return ServicesManager(setup, registry), setup, created.id

    @staticmethod
    async def _update(manager: ServicesManager, setup_id: str, entries: dict[str, str]) -> dict[str, Any]:
        return _env(
            await manager.services_manager(
                action={"action": "update_structure", "setup_id": setup_id, "entries": entries}
            )  # type: ignore[arg-type]
        )

    async def test_entries_are_merged_and_the_content_kept(self) -> None:
        manager, setup, setup_id = await self._service()
        before = setup.setups[setup_id].current_setup_version

        env = await self._update(manager, setup_id, {"products[0]": "P1 drill", "products": "All 2 products"})

        after = setup.setups[setup_id].current_setup_version
        assert env["metadata"]["success"] is True
        assert after.id != before.id
        assert after.content == before.content
        assert after.documentation == "doc"
        assert after.structure == {"products": "All 2 products", "products[0]": "P1 drill", "products[1]": "old P2"}

    async def test_batches_add_up(self) -> None:
        manager, setup, setup_id = await self._service()

        await self._update(manager, setup_id, {"products[0]": "P1"})
        await self._update(manager, setup_id, {"products[1]": "P2"})

        structure = setup.setups[setup_id].current_setup_version.structure
        assert (structure["products[0]"], structure["products[1]"]) == ("P1", "P2")

    async def test_an_empty_description_removes_the_entry(self) -> None:
        manager, setup, setup_id = await self._service()

        await self._update(manager, setup_id, {"products[1]": "", "products[9]": ""})

        assert "products[1]" not in setup.setups[setup_id].current_setup_version.structure

    async def test_an_unknown_path_is_refused_and_nothing_written(self) -> None:
        manager, setup, setup_id = await self._service()
        before = setup.setups[setup_id].current_setup_version.id

        env = await self._update(manager, setup_id, {"products[0]": "ok", "products[7]": "no such item"})

        assert env["metadata"]["success"] is False
        assert "products[7]" in env["error"]
        assert setup.setups[setup_id].current_setup_version.id == before

    async def test_a_bracket_quoted_key_is_accepted(self) -> None:
        manager, setup, setup_id = await self._service()

        env = await self._update(manager, setup_id, {'limits["max.tokens"]': "token cap"})

        assert env["metadata"]["success"] is True
        assert setup.setups[setup_id].current_setup_version.structure['limits["max.tokens"]'] == "token cap"

    async def test_a_long_description_is_clipped(self) -> None:
        manager, setup, setup_id = await self._service()

        await self._update(manager, setup_id, {"products[0]": "x" * 5000})

        assert len(setup.setups[setup_id].current_setup_version.structure["products[0]"]) < 5000

    async def test_the_receipt_carries_no_map(self) -> None:
        manager, _, setup_id = await self._service()

        output = (await self._update(manager, setup_id, {"products[0]": "P1"}))["output"]

        assert "structure" not in output
        assert output["setup_id"] == setup_id

    async def test_a_tool_setup_is_refused(self) -> None:
        env = await self._update(ServicesManager(*_stores()), "setups:duda", {"secret": "x"})

        assert env["metadata"]["success"] is False
        assert "not a service setup" in env["error"]

    async def test_entries_are_required(self) -> None:
        manager, _, setup_id = await self._service()

        assert (await self._update(manager, setup_id, {}))["metadata"]["success"] is False

    def test_update_structure_reaches_the_model(self) -> None:
        function = ServicesManager(*_stores()).async_functions["services_manager"]

        assert "UpdateStructureServiceAction" in json.loads(json.dumps(function.parameters))["$defs"]
        assert "'update_structure'" in function.description

    async def test_a_patch_refusal_points_at_update_structure(self) -> None:
        manager, _, setup_id = await self._service()

        env = _env(
            await manager.services_manager(
                action={  # type: ignore[arg-type]
                    "action": "patch",
                    "setup_id": setup_id,
                    "path": "limits",
                    "value": {"max.tokens": 3},
                    "structure": {"products[0]": "P1"},
                }
            )
        )

        assert "use update_structure" in env["error"]


class TestDisplayLabels:
    """Managers label themselves and resolve the called action's label per call."""

    @pytest.mark.parametrize(
        "args",
        [
            {"action": {"action": "create", "name": "x"}},
            {"action": '{"action": "create", "name": "x"}'},
            {"action": "create", "name": "x"},
        ],
    )
    def test_manager_labels(self, args: dict[str, Any]) -> None:
        assert ServicesManager(*_stores()).display_labels("services_manager", args) == ("Services Manager", "Create")

    @pytest.mark.parametrize(
        "args",
        [None, {}, {"action": "not json"}, {"action": "{broken"}, {"action": {"action": "nope!"}}, {"action": 3}],
    )
    def test_manager_labels_fallback(self, args: dict[str, Any] | None) -> None:
        assert ServicesManager(*_stores()).display_labels("services_manager", args) == ("Services Manager", None)

    def test_unknown_identifier_action_is_humanized(self) -> None:
        labels = ServicesManager(*_stores()).display_labels("services_manager", {"action": {"action": "do_thing"}})
        assert labels == ("Services Manager", "Do thing")

    def test_each_manager_has_its_label(self) -> None:
        setup, reg = _stores()
        args = {"action": {"action": "change_visibility"}}
        assert ToolsManager(setup, reg).display_labels("tools_manager", args) == ("Tools Manager", "Change visibility")
        assert KinsManager(setup, reg).display_labels("kins_manager", args) == ("Kins Manager", "Change visibility")

    def test_inherited_action_keeps_its_label(self) -> None:
        args = {"action": {"action": "update"}}
        assert ServicesManager(*_stores()).display_labels("services_manager", args) == ("Services Manager", "Update")

    def test_manager_llm_schema_has_no_display_name(self) -> None:
        fn = ServicesManager(*_stores()).async_functions["services_manager"]
        dumped = json.dumps(fn.to_dict(), ensure_ascii=False)
        assert "display_name" not in dumped

    def test_every_action_declares_a_label(self) -> None:
        for cls in (
            GetAction,
            SearchAction,
            UpdateAction,
            DeleteAction,
            ChangeVisibilityAction,
            ListVersionsAction,
            SetVersionAction,
            CreateServiceAction,
            StructureServiceAction,
            LoadServiceAction,
            UpdateServiceAction,
            PatchServiceAction,
            AppendServiceAction,
            RemoveServiceAction,
            GetServiceAction,
            GenerateServiceAction,
        ):
            assert cls.display_name
            assert "_" not in cls.display_name
