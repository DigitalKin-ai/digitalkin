"""Tests for the Registry Toolkit managers — Tools / Services / Kins via ``manage_*`` dispatch."""

import datetime
import json
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

from digitalkin.community.agno.toolkits import KinsManager, ServicesManager, ToolsManager
from digitalkin.community.agno.toolkits.registry.action import (
    DeleteAction,
    ChangeVisibilityAction,
    GetAction,
    ListVersionsAction,
    SearchAction,
    SetVersionAction,
    UpdateAction,
)
from digitalkin.community.agno.toolkits.registry.services.action import CreateServiceAction, LoadServiceAction
from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.services.registry.exceptions import RegistryServiceError
from digitalkin.models.services.registry import (
    ModuleInfo,
    RegistryModuleType,
    RegistrySetupStatus,
    RegistrySortBy,
    RegistryVisibility,
    SetupInfo,
)
from digitalkin.models.services.storage import Visibility
from digitalkin.services.registry import DefaultRegistry
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
    "setups:duda": "Builds websites. " + "x" * 400,
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
    now = datetime.datetime.now(datetime.timezone.utc)
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

    async def test_search_truncates_description(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager(SearchAction(query="duda")))
        assert len(env["output"]["setups"][0]["description"]) == 300


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


class TestGetAndLoad:
    async def test_get_returns_the_matching_object_type(self) -> None:
        env = _env(await ServicesManager(*_stores()).services_manager(GetAction(setup_id="setups:nikita")))
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
                CreateServiceAction(name="Nikita", content={"branding": True})
            )
        )
        assert env["metadata"]["tool"] == "create"
        assert env["output"]["name"] == "Nikita"
        assert env["output"]["current_setup_version"]["content"] == {"branding": True}

    async def test_create_then_load_round_trips(self) -> None:
        svc = ServicesManager(*_stores())
        created = _env(await svc.services_manager(CreateServiceAction(name="Nikita", content={"branding": True})))
        env = _env(await svc.services_manager(LoadServiceAction(setup_id=created["output"]["id"])))
        assert env["output"] == {"branding": True}


class TestCrudRoundTrip:
    """update / change_visibility / delete route through the setup service on the same type."""

    async def test_update_visibility_delete(self) -> None:
        svc = ServicesManager(*_stores())
        created = _env(await svc.services_manager(CreateServiceAction(name="X", content={"a": 1})))
        setup_id = created["output"]["id"]

        updated = _env(await svc.services_manager(UpdateAction(setup_id=setup_id, name="renamed", content={"a": 2})))
        assert updated["metadata"]["tool"] == "update"
        assert updated["output"]["name"] == "renamed"

        shared = _env(await svc.services_manager(ChangeVisibilityAction(setup_id=setup_id, visibility="internal")))
        assert shared["output"]["visibility"] == "internal"

        deleted = _env(await svc.services_manager(DeleteAction(setup_id=setup_id)))
        assert deleted["output"] is True


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
        return SimpleNamespace(
            get_module_config_schema=AsyncMock(return_value=schema), callbacks=SimpleNamespace()
        )

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
        env = _env(
            await mgr.kins_manager(UpdateAction(setup_id="setups:isaac", name="x", content={"rules": [12345]}))
        )
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
            await KinsManager(*_stores()).kins_manager(
                {"action": "update", "setup_id": "setups:isaac", "name": "Bad\x00name", "content": {"x": 1}}
            )
        )
        assert env["metadata"]["success"] is False
        assert "name" in env["error"]

    async def test_service_create_name_with_ansi_escape_is_refused(self) -> None:
        env = _env(
            await ServicesManager(*_stores()).services_manager(
                {"action": "create", "name": "svc\x1b[31m", "content": {"a": 1}}
            )
        )
        assert env["metadata"]["success"] is False
        assert "name" in env["error"]

    async def test_update_clean_name_still_passes(self) -> None:
        env = _env(
            await KinsManager(*_stores()).kins_manager(
                {"action": "update", "setup_id": "setups:isaac", "name": "Clean Name", "content": {"x": 1}}
            )
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
            await svc.services_manager(UpdateAction(setup_id="setups:nikita", name="zombie", content={"k": "v"}))
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

        async def _cv(_payload: dict[str, Any]) -> SetupData:
            return stale

        async def _get(_payload: dict[str, Any]) -> SetupData:
            return fresh

        setup.change_visibility = _cv  # type: ignore[method-assign]
        setup.get_setup = _get  # type: ignore[method-assign]

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
        await svc.services_manager(CreateServiceAction(name="X", content={"a": 1}))
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
            CreateServiceAction(name="x", content={})

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

        await manager.kins_manager(
            SetVersionAction(setup_id=setup_id, setup_version_id=previous["setup_version_id"])
        )
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
                raise RegistryServiceError("registry unreachable")

        env = _env(
            await ServicesManager(setup, _Unreachable("", "", "")).services_manager(
                DeleteAction(setup_id=setup_id)
            )
        )
        assert env["metadata"]["success"] is False
        assert setup_id in setup.setups

    async def test_a_resolvable_setup_of_another_kind_is_still_refused(self) -> None:
        env = _env(await ToolsManager(*_stores()).tools_manager(DeleteAction(setup_id="setups:isaac")))
        assert env["metadata"]["success"] is False
        assert "kind mismatch" in json.dumps(env)
