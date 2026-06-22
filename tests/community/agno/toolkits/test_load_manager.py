"""Tests for the load surface reachable without the real agno dependency.

The success path of ``LoadToolAction.execute`` (which builds a ModuleToolkit, needing real agno)
and the external-execution marking (needing a real agno Function) live in
``tests/community/agno/test_dynamic_tool_loading.py``.
"""

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from digitalkin.community.agno.toolkits import LoadManager
from digitalkin.community.agno.toolkits.registry.loader.action import LoadActionCtx, LoadToolAction
from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.models.services.registry import RegistryModuleType
from digitalkin.services.registry.exceptions import RegistryServiceError


def _ctx(
    resolve: Any = None,
    base_tools: list[Any] | None = None,
    *,
    setup: Any = ...,
    module_type: RegistryModuleType = RegistryModuleType.TOOL_MODULE,
) -> LoadActionCtx:
    """A load context whose registry resolves a TOOL_MODULE setup by default.

    Pass ``setup=None`` for an absent id, or ``module_type=`` a non-tool family for the kind gate.
    ``resolve`` is the ``resolve_tool`` mock reached only once the kind gate passes.
    """
    resolved_setup = SimpleNamespace(module_id="m1") if setup is ... else setup
    registry = SimpleNamespace(
        get_setup=AsyncMock(return_value=resolved_setup),
        discover_by_id=AsyncMock(return_value=SimpleNamespace(module_type=module_type)),
    )
    return LoadActionCtx(
        context=SimpleNamespace(registry=registry, resolve_tool=resolve),  # type: ignore[arg-type]
        base_tools=[] if base_tools is None else base_tools,
        notify=AsyncMock(),
    )


def _loader(base_tools: list[Any] | None = None, *, setup: Any = None) -> LoadManager:
    """A LoadManager with a stub context and an optionally bound tool list.

    ``setup`` (default ``None``) is what the registry resolves, so ``run_paused`` reaches a clean
    "no setup with that id exists" failure without needing real agno.
    """
    registry = SimpleNamespace(
        get_setup=AsyncMock(return_value=setup),
        discover_by_id=AsyncMock(return_value=SimpleNamespace(module_type=RegistryModuleType.TOOL_MODULE)),
    )
    context = SimpleNamespace(registry=registry, resolve_tool=AsyncMock(return_value=None), callbacks=SimpleNamespace())
    loader = LoadManager(context=context)  # type: ignore[arg-type]
    if base_tools is not None:
        loader.bind_tools(base_tools)
    return loader


# --- the action carries the load logic (execute), returning a structured LoadOutcome ------


@pytest.mark.asyncio
async def test_execute_permission_denied() -> None:
    outcome = await LoadToolAction(setup_id="s1").execute(_ctx(resolve=AsyncMock(side_effect=PermissionDeniedError("no"))))
    assert outcome.ok is False
    assert outcome.message == "permission denied: cannot load setup s1"


@pytest.mark.asyncio
async def test_execute_unknown_setup_not_found() -> None:
    outcome = await LoadToolAction(setup_id="s1").execute(_ctx(setup=None))
    assert outcome.ok is False
    assert outcome.message == "could not load setup s1: no setup with that id exists"


@pytest.mark.asyncio
async def test_execute_non_tool_family_is_refused() -> None:
    # A service setup gets a discriminating message, not the generic "resolution failed".
    outcome = await LoadToolAction(setup_id="s1").execute(_ctx(module_type=RegistryModuleType.SERVICE))
    assert outcome.ok is False
    assert "not a tool" in outcome.message
    assert "service" in outcome.message


@pytest.mark.asyncio
async def test_execute_registry_not_found_is_handled() -> None:
    # registry.get_setup RAISES (not returns None) on an unknown id; the pre-check must catch it,
    # else the RegistryServiceError escapes and crashes the whole module through the HITL runner.
    ctx = _ctx()
    ctx.context.registry.get_setup = AsyncMock(side_effect=RegistryServiceError("[NOT_FOUND] Resource not found"))
    outcome = await LoadToolAction(setup_id="setups:ghost").execute(ctx)
    assert outcome.ok is False
    assert outcome.message == "could not load setup setups:ghost: no setup with that id exists"


@pytest.mark.asyncio
async def test_execute_resolution_error_is_swallowed() -> None:
    outcome = await LoadToolAction(setup_id="s1").execute(_ctx(resolve=AsyncMock(side_effect=RuntimeError("boom"))))
    assert outcome.ok is False
    assert outcome.message == "could not load setup s1: resolution failed"


@pytest.mark.asyncio
async def test_execute_empty_setup_id_is_distinct() -> None:
    outcome = await LoadToolAction(setup_id="").execute(_ctx())
    assert outcome.ok is False
    assert "no setup id" in outcome.message


# --- the manager: stub tool, and the runner entry point that envelopes the outcome --------


def test_tool_name_is_load_manager() -> None:
    assert _loader().tool_name == "load_manager"


@pytest.mark.asyncio
async def test_load_manager_returns_pending_envelope() -> None:
    # The exposed tool is a stub (external-execution); it just returns "pending".
    env = json.loads(await _loader().load_manager(LoadToolAction(setup_id="s1")))
    assert env["metadata"]["success"] is True
    assert env["output"] == {"status": "pending"}


@pytest.mark.asyncio
async def test_run_paused_envelopes_the_outcome() -> None:
    # The runner-facing entry unwraps the action, runs execute, and envelopes the outcome.
    env = json.loads(await _loader(base_tools=[]).run_paused({"action": {"action": "tool", "setup_id": "s1"}}))
    assert env["metadata"]["success"] is False
    assert env["metadata"]["tool"] == "tool"
    assert env["error"] == "could not load setup s1: no setup with that id exists"


@pytest.mark.asyncio
async def test_run_paused_rejects_an_invalid_action() -> None:
    env = json.loads(await _loader(base_tools=[]).run_paused({"action": {"action": "nope"}}))
    assert env["metadata"]["success"] is False
    assert "invalid" in env["error"]


@pytest.mark.asyncio
async def test_run_paused_guards_an_unexpected_error() -> None:
    # A backend surprise inside execute must become a fail envelope, never crash the runner.
    loader = _loader(base_tools=[])
    loader._ctx.registry.get_setup = AsyncMock(side_effect=RuntimeError("boom"))
    env = json.loads(await loader.run_paused({"action": {"action": "tool", "setup_id": "s1"}}))
    assert env["metadata"]["success"] is False
    assert "could not load" in env["error"]


@pytest.mark.asyncio
async def test_run_paused_without_binding_is_unavailable() -> None:
    # No base_tools bound → cannot append, so loading is unavailable.
    env = json.loads(await _loader().run_paused({"action": {"action": "tool", "setup_id": "s1"}}))
    assert env["metadata"]["success"] is False
    assert "unavailable" in env["error"]


def test_bind_tools_stores_the_live_list() -> None:
    tools: list[Any] = []
    loader = _loader()
    loader.bind_tools(tools)
    assert loader._base_tools is tools
