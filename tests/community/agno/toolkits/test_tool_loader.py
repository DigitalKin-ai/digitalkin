"""Tests for ToolLoaderTools logic reachable without the real agno dependency.

The success path of ``load`` (which builds a ModuleToolkit, needing real agno) and the
external-execution marking (needing a real agno Function) live in
``tests/community/agno/test_dynamic_tool_loading.py``.
"""

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from digitalkin.community.agno.toolkits import ToolLoaderTools
from digitalkin.grpc_servers.exceptions import PermissionDeniedError


def _loader(resolve: Any = None, base_tools: list[Any] | None = None) -> ToolLoaderTools:
    """A ToolLoaderTools with a stub context and a bound (possibly empty) tool list."""
    context = SimpleNamespace(resolve_tool=resolve, callbacks=SimpleNamespace())
    loader = ToolLoaderTools(context=context)  # type: ignore[arg-type]
    if base_tools is not None:
        loader.bind_tools(base_tools)
    return loader


def test_tool_name_is_use_setup() -> None:
    assert _loader().tool_name == "use_setup"


@pytest.mark.asyncio
async def test_use_setup_returns_pending_envelope() -> None:
    env = json.loads(await _loader().use_setup("s1"))
    assert env["metadata"]["success"] is True
    assert env["output"] == {"setup_id": "s1", "status": "pending"}


def test_bind_tools_stores_the_live_list() -> None:
    tools: list[Any] = []
    loader = _loader()
    loader.bind_tools(tools)
    assert loader._base_tools is tools


def test_find_locates_loader_in_list() -> None:
    loader = _loader()
    assert ToolLoaderTools.find(["x", loader, "y"]) is loader


def test_find_via_factory_callable() -> None:
    loader = _loader()
    assert ToolLoaderTools.find(lambda _ctx=None: [loader]) is loader


def test_find_absent_returns_none() -> None:
    assert ToolLoaderTools.find(["x", "y"]) is None


@pytest.mark.asyncio
async def test_load_without_binding_is_unavailable() -> None:
    # No base_tools bound → cannot append, so loading is unavailable.
    loader = ToolLoaderTools(context=SimpleNamespace(resolve_tool=AsyncMock()))  # type: ignore[arg-type]
    assert "unavailable" in await loader.load("s1")


@pytest.mark.asyncio
async def test_load_permission_denied() -> None:
    loader = _loader(resolve=AsyncMock(side_effect=PermissionDeniedError("no")), base_tools=[])
    assert await loader.load("s1") == "permission denied: cannot load setup s1"


@pytest.mark.asyncio
async def test_load_unknown_setup_not_found() -> None:
    loader = _loader(resolve=AsyncMock(return_value=None), base_tools=[])
    assert await loader.load("s1") == "could not load setup s1: not found"


@pytest.mark.asyncio
async def test_load_resolution_error_is_swallowed() -> None:
    loader = _loader(resolve=AsyncMock(side_effect=RuntimeError("boom")), base_tools=[])
    assert await loader.load("s1") == "could not load setup s1"
