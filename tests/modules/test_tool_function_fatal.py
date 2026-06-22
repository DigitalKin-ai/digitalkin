"""BUG 2 regression: a tool's fatal stream.error aborts the tool call.

A fatal ``stream.error`` (e.g. SETUP_ACCESS_DENIED) yielded by ``call_module`` must be
raised as ``ToolCallError`` from the tool function, not surfaced as a benign result dict —
otherwise the parent run never reaches a terminal state and its dial-back BiDi hangs.
"""

from typing import Any

import pytest
from google.protobuf import struct_pb2

from digitalkin.models.module.module_context import ModuleContext, Session
from digitalkin.models.module.tool_cache import ToolDefinition, ToolModuleInfo
from digitalkin.models.services.registry import RegistryModuleType
from digitalkin.services.communication.exceptions import ToolCallError


def _frame(root: dict[str, Any]) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    s.update({"root": root})
    return s


class _FakeComm:
    """Communication stub whose ``call_module`` replays preset Struct frames."""

    def __init__(self, frames: list[struct_pb2.Struct]) -> None:
        self._frames = frames

    async def call_module(self, **_kwargs: Any) -> Any:
        for frame in self._frames:
            yield frame


def _tool_function(frames: list[struct_pb2.Struct]) -> Any:
    tmi = ToolModuleInfo(
        module_id="tool-1",
        module_type=RegistryModuleType.TOOL_MODULE,
        address="localhost",
        port=50051,
        version="1.0.0",
        module_name="SearchTool",
        setup_id="setup-1",
        tools=[ToolDefinition(name="search", description="Search")],
    )
    session = Session(job_id="jobs:1", mission_id="missions:1", setup_id="setup-1", setup_version_id="v1")
    return ModuleContext._create_single_tool_function(
        _FakeComm(frames),  # type: ignore[arg-type]
        session,
        tmi,
        tmi.tools[0],
    )


@pytest.mark.asyncio
async def test_fatal_stream_error_raises_tool_call_error() -> None:
    fn = _tool_function([
        _frame({"protocol": "message", "content": "partial"}),
        _frame({"protocol": "stream.error", "code": "SETUP_ACCESS_DENIED", "message": "denied", "fatal": True}),
    ])
    seen: list[dict] = []

    async def _drain() -> None:
        async for out in fn():
            seen.append(out)  # noqa: PERF401  # frames before the fatal must survive the raise

    with pytest.raises(ToolCallError, match=r"\[SETUP_ACCESS_DENIED\].*denied") as exc:
        await _drain()
    # The non-fatal frame before it is still delivered; the fatal one aborts.
    assert seen == [{"root": {"protocol": "message", "content": "partial"}}]
    assert "[SETUP_ACCESS_DENIED]" in str(exc.value)


@pytest.mark.asyncio
async def test_non_fatal_stream_error_is_yielded() -> None:
    fn = _tool_function([
        _frame({"protocol": "stream.error", "code": "TRANSIENT", "message": "retrying", "fatal": False}),
        _frame({"protocol": "message", "content": "done"}),
    ])
    seen = [out async for out in fn()]
    assert seen == [
        {"root": {"protocol": "stream.error", "code": "TRANSIENT", "message": "retrying", "fatal": False}},
        {"root": {"protocol": "message", "content": "done"}},
    ]


@pytest.mark.asyncio
async def test_clean_run_yields_all_frames() -> None:
    fn = _tool_function([
        _frame({"protocol": "message", "content": "a"}),
        _frame({"protocol": "message", "content": "b"}),
    ])
    seen = [out async for out in fn()]
    assert seen == [
        {"root": {"protocol": "message", "content": "a"}},
        {"root": {"protocol": "message", "content": "b"}},
    ]
