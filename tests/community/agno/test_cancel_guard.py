"""After a task is cancelled, no agent may keep calling tools."""

import asyncio
from typing import Any

import pytest
from agno.exceptions import RunCancelledException

from digitalkin.community.agno.cancel_guard import CancelGuard


async def _next_link(**kwargs: Any) -> str:
    await asyncio.sleep(0)
    return f"ok:{kwargs['x']}"


def _sync_link(**kwargs: Any) -> str:
    return f"sync:{kwargs['x']}"


async def test_passes_through_while_running() -> None:
    assert await CancelGuard(asyncio.Event(), owner="search").tool_hook("write_json", _next_link, {"x": 1}) == "ok:1"


async def test_supports_a_sync_next_link() -> None:
    assert await CancelGuard(asyncio.Event(), owner="search").tool_hook("write_json", _sync_link, {"x": 2}) == "sync:2"


async def test_raises_once_cancelled_without_calling_the_tool() -> None:
    event = asyncio.Event()
    event.set()
    calls: list[Any] = []

    def link(**kwargs: Any) -> None:
        calls.append(kwargs)

    with pytest.raises(RunCancelledException, match="write_json"):
        await CancelGuard(event, owner="search").tool_hook("write_json", link, {"x": 3})
    assert calls == []
