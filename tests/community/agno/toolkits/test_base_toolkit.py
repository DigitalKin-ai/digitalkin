"""Tests for DkToolkit — canonical envelope + best-effort AG-UI notifications."""

import json
from types import SimpleNamespace
from typing import Any

from digitalkin.community.agno.toolkits import DkToolkit


def test_ok_envelope() -> None:
    assert json.loads(DkToolkit._ok({"a": 1}, tool="t")) == {
        "output": {"a": 1},
        "metadata": {"success": True, "tool": "t"},
    }


def test_fail_envelope() -> None:
    assert json.loads(DkToolkit._fail("boom", tool="t")) == {
        "error": "boom",
        "metadata": {"success": False, "tool": "t"},
    }


class _Kit(DkToolkit):
    def __init__(self, context: Any = None) -> None:
        super().__init__(name="k", tools=[], context=context)


async def test_notify_emits_agui_custom_event() -> None:
    sent: list[Any] = []

    async def _send(message: Any) -> None:
        sent.append(message)

    ctx = SimpleNamespace(callbacks=SimpleNamespace(send_message=_send))
    await _Kit(ctx)._notify("live_view", {"url": "https://x"})

    assert len(sent) == 1
    dumped = sent[0].model_dump(mode="json")  # the callback contract (module_runner does this)
    assert dumped["root"]["protocol"] == "agui_custom"


async def test_notify_noop_without_context() -> None:
    await _Kit(None)._notify("x", 1)  # no context -> silent no-op


async def test_notify_noop_without_callback() -> None:
    ctx = SimpleNamespace(callbacks=SimpleNamespace())  # send_message not installed
    await _Kit(ctx)._notify("x", 1)  # silent no-op


async def test_notify_swallows_send_failure() -> None:
    async def _boom(_message: Any) -> None:
        raise RuntimeError("stream down")

    ctx = SimpleNamespace(callbacks=SimpleNamespace(send_message=_boom))
    await _Kit(ctx)._notify("x", 1)  # best-effort: swallowed, never raises
