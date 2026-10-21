"""Tests for DkToolkit — canonical envelope + best-effort AG-UI notifications."""

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

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


async def test_cite_merges_into_the_open_run() -> None:
    """Inside an agent's AG-UI run the source joins that run's single ``sources`` activity."""
    from digitalkin.mixins.agui_mixin import AgUiRunState

    send = AsyncMock()

    run = AgUiRunState()
    ctx = SimpleNamespace(
        callbacks=SimpleNamespace(send_message=send), agui_run=run, session=SimpleNamespace(current_ids=dict)
    )
    await _Kit(ctx)._cite("https://docs.digitalkin.ai/pricing", description="Pro is 49 €")

    event = send.await_args.args[0].model_dump(mode="json")["root"]["event"]
    assert event["messageId"] == f"sources-{run.sources_id}"
    assert event["content"] == {
        "sources": [{"url": "https://docs.digitalkin.ai/pricing", "description": "Pro is 49 €"}]
    }


async def test_cite_without_run_emits_one_source_snapshot_for_the_relay() -> None:
    """A tool module has no AG-UI run: it emits a one-source snapshot the calling agent merges."""
    send = AsyncMock()

    ctx = SimpleNamespace(callbacks=SimpleNamespace(send_message=send), agui_run=None)
    await _Kit(ctx)._cite("https://docs.digitalkin.ai/pricing", title="Pricing")

    root = send.await_args.args[0].model_dump(mode="json")["root"]
    assert root["protocol"] == "agui_activity_snapshot"
    assert root["event"]["activityType"] == "sources"
    assert root["event"]["replace"] is True
    assert root["event"]["content"] == {"sources": [{"url": "https://docs.digitalkin.ai/pricing", "title": "Pricing"}]}


async def test_cite_drops_invalid_url() -> None:
    sent: list[Any] = []

    async def _send(message: Any) -> None:
        sent.append(message)

    ctx = SimpleNamespace(callbacks=SimpleNamespace(send_message=_send))
    await _Kit(ctx)._cite("javascript:alert(1)", title="bad")

    assert sent == []


async def test_cite_noop_without_context() -> None:
    await _Kit(None)._cite("https://x.io")


async def test_cite_is_best_effort_when_the_run_fails() -> None:
    """A failing sources write never propagates into the tool call."""
    run = SimpleNamespace(cite=AsyncMock(side_effect=RuntimeError("stream closed")))
    ctx = SimpleNamespace(callbacks=SimpleNamespace(), agui_run=run)
    await _Kit(ctx)._cite("https://a.io/")
    run.cite.assert_awaited_once()
