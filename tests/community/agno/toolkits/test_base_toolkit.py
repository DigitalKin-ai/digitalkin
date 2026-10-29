"""Tests for DkToolkit — canonical envelope + best-effort AG-UI notifications."""

import json
from types import SimpleNamespace
from typing import Annotated, Any, ClassVar, Literal
from unittest.mock import AsyncMock

import pytest
from agno.tools.function import Function
from pydantic import BaseModel, Field

from digitalkin.community.agno.toolkits import DkToolkit
from digitalkin.models.events import ToolInfo


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

    async def _send(message: Any) -> None:  # ruff: ignore[unused-async]
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
    async def _boom(_message: Any) -> None:  # ruff: ignore[unused-async]
        msg = "stream down"
        raise RuntimeError(msg)

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

    async def _send(message: Any) -> None:  # ruff: ignore[unused-async]
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


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("rag_search", "Rag search"),
        ("rag__search", "Search"),
        ("ask_form", "Ask form"),
        ("Search", "Search"),
        ("rag__", "Rag"),
        ("__private__", "Private"),
        ("", ""),
    ],
)
def test_humanize(raw: str, expected: str) -> None:
    assert ToolInfo.humanize(raw) == expected


class _LabelledKit(DkToolkit):
    def __init__(self, name: str, display_name: str | None = None) -> None:
        async def _noop() -> str:  # ruff: ignore[unused-async]
            return ""

        super().__init__(
            name=name,
            tools=[
                Function(name="read", title="Read message", entrypoint=_noop, skip_entrypoint_processing=True),
                Function(name="outline", entrypoint=_noop, skip_entrypoint_processing=True),
            ],
            display_name=display_name,
        )


def test_display_labels_uses_function_title() -> None:
    assert _LabelledKit(name="x", display_name="History").display_labels("read", None) == ("History", "Read message")


def test_display_labels_without_title_is_humanized() -> None:
    assert _LabelledKit(name="x", display_name="History").display_labels("outline", None) == ("History", "Outline")


class _SingleKit(DkToolkit):
    def __init__(self) -> None:
        async def _noop() -> str:  # ruff: ignore[unused-async]
            return ""

        super().__init__(
            name="single", tools=[Function(name="only", entrypoint=_noop, skip_entrypoint_processing=True)]
        )


def test_single_function_without_title_has_no_function_label() -> None:
    assert _SingleKit().display_labels("only", None) == ("Single", None)


def test_display_labels_unknown_function() -> None:
    assert _LabelledKit(name="x", display_name="History").display_labels("nope", {}) == ("History", None)


def test_display_name_defaults_to_humanized_name() -> None:
    assert _LabelledKit(name="chat_history_tools").display_name == "Chat history tools"


class _CreateAction(BaseModel):
    action: Literal["create"] = "create"


class _DeleteAction(BaseModel):
    action: Literal["delete"] = "delete"


@pytest.mark.parametrize(
    ("actions", "members"),
    [
        (_CreateAction, (_CreateAction,)),
        (Annotated[_CreateAction, Field(discriminator="action")], (_CreateAction,)),
        (Annotated[_CreateAction | _DeleteAction, Field(discriminator="action")], (_CreateAction, _DeleteAction)),
    ],
)
def test_action_members_unwraps_the_discriminated_union(actions: Any, members: tuple[type, ...]) -> None:
    assert DkToolkit.action_members(actions) == members


class _LabelledCreate(BaseModel):
    display_name: ClassVar[str] = "Create a setup"
    action: Literal["create"] = "create"


@pytest.mark.parametrize(
    ("tool_args", "expected"),
    [
        (None, None),
        ('{"action": "create"}', None),
        ({}, None),
        ({"action": "create"}, "Create a setup"),
        ({"action": '{"action": "create"}'}, "Create a setup"),
        ({"action": {"action": "create"}}, "Create a setup"),
        ({"action": "list_versions"}, "List versions"),
        ({"action": '{"action": '}, None),
        ({"action": "{not json"}, None),
        ({"action": "drop table;"}, None),
        ({"action": 3}, None),
        ({"action": {"action": None}}, None),
    ],
)
def test_action_label_edge_cases(tool_args: Any, expected: str | None) -> None:
    assert DkToolkit.action_label(tool_args, _LabelledCreate) == expected
