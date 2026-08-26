"""Tests for ModuleToolkit's handling of a called tool's output and event stream.

Behaviours pinned here:

* Sentinel-protocol parsing: a successful response is the last frame whose
  ``root.protocol`` is not a lifecycle/error sentinel; ``stream.error`` frames
  surface as ``[CODE] message``.
* A tool that returns images emits OpenAI-style content parts. JSON-serialized into
  the tool message they would reach the model as a URL in text, never as an image.
  They are lifted into `ToolResult.images`, which Agno re-attaches as a user message.
* A called tool streams AG-UI events on its own gRPC job, which the frontend never
  reads. Custom events are relayed onto the agent's stream; nothing else is.
"""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("agno", reason="optional agno dependency not installed")

from agno.tools.function import ToolResult

from digitalkin.community.agno.module_toolkit import ModuleToolkit
from digitalkin.models.module.ag_ui import AgUiOutput


def _toolkit() -> ModuleToolkit:
    """Build a ModuleToolkit without running __init__ (which needs a live context)."""
    toolkit = ModuleToolkit.__new__(ModuleToolkit)
    toolkit._tool_module_info = MagicMock(module_id="mod_1", setup_id="setup_1")
    toolkit._context = MagicMock(session=MagicMock(job_id="job_1"))
    return toolkit


def _context(send_message: object | None = None) -> SimpleNamespace:
    """An agent ModuleContext stub exposing only the callbacks the relay touches."""
    callbacks = SimpleNamespace() if send_message is None else SimpleNamespace(send_message=send_message)
    return SimpleNamespace(callbacks=callbacks)


def _custom_event_message(name: str = "desktop_stream") -> dict:
    """A streamed tool message, shaped as json_format.MessageToDict produces it.

    Keys are camelCase and Struct numbers arrive as floats (1024 -> 1024.0).
    """
    return {
        "root": {
            "protocol": "agui_custom",
            "createdAt": "2026-07-10T09:00:00Z",
            "event": {
                "type": "CUSTOM",
                "name": name,
                "value": {"url": "https://6080-x.e2b.app/vnc.html?password=k", "width": 1024.0},
            },
        },
    }


def _screenshot_output(text: str = "1. Clicked at (640, 80).") -> dict:
    return {
        "root": {
            "protocol": "tool_content",
            "content": [
                {"type": "text", "text": text},
                {"type": "image_url", "image_url": {"url": "https://fs/shot.png"}},
            ],
        }
    }


class TestFindSuccessfulResponse:
    def test_returns_last_domain_frame(self):
        results = [
            {"root": {"protocol": "stream.start"}},
            {"root": {"protocol": "search", "results": [1]}},
            {"root": {"protocol": "search", "results": [2]}},
            {"root": {"protocol": "stream.end"}},
        ]
        resp = ModuleToolkit._find_successful_response(results)
        assert resp == {"root": {"protocol": "search", "results": [2]}}

    def test_sentinel_only_stream_returns_none(self):
        results = [
            {"root": {"protocol": "stream.start"}},
            {"root": {"protocol": "stream.error", "code": "X", "fatal": True}},
            {"root": {"protocol": "stream.end"}},
        ]
        assert ModuleToolkit._find_successful_response(results) is None

    def test_frames_without_root_are_skipped(self):
        results = [{"annotations": {}}, {"root": "not-a-dict"}]
        assert ModuleToolkit._find_successful_response(results) is None

    def test_empty_results_returns_none(self):
        assert ModuleToolkit._find_successful_response([]) is None


class TestExtractErrorMessage:
    def test_stream_error_surfaces_code_and_message(self):
        results = [
            {"root": {"protocol": "stream.error", "code": "SETUP_ACCESS_DENIED", "message": "denied", "fatal": True}},
        ]
        assert ModuleToolkit._extract_error_message(results) == "[SETUP_ACCESS_DENIED] denied"

    def test_domain_error_field_fallback(self):
        results = [{"root": {"protocol": "search", "error": "quota exceeded"}}]
        assert ModuleToolkit._extract_error_message(results) == "quota exceeded"

    def test_empty_results_returns_default(self):
        assert ModuleToolkit._extract_error_message([]) == "No successful response received from module"

    def test_no_error_frames_returns_default(self):
        results = [{"root": {"protocol": "stream.end"}}]
        assert ModuleToolkit._extract_error_message(results) == "No successful response received from module"


class TestExtractImages:
    def test_pulls_image_urls_out_and_keeps_text(self):
        payload, urls = ModuleToolkit._extract_images(_screenshot_output())

        assert urls == ["https://fs/shot.png"]
        assert payload["root"]["content"] == [{"type": "text", "text": "1. Clicked at (640, 80)."}]

    def test_does_not_mutate_the_original_output(self):
        output = _screenshot_output()
        ModuleToolkit._extract_images(output)
        assert len(output["root"]["content"]) == 2

    def test_multiple_images_preserve_order(self):
        output = {
            "root": {
                "protocol": "tool_content",
                "content": [
                    {"type": "image_url", "image_url": {"url": "a.png"}},
                    {"type": "image_url", "image_url": {"url": "b.png"}},
                ],
            }
        }
        _payload, urls = ModuleToolkit._extract_images(output)
        assert urls == ["a.png", "b.png"]

    def test_text_only_tool_content_is_untouched(self):
        output = {"root": {"protocol": "tool_content", "content": "just text"}}
        payload, urls = ModuleToolkit._extract_images(output)
        assert urls == []
        assert payload is output

    def test_other_protocols_are_untouched(self):
        output = {"root": {"protocol": "agui_run_finished", "event": {}}}
        payload, urls = ModuleToolkit._extract_images(output)
        assert urls == []
        assert payload is output

    def test_string_output_is_untouched(self):
        payload, urls = ModuleToolkit._extract_images("plain")
        assert (payload, urls) == ("plain", [])

    def test_malformed_image_part_is_kept_as_text_not_crashing(self):
        output = {"root": {"protocol": "tool_content", "content": [{"type": "image_url", "image_url": None}]}}
        payload, urls = ModuleToolkit._extract_images(output)
        assert urls == []
        assert payload is output


class TestHandleSuccess:
    def test_returns_tool_result_with_images_when_tool_returned_screenshots(self):
        result = _toolkit()._handle_success("computer_use", _screenshot_output(), 12.0, {})

        assert isinstance(result, ToolResult)
        assert [image.url for image in result.images] == ["https://fs/shot.png"]

    def test_image_url_is_not_duplicated_into_the_text_body(self):
        result = _toolkit()._handle_success("computer_use", _screenshot_output(), 12.0, {})

        assert isinstance(result, ToolResult)
        assert "https://fs/shot.png" not in result.content
        # The textual part of the tool output still reaches the model.
        assert "Clicked at (640, 80)." in result.content

    def test_body_stays_valid_json_with_output_and_metadata(self):
        result = _toolkit()._handle_success("computer_use", _screenshot_output(), 12.0, {})

        assert isinstance(result, ToolResult)
        body = json.loads(result.content)
        assert body["output"]["root"]["protocol"] == "tool_content"
        assert body["metadata"]["success"] is True

    def test_returns_a_plain_string_when_there_is_no_image(self):
        output = {"root": {"protocol": "tool_content", "content": "no image here"}}
        result = _toolkit()._handle_success("some_tool", output, 5.0, {})

        assert isinstance(result, str)
        assert json.loads(result)["output"] == output

    def test_the_signed_url_never_reaches_the_model_as_text(self):
        """A presigned S3 URL expires; the model must not quote it back to the user."""
        signed = "https://bucket.s3.amazonaws.com/shot.png?X-Amz-Signature=deadbeef"
        output = {
            "root": {
                "protocol": "tool_content",
                "content": [
                    {"type": "text", "text": "done"},
                    {"type": "image_url", "image_url": {"url": signed}},
                ],
            }
        }
        result = _toolkit()._handle_success("computer_use", output, 1.0, {})

        assert isinstance(result, ToolResult)
        assert "X-Amz-Signature" not in result.content
        # …but it does reach the provider through the vision channel.
        assert result.images[0].url == signed


class TestRelayCustomEvent:
    def test_relays_a_custom_event_onto_the_agent_stream(self):
        sent: list[object] = []

        async def send(message: object) -> None:
            sent.append(message)

        asyncio.run(ModuleToolkit._relay_custom_event(_context(send), _custom_event_message()))

        assert len(sent) == 1
        relayed = sent[0]
        assert isinstance(relayed, AgUiOutput)
        assert relayed.root.protocol == "agui_custom"
        assert relayed.root.event.name == "desktop_stream"
        assert relayed.root.event.value["url"].startswith("https://6080-x.e2b.app")

    def test_does_not_relay_the_tools_run_lifecycle_events(self):
        """Relaying them would nest a second run inside the agent's own."""
        sent: list[object] = []

        async def send(message: object) -> None:
            sent.append(message)

        context = _context(send)
        for protocol in ("agui_run_started", "agui_text_message_content", "agui_run_finished", "tool_content"):
            asyncio.run(ModuleToolkit._relay_custom_event(context, {"root": {"protocol": protocol}}))

        assert sent == []

    def test_ignores_a_custom_event_without_a_name(self):
        send = AsyncMock()
        message = {"root": {"protocol": "agui_custom", "event": {"value": {"a": 1}}}}

        asyncio.run(ModuleToolkit._relay_custom_event(_context(send), message))

        send.assert_not_awaited()

    def test_a_failing_callback_is_swallowed(self):
        """A broken agent stream must never fail the tool call."""
        send = AsyncMock(side_effect=RuntimeError("stream closed"))

        asyncio.run(ModuleToolkit._relay_custom_event(_context(send), _custom_event_message()))

        send.assert_awaited_once()

    def test_missing_callback_is_a_no_op(self):
        """The toolkit is constructible outside a running job."""
        asyncio.run(ModuleToolkit._relay_custom_event(_context(), _custom_event_message()))

    def test_malformed_message_is_a_no_op(self):
        send = AsyncMock()

        for message in ({}, {"annotations": {}}, {"root": "not-a-dict"}):
            asyncio.run(ModuleToolkit._relay_custom_event(_context(send), message))

        send.assert_not_awaited()
