"""AG-UI 1.0 run input handling in the HITL runner: typed content parts, nothing repr'd, nothing fetched."""

import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("agno", reason="optional agno dependency not installed")

from ag_ui.core import (
    ActivityMessage,
    AssistantMessage,
    DataSource,
    DocumentPart,
    FileSource,
    ImagePart,
    ReasoningMessage,
    RunAgentInput,
    TextPart,
    ToolMessage,
    UrlSource,
    UserMessage,
)
from agno.models.message import Message as AgnoMessage

from digitalkin.community.agno.hitl import AgnoHitlRunner, HitlEvents, PausedRunRecord

pytestmark = pytest.mark.unit

PNG = base64.b64encode(b"\x89PNG-bytes").decode()


def _input(*messages: object) -> RunAgentInput:
    return RunAgentInput(thread_id="t1", run_id="r1", messages=list(messages), tools=[])


def _runner(record: PausedRunRecord | None = None) -> AgnoHitlRunner:
    return AgnoHitlRunner(agent=SimpleNamespace(), store=SimpleNamespace(load=AsyncMock(return_value=record)))


class TestContentText:
    def test_plain_string_is_unchanged(self) -> None:
        assert AgnoHitlRunner._content_text("m1", "hello", None) == "hello"

    def test_parts_keep_text_and_inline_image_and_log_the_rest(self, monkeypatch: pytest.MonkeyPatch) -> None:
        log = MagicMock()
        monkeypatch.setattr("digitalkin.community.agno.hitl.logger", log)
        images: list = []
        content = [
            TextPart(text="look at"),
            ImagePart(id="img-data", source=DataSource(value=PNG, mime_type="image/png")),
            ImagePart(id="img-url", source=UrlSource(value="http://169.254.169.254/latest/meta-data")),
            DocumentPart(id="doc-file", source=FileSource(value="file-abc123", provider="openai")),
            TextPart(text="this"),
        ]

        text = AgnoHitlRunner._content_text("m1", content, images)

        assert text == "look at this"
        assert [(i.content, i.mime_type, i.url) for i in images] == [(b"\x89PNG-bytes", "image/png", None)]
        dropped = [c.args for c in log.info.call_args_list if c.args[0].startswith("[VALIDATE AGUI-PART-DROPPED]")]
        assert [(a[2], a[3], a[4]) for a in dropped] == [("image", "img-url", "url"), ("document", "doc-file", "file")]
        # A file handle or URL is never logged in full nor handed to the model.
        assert all("file-abc123" not in repr(a) and "169.254" not in repr(a) for a in dropped)

    def test_oversized_inline_image_is_dropped_before_decoding(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_AGUI_MAX_INLINE_B64_CHARS", "4")
        monkeypatch.setattr("digitalkin.community.agno.hitl.base64.b64decode", MagicMock(side_effect=AssertionError))
        images: list = []

        AgnoHitlRunner._content_text("m1", [ImagePart(source=DataSource(value=PNG, mime_type="image/png"))], images)

        assert images == []

    def test_invalid_base64_is_dropped(self) -> None:
        images: list = []
        part = ImagePart(source=DataSource(value="not*base64", mime_type="image/png"))

        assert not AgnoHitlRunner._content_text("m1", [part], images)
        assert images == []

    def test_images_none_drops_inline_images(self) -> None:
        part = ImagePart(source=DataSource(value=PNG, mime_type="image/png"))
        assert AgnoHitlRunner._content_text("m1", [TextPart(text="a"), part], None) == "a"


class TestHandleAgUiInput:
    async def test_only_the_last_user_message_reaches_the_model(self) -> None:
        """Reasoning, activity and assistant messages a 1.x front sends back never reach the LLM."""
        runner = _runner()
        runner.run = AsyncMock(return_value=None)  # type: ignore[method-assign]
        data = _input(
            UserMessage(id="u0", content="old question"),
            ReasoningMessage(id="rz", content="secret chain of thought"),
            ActivityMessage(id="sources-r0", activity_type="sources", content={"sources": [{"url": "https://a.io/"}]}),
            AssistantMessage(id="a0", content="old answer"),
            UserMessage(
                id="u1",
                content=[TextPart(text="new"), ImagePart(source=DataSource(value=PNG, mime_type="image/png"))],
            ),
        )

        await runner.handle_agui_input(data, send=AsyncMock())

        kwargs = runner.run.await_args.kwargs
        assert kwargs["message"] == "new"
        assert [i.content for i in kwargs["images"]] == [b"\x89PNG-bytes"]
        assert (kwargs["thread_id"], kwargs["agui_tools"]) == ("t1", [])

    async def test_text_only_user_message_passes_no_images(self) -> None:
        runner = _runner()
        runner.run = AsyncMock(return_value=None)  # type: ignore[method-assign]

        await runner.handle_agui_input(_input(UserMessage(id="u1", content="hi")), send=AsyncMock())

        assert runner.run.await_args.kwargs["images"] is None

    async def test_tool_result_parts_are_flattened_to_text(self) -> None:
        record = PausedRunRecord(thread_id="t1", run_id="agno-r", pending_tool_call_ids=["c1"])
        runner = _runner(record)
        runner.continue_paused_run = AsyncMock(return_value=None)  # type: ignore[method-assign]
        data = _input(
            ToolMessage(
                id="tm1",
                tool_call_id="c1",
                content=[TextPart(text="22°C"), ImagePart(source=UrlSource(value="https://x.io/map.png"))],
            )
        )

        resumed, _ = await runner.try_resume(data, send=AsyncMock())

        assert resumed is True
        kwargs = runner.continue_paused_run.await_args.kwargs
        assert kwargs["tool_results"] == {"c1": "22°C"}
        assert (kwargs["run_id"], kwargs["agui_tools"]) == ("r1", [])


class TestAgnoMessagesToAgUi:
    def test_list_content_becomes_text_not_repr(self) -> None:
        messages = [
            AgnoMessage(
                id="u1",
                role="user",
                content=[{"type": "text", "text": "hello"}, {"type": "image_url", "image_url": {"url": "x"}}],
            ),
            AgnoMessage(
                id="a1",
                role="assistant",
                content=None,
                tool_calls=[
                    {"id": "c1", "function": {"name": "ask", "arguments": {"q": 1}}},
                    {"id": "c2", "function": {"arguments": "{}"}},
                ],
            ),
            AgnoMessage(id="t1", role="tool", tool_call_id="c1", content=[{"type": "text", "text": "ok"}]),
            AgnoMessage(id="s1", role="system", content="sys"),
        ]

        user, assistant, tool = HitlEvents.agno_messages_to_agui(messages)

        assert (user.id, user.content) == ("u1", "hello")
        assert assistant.content is None
        assert [(c.id, c.function.name, c.function.arguments) for c in assistant.tool_calls] == [
            ("c1", "ask", '{"q": 1}')
        ]
        assert (tool.tool_call_id, tool.content) == ("c1", "ok")
