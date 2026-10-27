"""Tests for SafeLiteLLM — empty content must never reach litellm's Anthropic sanitizer."""

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace

import litellm
import pytest
from agno.media import Image
from agno.models.litellm import LiteLLM
from agno.models.message import Message
from litellm.litellm_core_utils.prompt_templates.factory import anthropic_messages_pt

from digitalkin.community.agno.safe_litellm import SafeLiteLLM

EMPTY_RESPONSE_NOTICE = SafeLiteLLM.empty_response_notice

PLACEHOLDER = "[System: Empty message content sanitised"


def _tool_call(call_id: str = "call_1") -> dict:
    return {"id": call_id, "type": "function", "function": {"name": "get_weather", "arguments": "{}"}}


def _conversation() -> list[Message]:
    """A tool-use turn as Agno produces it: assistant tool-call messages carry content=None."""
    return [
        Message(role="user", content="What is the weather in Paris?"),
        Message(role="assistant", content=None, tool_calls=[_tool_call()]),
        Message(role="tool", content="Sunny, 22C", tool_call_id="call_1", name="get_weather"),
        Message(role="assistant", content="It is sunny in Paris."),
    ]


def _safe_model() -> SafeLiteLLM:
    return SafeLiteLLM(id="claude-sonnet-4-6", append_trailing_user_message=False)


def _stock_model() -> LiteLLM:
    return LiteLLM(id="claude-sonnet-4-6", append_trailing_user_message=False)


class TestSafeLiteLLMFormatMessages:
    def test_assistant_tool_call_message_has_no_content_key(self) -> None:
        formatted = _safe_model()._format_messages(_conversation())

        tool_call_msgs = [m for m in formatted if m["role"] == "assistant" and m.get("tool_calls")]
        assert len(tool_call_msgs) == 1
        assert "content" not in tool_call_msgs[0]
        assert tool_call_msgs[0]["tool_calls"][0]["id"] == "call_1"

    def test_no_empty_string_content_in_output(self) -> None:
        messages = [*_conversation(), Message(role="assistant", content="   "), Message(role="user", content="")]

        formatted = _safe_model()._format_messages(messages)

        for msg in formatted:
            content = msg.get("content")
            if isinstance(content, str):
                assert content.strip(), f"empty string content leaked: {msg}"
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        assert block["text"].strip(), f"empty text block leaked: {msg}"

    def test_empty_messages_without_tool_calls_are_dropped(self) -> None:
        messages = [
            Message(role="user", content="hi"),
            Message(role="assistant", content=""),
            Message(role="user", content="   "),
        ]

        formatted = _safe_model()._format_messages(messages)

        assert formatted == [{"role": "user", "content": "hi"}]

    def test_tool_messages_left_intact_even_when_empty(self) -> None:
        messages = [
            Message(role="user", content="hi"),
            Message(role="assistant", content=None, tool_calls=[_tool_call()]),
            Message(role="tool", content="", tool_call_id="call_1", name="get_weather"),
        ]

        formatted = _safe_model()._format_messages(messages)

        tool_msgs = [m for m in formatted if m["role"] == "tool"]
        assert len(tool_msgs) == 1
        assert tool_msgs[0]["content"] == ""  # ruff: ignore[compare-to-empty-string] - "" exactly, not None
        assert tool_msgs[0]["tool_call_id"] == "call_1"

    def test_stock_litellm_emits_empty_content_for_tool_call_turns(self) -> None:
        """Regression canary: agno serializes content=None as "" (agno/models/litellm/chat.py)."""
        formatted = _stock_model()._format_messages(_conversation())

        tool_call_msgs = [m for m in formatted if m["role"] == "assistant" and m.get("tool_calls")]
        assert tool_call_msgs[0]["content"] == ""  # ruff: ignore[compare-to-empty-string] - "" exactly, not None


class TestStripEmptyContentListBlocks:
    def test_empty_text_blocks_removed(self) -> None:
        msg = {
            "role": "user",
            "content": [{"type": "text", "text": ""}, {"type": "text", "text": "keep me"}],
        }

        result = _safe_model()._strip_empty_content(msg)

        assert result == {"role": "user", "content": [{"type": "text", "text": "keep me"}]}

    def test_emptied_list_with_tool_calls_removes_content_key(self) -> None:
        msg = {"role": "assistant", "content": [{"type": "text", "text": "   "}], "tool_calls": [_tool_call()]}

        result = _safe_model()._strip_empty_content(msg)

        assert result is not None
        assert "content" not in result
        assert result["tool_calls"]

    def test_emptied_list_without_tool_calls_drops_message(self) -> None:
        msg = {"role": "assistant", "content": [{"type": "text", "text": ""}]}

        assert _safe_model()._strip_empty_content(msg) is None

    def test_non_text_blocks_preserved(self) -> None:
        image_block = {"type": "image_url", "image_url": {"url": "https://example.com/x.png"}}
        msg = {"role": "user", "content": [{"type": "text", "text": ""}, image_block]}

        result = _safe_model()._strip_empty_content(msg)

        assert result == {"role": "user", "content": [image_block]}


class TestAnthropicPayload:
    def test_placeholder_absent_from_final_payload_with_safe_model(self) -> None:
        formatted = _safe_model()._format_messages(_conversation())

        payload = anthropic_messages_pt(formatted, model="claude-sonnet-4-6", llm_provider="anthropic")

        assert PLACEHOLDER not in json.dumps(payload)

    def test_stock_model_produces_placeholder_from_assistant_role(self, caplog: pytest.LogCaptureFixture) -> None:
        """Confirms the empty content reaching litellm comes from assistant messages (task 3).

        Equivalent of running with litellm.set_verbose: litellm's sanitizer logs the role of every
        message it rewrites on the "LiteLLM" verbose logger.
        """
        formatted = _stock_model()._format_messages(_conversation())

        with caplog.at_level(logging.DEBUG, logger="LiteLLM"):
            payload = anthropic_messages_pt(formatted, model="claude-sonnet-4-6", llm_provider="anthropic")

        assert PLACEHOLDER in json.dumps(payload)
        assert "Replaced empty text content in assistant message" in caplog.text
        assert "Replaced empty text content in user message" not in caplog.text


class TestTemperatureGate:
    def test_claude_5_family_is_detected(self) -> None:
        assert SafeLiteLLM.supports_temperature("claude-sonnet-5") is False
        assert SafeLiteLLM.supports_temperature("claude-opus-5") is False
        assert SafeLiteLLM.supports_temperature("claude-haiku-5.1") is False
        assert SafeLiteLLM.supports_temperature("claude-haiku-4.5") is True
        assert SafeLiteLLM.supports_temperature("gpt-5") is True

    def test_request_omits_temperature_when_disabled(self) -> None:
        model = SafeLiteLLM(id="claude-sonnet-5", send_temperature=False)
        assert "temperature" not in model.get_request_params()

    def test_request_keeps_temperature_by_default(self) -> None:
        model = SafeLiteLLM(id="gpt-5", temperature=0.3)
        assert model.get_request_params()["temperature"] == pytest.approx(0.3)


CACHE = {"type": "ephemeral"}


def _cached_model() -> SafeLiteLLM:
    return SafeLiteLLM(id="claude-sonnet-5", append_trailing_user_message=False, enable_prompt_caching=True)


class TestPromptCaching:
    def test_model_family_detection(self) -> None:
        assert SafeLiteLLM.uses_anthropic_cache("claude-sonnet-5") is True
        assert SafeLiteLLM.uses_anthropic_cache("claude-haiku-4.5") is True
        assert SafeLiteLLM.uses_anthropic_cache("gpt-5") is False
        assert SafeLiteLLM.uses_anthropic_cache("gemini-2.5-pro") is False

    def test_system_prompt_gets_a_breakpoint(self) -> None:
        messages = [Message(role="system", content="You are a kin."), *_conversation()]
        formatted = _cached_model()._format_messages(messages)
        assert formatted[0]["role"] == "system"
        assert formatted[0]["content"] == [{"type": "text", "text": "You are a kin.", "cache_control": CACHE}]

    def test_last_tool_result_gets_a_breakpoint(self) -> None:
        messages = _conversation()[:3]  # ends on the tool result
        formatted = _cached_model()._format_messages(messages)
        assert formatted[-1]["role"] == "tool"
        assert formatted[-1]["cache_control"] == CACHE
        assert all("cache_control" not in m for m in formatted[:-1])

    def test_last_user_message_gets_a_breakpoint(self) -> None:
        formatted = _cached_model()._format_messages([Message(role="user", content="hi")])
        assert formatted[-1]["content"] == [{"type": "text", "text": "hi", "cache_control": CACHE}]

    def test_last_tool_definition_gets_a_breakpoint(self) -> None:
        tools = [
            {"type": "function", "function": {"name": "a", "parameters": {}}},
            {"type": "function", "function": {"name": "b", "parameters": {}}},
        ]
        params = _cached_model().get_request_params(tools=tools)
        assert "cache_control" not in params["tools"][0]
        assert params["tools"][1]["cache_control"] == CACHE
        assert "cache_control" not in tools[1], "caller's tool list must not be mutated"

    def test_disabled_by_default(self) -> None:
        formatted = _safe_model()._format_messages([Message(role="system", content="x"), *_conversation()])
        assert formatted[0]["content"] == "x"
        assert all("cache_control" not in m for m in formatted)


class TestToolResultCap:
    def _model(self, **kwargs: object) -> SafeLiteLLM:
        return SafeLiteLLM(
            id="claude-sonnet-4-6", append_trailing_user_message=False, max_tool_result_chars=50, **kwargs
        )

    def _tool_turn(self, size: int) -> list[Message]:
        return [
            *_conversation()[:2],
            Message(role="tool", content="x" * size, tool_call_id="call_1", name="get_weather"),
        ]

    def test_oversized_tool_result_is_cut_with_a_note(self) -> None:
        formatted = self._model()._format_messages(self._tool_turn(500))
        content = formatted[-1]["content"]
        assert content.startswith("x" * 50)
        assert "x" * 51 not in content
        assert "truncated" in content
        assert "50 of 500" in content

    def test_tool_result_under_the_cap_is_untouched(self) -> None:
        formatted = self._model()._format_messages(self._tool_turn(50))
        assert formatted[-1]["content"] == "x" * 50

    def test_user_content_is_never_capped(self) -> None:
        formatted = self._model()._format_messages([Message(role="user", content="y" * 500)])
        assert formatted[-1]["content"] == "y" * 500

    def test_cap_applies_before_the_cache_breakpoint(self) -> None:
        formatted = self._model(enable_prompt_caching=True)._format_messages(self._tool_turn(500))
        assert formatted[-1]["cache_control"] == CACHE
        assert "truncated" in formatted[-1]["content"]


class TestContextWindowTrim:
    """The trim must see string system content: litellm's trimmer cannot read cache-marked blocks."""

    @pytest.fixture
    def tiny_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import litellm

        # litellm's trimmer reads ``max_tokens`` even when ``max_input_tokens`` is set, and the cost
        # map it downloads at import may lack the key: pin both so the test owns the window.
        spec = {**litellm.model_cost.get("claude-sonnet-4-6", {}), "max_input_tokens": 400, "max_tokens": 400}
        monkeypatch.setitem(litellm.model_cost, "claude-sonnet-4-6", spec)

    @staticmethod
    def _cached_tiny_model() -> SafeLiteLLM:
        return SafeLiteLLM(id="claude-sonnet-4-6", append_trailing_user_message=False, enable_prompt_caching=True)

    @staticmethod
    def _long_conversation() -> list[Message]:
        turns = [
            Message(role="user" if i % 2 == 0 else "assistant", content=f"turn {i} " + "word " * 100) for i in range(20)
        ]
        return [Message(role="system", content="You are a kin."), *turns]

    @pytest.mark.usefixtures("tiny_window")
    def test_oldest_turns_are_dropped_to_fit(self) -> None:
        formatted = _safe_model()._format_messages(self._long_conversation())
        assert len(formatted) < 21
        assert formatted[0]["role"] == "system"
        assert formatted[-1]["content"].startswith("turn 19")

    @pytest.mark.usefixtures("tiny_window")
    def test_trim_runs_before_cache_marking(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.ERROR, logger="LiteLLM"):
            formatted = self._cached_tiny_model()._format_messages(self._long_conversation())
        assert "token trimming" not in caplog.text
        assert len(formatted) < 21
        assert formatted[0]["content"][-1]["cache_control"] == CACHE

    @pytest.mark.usefixtures("tiny_window")
    def test_stock_trimmer_gives_up_on_cache_marked_system_blocks(self, caplog: pytest.LogCaptureFixture) -> None:
        """The production failure: trimming after marking logs an error and changes nothing."""
        from litellm.utils import trim_messages

        plain = _stock_model()._format_messages(self._long_conversation())
        marked = self._cached_tiny_model()._mark_cache_breakpoints(plain)
        with caplog.at_level(logging.ERROR, logger="LiteLLM"):
            untouched = trim_messages(marked, "claude-sonnet-4-6")
        assert "token trimming" in caplog.text
        assert len(untouched) == len(marked)


class TestCancelToken:
    @pytest.mark.asyncio
    async def test_ainvoke_refuses_after_cancel(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import asyncio

        from agno.exceptions import RunCancelledException

        event = asyncio.Event()
        event.set()
        model = SafeLiteLLM(id="claude-sonnet-5", cancel_event=event)
        monkeypatch.setattr(model, "get_client", lambda: pytest.fail("client must not be called"))
        with pytest.raises(RunCancelledException):
            await model.ainvoke(
                messages=[Message(role="user", content="hi")], assistant_message=Message(role="assistant")
            )

    @pytest.mark.asyncio
    async def test_ainvoke_stream_refuses_after_cancel(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import asyncio

        from agno.exceptions import RunCancelledException

        event = asyncio.Event()
        event.set()
        model = SafeLiteLLM(id="claude-sonnet-5", cancel_event=event)
        monkeypatch.setattr(model, "get_client", lambda: pytest.fail("client must not be called"))
        with pytest.raises(RunCancelledException):
            async for _ in model.ainvoke_stream(
                messages=[Message(role="user", content="hi")], assistant_message=Message(role="assistant")
            ):
                pass


class TestHistoryAlignment:
    @staticmethod
    def _history(*messages: Message) -> list[Message]:
        for msg in messages:
            msg.from_history = True
        return list(messages)

    def test_history_starting_mid_tool_loop_gets_the_note_and_keeps_every_message(self) -> None:
        history = self._history(
            Message(role="assistant", content=None, tool_calls=[_tool_call("call_0")]),
            Message(role="tool", content="old", tool_call_id="call_0", name="get_weather"),
            Message(role="assistant", content="Old answer."),
            *_conversation(),
        )
        messages = [Message(role="system", content="sys"), *history, Message(role="user", content="And Lyon?")]

        formatted = _safe_model()._format_messages(messages)

        assert [m["role"] for m in formatted] == [
            "system", "user", "assistant", "tool", "assistant", "user", "assistant", "tool", "assistant", "user",
        ]  # fmt: skip
        assert formatted[1]["content"].startswith("[Earlier messages of this conversation were omitted")
        assert formatted[4]["content"] == "Old answer."
        assert len(messages) == 9, "the note must not leak into the persisted run"

    def test_history_starting_on_the_media_follow_up_gets_the_note(self) -> None:
        history = self._history(
            Message(role="user", content="The tool call above generated the attached media."),
            Message(role="assistant", content="Here is the chart."),
        )
        formatted = _safe_model()._format_messages([*history, Message(role="user", content="Lyon?")])

        assert formatted[0]["content"].startswith("[Earlier messages")
        assert formatted[1]["content"] == "The tool call above generated the attached media."

    def test_history_starting_on_a_user_turn_is_untouched(self) -> None:
        messages = [*self._history(*_conversation()), Message(role="user", content="And Lyon?")]

        formatted = _safe_model()._format_messages(messages)

        assert formatted[0] == {"role": "user", "content": "What is the weather in Paris?"}
        assert len(formatted) == 5

    def test_messages_without_history_are_untouched(self) -> None:
        messages = [Message(role="assistant", content=None, tool_calls=[_tool_call()]), *_conversation()[2:]]

        formatted = _safe_model()._format_messages(messages)

        assert [m["role"] for m in formatted] == ["assistant", "tool", "assistant"]


class _Ns(SimpleNamespace):
    """A litellm message/delta stand-in: litellm's objects expose a dict-like ``get``."""

    def get(self, key: str, default: object = None) -> object:
        return self.__dict__.get(key, default)


def _completion(content: str | None, tool_calls: list | None = None) -> SimpleNamespace:
    message = _Ns(content=content, tool_calls=tool_calls)
    return SimpleNamespace(choices=[SimpleNamespace(message=message, finish_reason="stop")], usage=None)


def _chunks(*contents: str | None) -> list[SimpleNamespace]:
    return [
        SimpleNamespace(
            choices=[SimpleNamespace(delta=_Ns(content=c, tool_calls=None), finish_reason=None)], usage=None
        )
        for c in contents
    ]


class _FakeClient:
    """Stands in for litellm: replays one scripted reply per call and records each request.

    A scripted reply that is an exception is raised at ``acompletion`` time, after the request is
    recorded: the proxy rejects a request before streaming anything.
    """

    def __init__(self, *replies: object) -> None:
        self.replies = list(replies)
        self.requests: list[list[dict]] = []

    async def acompletion(self, **kwargs: object) -> object:
        self.requests.append(kwargs["messages"])  # type: ignore[arg-type]
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        if not kwargs.get("stream"):
            return reply

        async def stream() -> AsyncIterator[object]:  # ruff: ignore[unused-async] - an async generator
            for chunk in reply:  # type: ignore[attr-defined]
                yield chunk

        return stream()


def _model_with(client: _FakeClient, **kwargs: object) -> SafeLiteLLM:
    model = SafeLiteLLM(id="gpt-5", append_trailing_user_message=False, **kwargs)
    model.get_client = lambda: client  # type: ignore[method-assign]
    return model


def _last_text(request: list[dict]) -> str:
    content = request[-1]["content"]
    return content if isinstance(content, str) else content[-1]["text"]


class TestEmptyReplyRecovery:
    @pytest.mark.asyncio
    async def test_non_empty_reply_is_not_retried(self) -> None:
        client = _FakeClient(_completion("Hello"))

        response = await _model_with(client).ainvoke([Message(role="user", content="hi")], Message(role="assistant"))

        assert response.content == "Hello"
        assert len(client.requests) == 1

    @pytest.mark.asyncio
    async def test_tool_call_reply_is_not_empty(self) -> None:
        call = SimpleNamespace(id="call_1", function=SimpleNamespace(name="get_weather", arguments="{}"))
        client = _FakeClient(_completion(None, [call]))

        response = await _model_with(client).ainvoke([Message(role="user", content="hi")], Message(role="assistant"))

        assert response.tool_calls
        assert len(client.requests) == 1

    @pytest.mark.asyncio
    async def test_empty_reply_is_retried_with_a_continuation_prompt(self) -> None:
        client = _FakeClient(_completion("  "), _completion("Recovered"))
        messages = [Message(role="user", content="hi")]

        response = await _model_with(client).ainvoke(messages, Message(role="assistant"))

        assert response.content == "Recovered"
        assert _last_text(client.requests[1]) == "Please continue."
        assert len(messages) == 1, "the continuation prompt must not leak into the persisted run"

    @pytest.mark.asyncio
    async def test_notice_replaces_a_reply_empty_twice(self) -> None:
        client = _FakeClient(_completion(None), _completion(""))

        response = await _model_with(client).ainvoke([Message(role="user", content="hi")], Message(role="assistant"))

        assert response.content == EMPTY_RESPONSE_NOTICE
        assert len(client.requests) == 2

    @pytest.mark.asyncio
    async def test_structured_output_never_gets_the_notice(self) -> None:
        client = _FakeClient(_completion(None), _completion(None))

        response = await _model_with(client).ainvoke(
            [Message(role="user", content="hi")], Message(role="assistant"), response_format={"type": "json_object"}
        )

        assert response.content is None

    @pytest.mark.asyncio
    async def test_notice_can_be_disabled(self) -> None:
        client = _FakeClient(_completion(None), _completion(None))

        response = await _model_with(client, empty_response_notice=None).ainvoke(
            [Message(role="user", content="hi")], Message(role="assistant")
        )

        assert response.content is None

    @pytest.mark.asyncio
    async def test_stream_with_text_is_not_retried(self) -> None:
        client = _FakeClient(_chunks("Hel", "lo"))

        chunks = [
            c
            async for c in _model_with(client).ainvoke_stream(
                [Message(role="user", content="hi")], Message(role="assistant")
            )
        ]

        assert "".join(c.content or "" for c in chunks) == "Hello"
        assert len(client.requests) == 1

    @pytest.mark.asyncio
    async def test_empty_stream_is_retried_with_a_continuation_prompt(self) -> None:
        client = _FakeClient(_chunks(None, ""), _chunks("Recovered"))

        chunks = [
            c
            async for c in _model_with(client).ainvoke_stream(
                [Message(role="user", content="hi")], Message(role="assistant")
            )
        ]

        assert "".join(c.content or "" for c in chunks) == "Recovered"
        assert _last_text(client.requests[1]) == "Please continue."

    @pytest.mark.asyncio
    async def test_stream_empty_twice_ends_with_the_notice(self) -> None:
        client = _FakeClient(_chunks(None), _chunks(" "))

        chunks = [
            c
            async for c in _model_with(client).ainvoke_stream(
                [Message(role="user", content="hi")], Message(role="assistant")
            )
        ]

        assert chunks[-1].content == EMPTY_RESPONSE_NOTICE
        assert len(client.requests) == 2

    @pytest.mark.asyncio
    async def test_retry_is_refused_once_cancelled(self) -> None:
        import asyncio

        from agno.exceptions import RunCancelledException

        event = asyncio.Event()
        client = _FakeClient(_completion(None))
        model = _model_with(client, cancel_event=event)
        original = client.acompletion

        async def cancel_after_first(**kwargs: object) -> object:
            reply = await original(**kwargs)
            event.set()
            return reply

        client.acompletion = cancel_after_first  # type: ignore[method-assign]

        with pytest.raises(RunCancelledException):
            await model.ainvoke([Message(role="user", content="hi")], Message(role="assistant"))
        assert len(client.requests) == 1


_MEDIA_FOLLOW_UP_TEXT = "The tool call above generated the attached media."


def _image_rejection() -> litellm.BadRequestError:
    """The error the proxy raised in production for an undecodable figure."""
    return litellm.BadRequestError(
        message=(
            "Vertex_ai_betaException BadRequestError - Failed to decode image data. "
            "Please make sure the image is valid."
        ),
        model="gemini-3.7-flash-reasoning-high",
        llm_provider="litellm_proxy",
    )


def _media_turn() -> list[Message]:
    """A tool call whose result carried two figures, as Agno builds it."""
    return [
        Message(role="user", content="Read the paper"),
        Message(role="assistant", content=None, tool_calls=[_tool_call()]),
        Message(role="tool", content="# Paper text", tool_call_id="call_1", name="get_weather"),
        Message(
            role="user",
            content=_MEDIA_FOLLOW_UP_TEXT,
            images=[Image(url="https://x.test/fig1.png"), Image(url="https://x.test/fig2.png")],
        ),
    ]


def _image_parts(request: list[dict]) -> int:
    return sum(
        1
        for msg in request
        if isinstance(msg.get("content"), list)
        for part in msg["content"]
        if isinstance(part, dict) and part.get("type") == "image_url"
    )


def _all_text(request: list[dict]) -> str:
    texts: list[str] = []
    for msg in request:
        content = msg.get("content")
        if isinstance(content, str):
            texts.append(content)
        elif isinstance(content, list):
            texts.extend(p["text"] for p in content if isinstance(p, dict) and p.get("type") == "text")
    return "\n".join(texts)


def _mixed_turn() -> list[Message]:
    """A user message with one image by URL (an attachment) and one inline (a view_image result)."""
    return [
        Message(
            role="user",
            content="What do these show?",
            images=[Image(url="http://localhost:3000/v1/files/f1"), Image(content=b"\x89PNG", format="png")],
        )
    ]


class TestUnfetchableImageUrls:
    """A provider that cannot fetch an image URL costs only the URL images, inline ones are kept."""

    @pytest.mark.asyncio
    async def test_an_unfetchable_url_drops_only_the_url_images(self) -> None:
        rejection = litellm.BadRequestError(
            message="Unable to fetch image from URL after 3 attempts. url=http://localhost:3000/v1/files/f1",
            model="gemini-4-argon",
            llm_provider="litellm_proxy",
        )
        client = _FakeClient(rejection, _completion("Done"), _completion("Again"))
        model = _model_with(client)

        response = await model.ainvoke(_mixed_turn(), Message(role="assistant"))

        assert response.content == "Done"
        assert (model.drop_image_urls, model.drop_images) == (True, False)
        assert _image_parts(client.requests[1]) == 1, "the inline image is still sent"
        assert "[1 image(s) removed from this message" in _all_text(client.requests[1])

        await model.ainvoke(_mixed_turn(), Message(role="assistant"))
        assert _image_parts(client.requests[2]) == 1, "later requests keep the inline image too"

    @pytest.mark.asyncio
    async def test_a_proxy_500_blaming_an_image_is_retried(self) -> None:
        """Regression: a blocked image URL came back as a 500 and failed the run."""
        rejection = litellm.APIConnectionError(
            message="Error code: 500 - URL targets a blocked address (127.0.0.1)",
            model="gpt-6.1-sol",
            llm_provider="litellm_proxy",
        )
        client = _FakeClient(rejection, _completion("Done"))
        model = _model_with(client)

        response = await model.ainvoke(_mixed_turn(), Message(role="assistant"))

        assert response.content == "Done"
        assert model.drop_image_urls is True
        assert _image_parts(client.requests[1]) == 1

    @pytest.mark.asyncio
    async def test_a_500_unrelated_to_images_is_raised(self) -> None:
        client = _FakeClient(
            litellm.APIConnectionError(message="Error code: 500 - upstream timeout", model="m", llm_provider="p")
        )

        with pytest.raises(litellm.APIConnectionError):
            await _model_with(client).ainvoke(_mixed_turn(), Message(role="assistant"))


class TestImageRejectionFallback:
    @pytest.mark.asyncio
    async def test_image_rejection_is_retried_without_images(self) -> None:
        client = _FakeClient(_image_rejection(), _completion("Done"))
        model = _model_with(client)

        response = await model.ainvoke(_media_turn(), Message(role="assistant"))

        assert response.content == "Done"
        assert len(client.requests) == 2
        assert _image_parts(client.requests[0]) == 2
        assert _image_parts(client.requests[1]) == 0
        assert "[2 image(s) removed from this message" in _all_text(client.requests[1])
        assert model.drop_images is True

    @pytest.mark.asyncio
    async def test_later_requests_of_the_task_send_no_images(self) -> None:
        client = _FakeClient(_image_rejection(), _completion("First"), _completion("Second"))
        model = _model_with(client)

        await model.ainvoke(_media_turn(), Message(role="assistant"))
        await model.ainvoke(_media_turn(), Message(role="assistant"))

        assert len(client.requests) == 3, "the second call must not pay another rejection"
        assert _image_parts(client.requests[2]) == 0

    @pytest.mark.asyncio
    async def test_persisted_messages_keep_their_images(self) -> None:
        client = _FakeClient(_image_rejection(), _completion("Done"))
        messages = _media_turn()

        await _model_with(client).ainvoke(messages, Message(role="assistant"))

        assert messages[-1].images is not None
        assert len(messages[-1].images) == 2, "stripping belongs to the request, not to the stored run"

    @pytest.mark.asyncio
    async def test_rejection_without_images_in_the_request_is_raised(self) -> None:
        client = _FakeClient(_image_rejection())

        with pytest.raises(litellm.BadRequestError):
            await _model_with(client).ainvoke([Message(role="user", content="hi")], Message(role="assistant"))
        assert len(client.requests) == 1

    @pytest.mark.asyncio
    async def test_other_bad_requests_are_raised(self) -> None:
        error = litellm.BadRequestError(message="prompt is too long", model="gpt-5", llm_provider="litellm_proxy")
        client = _FakeClient(error)
        model = _model_with(client)

        with pytest.raises(litellm.BadRequestError, match="prompt is too long"):
            await model.ainvoke(_media_turn(), Message(role="assistant"))
        assert len(client.requests) == 1
        assert model.drop_images is False

    @pytest.mark.asyncio
    async def test_a_second_rejection_is_raised(self) -> None:
        client = _FakeClient(_image_rejection(), _image_rejection())

        with pytest.raises(litellm.BadRequestError):
            await _model_with(client).ainvoke(_media_turn(), Message(role="assistant"))
        assert len(client.requests) == 2

    @pytest.mark.asyncio
    async def test_retry_without_images_is_refused_once_cancelled(self) -> None:
        import asyncio

        from agno.exceptions import RunCancelledException

        event = asyncio.Event()
        client = _FakeClient(_image_rejection())
        model = _model_with(client, cancel_event=event)
        original = client.acompletion

        async def cancel_on_rejection(**kwargs: object) -> object:
            try:
                return await original(**kwargs)
            finally:
                event.set()

        client.acompletion = cancel_on_rejection  # type: ignore[method-assign]

        with pytest.raises(RunCancelledException):
            await model.ainvoke(_media_turn(), Message(role="assistant"))
        assert len(client.requests) == 1

    @pytest.mark.asyncio
    async def test_stream_image_rejection_is_retried_without_images(self) -> None:
        client = _FakeClient(_image_rejection(), _chunks("Done"))

        chunks = [c async for c in _model_with(client).ainvoke_stream(_media_turn(), Message(role="assistant"))]

        assert "".join(c.content or "" for c in chunks) == "Done"
        assert len(client.requests) == 2
        assert _image_parts(client.requests[1]) == 0

    @pytest.mark.asyncio
    async def test_stream_failing_after_output_is_not_retried(self) -> None:
        class _MidStreamFailure:
            """Yields one chunk, then fails like a rejection would."""

            def __iter__(self) -> Iterator[object]:
                yield from _chunks("Partial")
                raise _image_rejection()

        client = _FakeClient(_MidStreamFailure())
        received: list[str] = []

        with pytest.raises(litellm.BadRequestError):  # ruff: ignore[pytest-raises-with-multiple-statements]
            async for chunk in _model_with(client).ainvoke_stream(_media_turn(), Message(role="assistant")):
                received.append(chunk.content or "")  # ruff: ignore[manual-list-comprehension] - keep chunks seen before the error
        assert received == ["Partial"]
        assert len(client.requests) == 1


class _ConcurrentClient:
    """Holds every call until two requests are in flight, then answers each by its own content.

    A request carrying image parts is rejected; any other request succeeds. This interleaves the
    two calls exactly where production does: both are formatted before either gets its answer.
    """

    def __init__(self) -> None:
        self.requests: list[list[dict]] = []
        self._both_sent = asyncio.Event()

    async def acompletion(self, **kwargs: object) -> object:
        request = kwargs["messages"]
        self.requests.append(request)  # type: ignore[arg-type]
        if len(self.requests) >= 2:
            self._both_sent.set()
        await self._both_sent.wait()
        if _image_parts(request):  # type: ignore[arg-type]
            raise _image_rejection()
        return _completion("Done")


class TestImageRejectionFallbackConcurrency:
    @pytest.mark.asyncio
    async def test_parallel_calls_on_one_model_each_keep_their_own_image_count(self) -> None:
        client = _ConcurrentClient()
        model = SafeLiteLLM(id="gpt-5", append_trailing_user_message=False)
        model.get_client = lambda: client  # type: ignore[method-assign]

        with_images, text_only = await asyncio.gather(
            model.ainvoke(_media_turn(), Message(role="assistant")),
            model.ainvoke([Message(role="user", content="hi")], Message(role="assistant")),
        )

        assert with_images.content == "Done", "the image request must fall back even with a call in parallel"
        assert text_only.content == "Done"
        assert len(client.requests) == 3
        assert model.drop_images is True


class _FakeCompactor:
    """Stands in for HistoryCompactor: replaces the request with a marked view, records each call."""

    def __init__(self, *, refit: bool = True) -> None:
        self.refit = refit
        self.views: list[list[Message]] = []
        self.forced: list[list[Message]] = []
        self.observed: list[int] = []

    async def view(self, messages: list[Message], tools: object = None, run_response: object = None) -> list[Message]:
        self.views.append(messages)
        return [Message(role="user", content="COMPACTED VIEW"), *messages[-1:]]

    async def force_fold(
        self, messages: list[Message], tools: object = None, run_response: object = None
    ) -> list[Message] | None:
        self.forced.append(messages)
        return [Message(role="user", content="REFOLDED VIEW"), *messages[-1:]] if self.refit else None

    async def observe(self, sent: list[Message], tools: object, provider_tokens: int) -> None:
        self.observed.append(provider_tokens)


def _overflow() -> litellm.ContextWindowExceededError:
    return litellm.ContextWindowExceededError(
        message="prompt is too long: 1050000 tokens > 1000000 maximum",
        model="claude-sonnet-5",
        llm_provider="anthropic",
    )


def _proxied_overflow() -> litellm.BadRequestError:
    """The proxy can relay an overflow as a plain 400."""
    return litellm.BadRequestError(
        message="litellm.BadRequestError: AnthropicException - prompt is too long: 1050000 tokens",
        model="claude-sonnet-5",
        llm_provider="litellm_proxy",
    )


def _output_overflow() -> litellm.BadRequestError:
    """Anthropic counts the requested output against the window too."""
    return litellm.BadRequestError(
        message="AnthropicException - input length and `max_tokens` exceed context limit: 180000 + 64000 > 200000",
        model="claude-sonnet-5",
        llm_provider="litellm_proxy",
    )


class TestHistoryCompaction:
    @pytest.mark.asyncio
    async def test_the_compacted_view_is_what_is_sent(self) -> None:
        client = _FakeClient(_completion("Done"))
        compactor = _FakeCompactor()
        messages = [Message(role="user", content="old", from_history=True), Message(role="user", content="now")]

        await _model_with(client, compactor=compactor).ainvoke(messages, Message(role="assistant"))

        assert compactor.views == [messages]
        assert "COMPACTED VIEW" in _all_text(client.requests[0])
        assert "old" not in _all_text(client.requests[0])
        assert len(messages) == 2, "Agno's own list is never rewritten"

    @pytest.mark.asyncio
    async def test_the_stream_sends_the_compacted_view(self) -> None:
        client = _FakeClient(_chunks("Done"))
        compactor = _FakeCompactor()

        chunks = [
            chunk
            async for chunk in _model_with(client, compactor=compactor).ainvoke_stream(
                [Message(role="user", content="now")], Message(role="assistant")
            )
        ]

        assert [c.content for c in chunks] == ["Done"]
        assert "COMPACTED VIEW" in _all_text(client.requests[0])

    @pytest.mark.asyncio
    async def test_without_a_compactor_the_messages_are_sent_as_is(self) -> None:
        client = _FakeClient(_completion("Done"))

        await _model_with(client).ainvoke([Message(role="user", content="now")], Message(role="assistant"))

        assert _all_text(client.requests[0]) == "now"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error", [_overflow, _proxied_overflow, _output_overflow])
    async def test_an_overflow_is_folded_and_resent_once(self, error: object) -> None:
        client = _FakeClient(error(), _completion("Done"))  # type: ignore[operator]
        compactor = _FakeCompactor()
        messages = [Message(role="user", content="now")]

        response = await _model_with(client, compactor=compactor).ainvoke(messages, Message(role="assistant"))

        assert response.content == "Done"
        assert compactor.forced == [messages]
        assert "REFOLDED VIEW" in _all_text(client.requests[1])

    @pytest.mark.asyncio
    async def test_a_streamed_overflow_is_folded_and_resent_once(self) -> None:
        client = _FakeClient(_overflow(), _chunks("Done"))
        compactor = _FakeCompactor()

        chunks = [
            chunk
            async for chunk in _model_with(client, compactor=compactor).ainvoke_stream(
                [Message(role="user", content="now")], Message(role="assistant")
            )
        ]

        assert [c.content for c in chunks] == ["Done"]
        assert "REFOLDED VIEW" in _all_text(client.requests[1])

    @pytest.mark.asyncio
    async def test_an_overflow_nothing_can_fold_is_raised(self) -> None:
        client = _FakeClient(_overflow())

        with pytest.raises(litellm.ContextWindowExceededError):
            await _model_with(client, compactor=_FakeCompactor(refit=False)).ainvoke(
                [Message(role="user", content="now")], Message(role="assistant")
            )
        assert len(client.requests) == 1

    @pytest.mark.asyncio
    async def test_an_overflow_without_a_compactor_is_raised(self) -> None:
        client = _FakeClient(_overflow())

        with pytest.raises(litellm.ContextWindowExceededError):
            await _model_with(client).ainvoke([Message(role="user", content="now")], Message(role="assistant"))

    @pytest.mark.asyncio
    async def test_other_bad_requests_are_not_folded(self) -> None:
        client = _FakeClient(litellm.BadRequestError(message="invalid tool schema", model="m", llm_provider="p"))
        compactor = _FakeCompactor()

        with pytest.raises(litellm.BadRequestError):
            await _model_with(client, compactor=compactor).ainvoke(
                [Message(role="user", content="now")], Message(role="assistant")
            )
        assert compactor.forced == []

    def test_trim_keeps_95_percent_of_the_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import digitalkin.community.agno.safe_litellm as safe_mod

        calls: list[dict] = []

        def fake_trim(messages: list[dict], model: str, **kwargs: object) -> list[dict]:
            calls.append(kwargs)
            return messages

        monkeypatch.setattr(safe_mod, "trim_messages", fake_trim)

        _safe_model()._format_messages([Message(role="user", content="hi")])

        assert calls == [{"trim_ratio": 0.95}]

    def test_trim_fits_the_input_budget_when_known(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The proxy model group is unknown to litellm: the budget from the spec drives the trim."""
        import digitalkin.community.agno.safe_litellm as safe_mod

        calls: list[dict] = []

        def fake_trim(messages: list[dict], model: str, **kwargs: object) -> list[dict]:
            calls.append(kwargs)
            return messages[1:]

        monkeypatch.setattr(safe_mod, "trim_messages", fake_trim)
        model = SafeLiteLLM(id="claude-sonnet-5", append_trailing_user_message=False, input_tokens=136_000)

        model._format_messages([Message(role="user", content="old"), Message(role="user", content="hi")])

        assert calls == [{"max_tokens": 129_200}]


def _usage(prompt_tokens: int) -> SimpleNamespace:
    return SimpleNamespace(
        prompt_tokens=prompt_tokens, completion_tokens=5, prompt_tokens_details=None, completion_tokens_details=None
    )


class TestTokenCalibration:
    @pytest.mark.asyncio
    async def test_the_provider_prompt_size_reaches_the_compactor(self) -> None:
        reply = _completion("Done")
        reply.usage = _usage(46_996)
        compactor = _FakeCompactor()

        await _model_with(_FakeClient(reply), compactor=compactor).ainvoke(
            [Message(role="user", content="now")], Message(role="assistant")
        )

        assert compactor.observed == [46_996]

    @pytest.mark.asyncio
    async def test_the_streamed_usage_reaches_the_compactor(self) -> None:
        chunks = _chunks("Done", None)
        chunks[-1].usage = _usage(12_345)
        compactor = _FakeCompactor()

        async for _ in _model_with(_FakeClient(chunks), compactor=compactor).ainvoke_stream(
            [Message(role="user", content="now")], Message(role="assistant")
        ):
            pass

        assert compactor.observed == [12_345]

    @pytest.mark.asyncio
    async def test_no_usage_no_calibration(self) -> None:
        compactor = _FakeCompactor()

        await _model_with(_FakeClient(_completion("Done")), compactor=compactor).ainvoke(
            [Message(role="user", content="now")], Message(role="assistant")
        )

        assert compactor.observed == []
