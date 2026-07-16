"""Tests for ChatHistoryTools — outline index, read-by-id, role filter, truncation, media, bind_host."""

import json
from typing import Any

from digitalkin.community.agno.toolkits import ChatHistoryTools


class _FakeMedia:
    """Stand-in for an agno media item (Image/Audio/Video/File)."""

    def __init__(self, media_id: str, mime_type: str, media_format: str, content: bytes = b"") -> None:
        self.id = media_id
        self.mime_type = mime_type
        self.format = media_format
        self.content = content


class _FakeMessage:
    """Stand-in for ``agno.models.message.Message`` with the attributes the toolkit reads."""

    def __init__(  # noqa: PLR0913
        self,
        role: str,
        content: str,
        message_id: str,
        tool_name: str | None = None,
        tool_call_error: bool = False,
        images: list[Any] | None = None,
        from_history: bool = False,
    ) -> None:
        self.role = role
        self.content = content
        self.id = message_id
        self.created_at = 1234
        self.tool_name = tool_name
        self.tool_call_error = tool_call_error
        self.images = images
        self.files = None
        self.videos = None
        self.audio = None
        self.from_history = from_history

    def get_content_string(self) -> str:
        return self.content


class _FakeHost:
    """Stand-in for the bound Agent/Team; emulates Agno's ``skip_roles`` filtering."""

    def __init__(self, messages: list[_FakeMessage]) -> None:
        self._messages = messages

    async def aget_session_messages(
        self,
        session_id: str | None,
        skip_roles: list[str],
        skip_history_messages: bool,
    ) -> list[_FakeMessage]:
        return [m for m in self._messages if m.role not in skip_roles]


def _conversation() -> list[_FakeMessage]:
    return [
        _FakeMessage("user", "First human question", "m0"),
        _FakeMessage("assistant", "First AI answer", "m1"),
        _FakeMessage("tool", "tool output", "m2", tool_name="search"),
        _FakeMessage("user", "Second human question", "m3"),
        _FakeMessage("assistant", "Second AI answer", "m4"),
    ]


def _tools(messages: list[_FakeMessage]) -> ChatHistoryTools:
    tools = ChatHistoryTools()
    tools.host = _FakeHost(messages)
    return tools


async def test_outline_empty_session_reports_total_zero() -> None:
    tools = _tools([])
    result = json.loads(await tools.outline_chat_history())
    assert result["output"] == {"total": 0, "returned": 0, "offset": 0, "messages": []}


async def test_outline_first_returns_earliest() -> None:
    tools = _tools(_conversation())
    result = json.loads(await tools.outline_chat_history(first=1))["output"]
    assert result["total"] == 5
    assert result["returned"] == 1
    assert result["messages"][0]["id"] == "m0"
    assert result["messages"][0]["ord"] == 0
    # metadata only — no full body field
    assert "content" not in result["messages"][0]


async def test_outline_last_returns_most_recent() -> None:
    tools = _tools(_conversation())
    result = json.loads(await tools.outline_chat_history(last=1))["output"]
    assert result["messages"][0]["id"] == "m4"


async def test_outline_role_human_only() -> None:
    tools = _tools(_conversation())
    result = json.loads(await tools.outline_chat_history(role="human"))["output"]
    assert [m["id"] for m in result["messages"]] == ["m0", "m3"]
    assert all(m["role"] == "human" for m in result["messages"])


async def test_outline_role_tool_has_name() -> None:
    tools = _tools(_conversation())
    result = json.loads(await tools.outline_chat_history(role="tool"))["output"]
    assert result["messages"][0]["role"] == "tool"
    assert result["messages"][0]["name"] == "search"


async def test_outline_invalid_role_errors() -> None:
    tools = _tools(_conversation())
    result = json.loads(await tools.outline_chat_history(role="bogus"))
    assert "error" in result


async def test_read_by_id_returns_content_and_missing() -> None:
    tools = _tools(_conversation())
    result = json.loads(await tools.read_chat_messages(ids=["m0", "ghost"]))["output"]
    assert result["missing"] == ["ghost"]
    assert len(result["messages"]) == 1
    assert result["messages"][0]["id"] == "m0"
    assert result["messages"][0]["content"] == "First human question"


async def test_read_truncates_long_body() -> None:
    tools = _tools([_FakeMessage("assistant", "x" * 50, "big")])
    result = json.loads(await tools.read_chat_messages(ids=["big"], max_content_chars=10))["output"]
    msg = result["messages"][0]
    assert msg["truncated"] is True
    assert msg["content"].startswith("x" * 10)
    assert "[…truncated]" in msg["content"]


async def test_media_is_referenced_not_inlined() -> None:
    image = _FakeMedia("img1", "image/png", "png", content=b"RAWBYTES")
    tools = _tools([_FakeMessage("user", "see this", "m0", images=[image])])

    outline = json.loads(await tools.outline_chat_history())["output"]
    assert outline["messages"][0]["has_media"] is True

    read = json.loads(await tools.read_chat_messages(ids=["m0"]))
    assert read["output"]["messages"][0]["media"] == [
        {"kind": "image", "id": "img1", "mime_type": "image/png", "format": "png"}
    ]
    # raw bytes must never leak into the tool result
    assert "RAWBYTES" not in json.dumps(read)


async def test_unbound_host_reports_unavailable() -> None:
    tools = ChatHistoryTools()  # host left as None
    result = json.loads(await tools.outline_chat_history())
    assert result["error"] == "chat history is not available"


def test_bind_host_on_raw_list() -> None:
    tools = ChatHistoryTools()
    host = object()
    ChatHistoryTools.bind_host([object(), tools], host)
    assert tools.host is host


def test_bind_host_resolves_factory_callable() -> None:
    tools = ChatHistoryTools()
    base = [tools]
    host = object()
    ChatHistoryTools.bind_host(lambda run_context=None: list(base), host)
    assert tools.host is host


def test_bind_host_noop_without_instance() -> None:
    ChatHistoryTools.bind_host([object()], object())
    ChatHistoryTools.bind_host(None, object())
