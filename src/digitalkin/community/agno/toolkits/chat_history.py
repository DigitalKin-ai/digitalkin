"""Toolkit for progressive chat-history access (outline first, then read by id).

Replaces Agno's built-in ``get_chat_history`` (which dumps every message in full)
with a two-step, token-cheap surface:

1. ``outline_chat_history`` — a metadata-only index (role, who, size, preview) so the
   agent can see *what* exists before loading anything.
2. ``read_chat_messages`` — fetch full content only for the message ids that matter.

The toolkit is leader-only: it is attached to the head agent / team leader and its
underlying ``aget_session_messages`` call skips team-member sub-conversations.

Note: the tools intentionally take NO ``run_context`` parameter. The session id is
captured at construction and the runtime Agent/Team is late-bound as ``host`` — so
every tool parameter is a plain builtin type, which keeps the LLM-facing JSON schema
correct under ``from __future__ import annotations``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.logger import logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from agno.media import Audio, File, Image, Video
    from agno.models.message import Message

    from digitalkin.models.module import ModuleContext


class ChatHistoryTools(DkToolkit):
    """Two-tool chat-history surface bound to a constructed Agent or Team.

    ``host`` is late-bound after the agent/team is created (the same pattern Agno uses
    for its own history tools, which close over the agent + session). Until bound, the
    tools report that history is unavailable rather than raising.
    """

    # Agno message roles -> the labels surfaced to the LLM.
    _ROLE_TO_LABEL: ClassVar[dict[str, str]] = {"user": "human", "assistant": "ai", "tool": "tool", "system": "system"}

    # Requested label -> Agno ``skip_roles`` (which roles to exclude). ``system`` is special-cased.
    _LABEL_TO_SKIP: ClassVar[dict[str, list[str]]] = {
        "human": ["system", "assistant", "tool"],
        "ai": ["system", "user", "tool"],
        "tool": ["system", "user", "assistant"],
        "system": [],
    }

    _MAX_READ_IDS: ClassVar[int] = 50
    _MAX_CONTENT_CHARS: ClassVar[int] = 20000

    def __init__(self, session_id: str | None = None, context: ModuleContext | None = None) -> None:
        """Register the outline + read tools.

        Args:
            session_id: The session whose history to read. Captured here (it is known at
                agent-construction time) so the tools need no ``run_context`` parameter.
            context: Module context; enables AG-UI notifications via the base toolkit.
        """
        super().__init__(
            name="chat_history_tools",
            tools=[self.outline_chat_history, self.read_chat_messages],
            context=context,
        )
        self._session_id = session_id
        # Late-bound to the runtime Agent (single mode) or Team (team mode).
        self.host: Any = None

    @staticmethod
    def bind_host(tools: list[Any] | Callable[..., list[Any]] | None, host: Any) -> None:
        """Late-bind the runtime Agent/Team into the ChatHistoryTools instance, if present.

        The toolkit needs a handle to call ``aget_session_messages``, which only exists once
        the agent/team is constructed. In team mode call this again with the ``Team`` so
        history reads target the team session rather than the bare head agent.

        Args:
            tools: The head tools — either the raw list or an
                :meth:`AguiTools.make_tools_factory` callable (calling it without a
                RunContext returns the base list). Binds the first ChatHistoryTools
                found; no-op if absent.
            host: The constructed Agent or Team to bind as the history source.
        """
        if callable(tools) and not isinstance(tools, list):
            tools = tools(None)
        if not isinstance(tools, list):
            return
        for tool in tools:
            if isinstance(tool, ChatHistoryTools):
                tool.host = host
                return

    async def outline_chat_history(
        self,
        role: str | None = None,
        first: int | None = None,
        last: int | None = None,
        offset: int = 0,
    ) -> str:
        """List the conversation as a cheap metadata index — call this FIRST.

        Returns one lightweight row per message (role, who, timestamp, size and a short
        preview) WITHOUT the full content, so it is safe to scan a long thread. Once you
        know which messages you need, call ``read_chat_messages`` with their ids to get
        full content. Prefer this over loading everything.

        Args:
            role: Filter by message type: "human", "ai", "tool", or "system".
                Omit to get all messages except the system prompt.
            first: Return only the first N messages (oldest), at most 200. Use this to reach
                the start of the conversation, e.g. the user's first request.
            last: Return only the last N messages (most recent), at most 200. Mutually
                exclusive with first.
            offset: Skip this many messages from the relevant end (for pagination).

        Returns:
            JSON string: {"total", "returned", "offset", "messages": [{"ord", "id", "role",
            "ts", "chars", "preview", ...}]} plus a "note" when the page was capped. "total"
            is the full count after filtering (null when ``last`` is used, since only the
            tail is loaded). Without ``first``/``last`` at most 50 messages are returned.
        """
        if role is not None and role not in self._LABEL_TO_SKIP:
            msg = f"invalid role '{role}'; use one of: human, ai, tool, system"
            return self._fail(msg, tool="outline_chat_history")

        offset = max(offset, 0)
        requested = first if first is not None else last
        page = 50 if requested is None else min(max(requested, 0), 200)
        tail = first is None and last is not None
        skip_roles = self._LABEL_TO_SKIP[role] if role is not None else ["system"]
        messages = await self._fetch(skip_roles, limit=offset + page if tail else None)
        if messages is None:
            return self._fail("chat history is not available", tool="outline_chat_history")
        if role == "system":
            messages = [m for m in messages if m.role == "system"]

        total = len(messages)
        start = max(total - offset - page, 0) if tail else offset
        end = max(total - offset, 0) if tail else offset + page
        rows = [self._index_row(start + i, m) for i, m in enumerate(messages[start:end])]
        output: dict[str, Any] = {
            "total": None if tail else total,
            "returned": len(rows),
            "offset": offset,
            "messages": rows,
        }
        if requested is not None and requested > page:
            output["note"] = f"'{'first' if first is not None else 'last'}' was capped at {page}; page with offset."
        elif requested is None and total > end:
            output["note"] = f"showing {page} of {total} messages; pass first/last/offset to page."
        if "note" in output:
            # TODO(validate): CHAT-HISTORY-CAP chat-history page/read/content caps apply
            logger.info("[VALIDATE CHAT-HISTORY-CAP] outline_chat_history: %s", output["note"])
        return self._ok(output, tool="outline_chat_history")

    async def read_chat_messages(
        self,
        ids: list[str],
        max_content_chars: int = 4000,
    ) -> str:
        """Fetch the full content of specific messages by id (from ``outline_chat_history``).

        Use the "id" values returned by ``outline_chat_history`` — they are stable even as
        the conversation grows (unlike the "ord" position). Long bodies are truncated to
        ``max_content_chars``; attached media is returned as a reference, never inlined.

        Args:
            ids: The message ids to read (at most 50), taken from an earlier
                ``outline_chat_history`` call.
            max_content_chars: Truncate each message body to this many characters
                (default 4000, at most 20000).

        Returns:
            JSON string: {"messages": [{"id", "role", "ts", "content", ...}], "missing": [...]}
            plus a "note" when a limit was capped. Any requested id that no longer exists is
            listed under "missing".
        """
        if not ids:
            return self._ok({"messages": [], "missing": []}, tool="read_chat_messages")

        notes: list[str] = []
        cap = self._MAX_READ_IDS
        if len(ids) > cap:
            # TODO(validate): CHAT-HISTORY-CAP chat-history page/read/content caps apply
            logger.info("[VALIDATE CHAT-HISTORY-CAP] read_chat_messages dropped ids beyond %d: %s", cap, ids[cap:])
            notes.append(f"only the first {cap} of {len(ids)} ids were read; request the rest separately.")
            ids = ids[:cap]
        if max_content_chars > self._MAX_CONTENT_CHARS:
            # TODO(validate): CHAT-HISTORY-CAP chat-history page/read/content caps apply
            logger.info(
                "[VALIDATE CHAT-HISTORY-CAP] max_content_chars %d capped at %d",
                max_content_chars,
                self._MAX_CONTENT_CHARS,
            )
            notes.append(f"max_content_chars was capped at {self._MAX_CONTENT_CHARS} (asked {max_content_chars}).")
            max_content_chars = self._MAX_CONTENT_CHARS

        messages = await self._fetch(skip_roles=[])
        if messages is None:
            return self._fail("chat history is not available", tool="read_chat_messages")

        by_id = {message.id: message for message in messages}
        out: list[dict[str, Any]] = []
        missing: list[str] = []
        for message_id in ids:
            message = by_id.get(message_id)
            if message is None:
                missing.append(message_id)
            else:
                out.append(self._full_row(message, max_content_chars))

        output: dict[str, Any] = {"messages": out, "missing": missing}
        if notes:
            output["note"] = " ".join(notes)
        return self._ok(output, tool="read_chat_messages")

    async def _fetch(self, skip_roles: list[str], limit: int | None = None) -> list[Message] | None:
        """Load session messages via the bound agent/team, or None if unavailable.

        Args:
            skip_roles: Roles to exclude (passed to Agno's ``aget_session_messages``).
            limit: Keep only the latest N messages; None loads them all.

        Returns:
            The deduplicated session messages, or None if no host is bound or the call fails.
        """
        if self.host is None:
            logger.warning("ChatHistoryTools called before host was bound")
            return None
        try:
            return await self.host.aget_session_messages(
                session_id=self._session_id,
                limit=limit,
                skip_roles=skip_roles,
                skip_history_messages=True,
            )
        except Exception as error:
            logger.warning("ChatHistoryTools: failed to load session messages: %s", error)
            return None

    def _index_row(self, ordinal: int, message: Message) -> dict[str, Any]:
        """Build a metadata-only index row for one message.

        Args:
            ordinal: Position of the message in the filtered list (display-only).
            message: The Agno message.

        Returns:
            A compact dict with role, id, timestamp, size and a short preview.
        """
        content = message.get_content_string() or ""
        row: dict[str, Any] = {
            "ord": ordinal,
            "id": message.id,
            "role": self._ROLE_TO_LABEL.get(message.role, message.role),
            "ts": message.created_at,
            "chars": len(content),
            "preview": content[:120],
        }
        if message.images or message.files or message.videos or message.audio:
            row["has_media"] = True
        if message.from_history:
            row["from_history"] = True
        if message.role == "tool":
            row["name"] = message.tool_name
            if message.tool_call_error:
                row["error"] = True
        return row

    def _full_row(self, message: Message, max_content_chars: int) -> dict[str, Any]:
        """Build a full-content row for one message, truncating the body if needed.

        Args:
            message: The Agno message.
            max_content_chars: Maximum body length before truncation.

        Returns:
            A dict with the (possibly truncated) content plus media references.
        """
        content = message.get_content_string() or ""
        truncated = len(content) > max_content_chars
        if truncated:
            content = content[:max_content_chars] + " […truncated]"

        row: dict[str, Any] = {
            "id": message.id,
            "role": self._ROLE_TO_LABEL.get(message.role, message.role),
            "ts": message.created_at,
            "content": content,
        }
        if truncated:
            row["truncated"] = True
        if message.role == "tool":
            row["name"] = message.tool_name
            if message.tool_call_error:
                row["error"] = True
        media = self._media_refs(message)
        if media:
            row["media"] = media
        return row

    @staticmethod
    def _media_refs(message: Message) -> list[dict[str, Any]]:
        """Build reference descriptors for attached media — never the raw bytes.

        Args:
            message: The Agno message.

        Returns:
            A list of {"kind", "id", "mime_type", "format"} descriptors.
        """
        groups: tuple[tuple[str, Any], ...] = (
            ("image", message.images),
            ("audio", message.audio),
            ("video", message.videos),
            ("file", message.files),
        )
        refs: list[dict[str, Any]] = []
        for kind, items in groups:
            for item in items or []:
                media_item: Image | Audio | Video | File = item
                refs.append({
                    "kind": kind,
                    "id": media_item.id,
                    "mime_type": media_item.mime_type,
                    "format": media_item.format,
                })
        return refs
