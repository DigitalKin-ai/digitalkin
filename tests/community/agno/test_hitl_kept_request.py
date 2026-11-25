"""A mission's request survives a first run that fails or never saves (DEV-982).

Runs a real agno ``Agent`` through :class:`AgnoHitlRunner` over a ``StorageDb`` backed by an in-memory
storage: a cut first run left ``outline_chat_history`` with 0 messages on magicbox (2026-11-25).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import pytest
from agno.agent import Agent
from agno.models.message import Message
from litellm.types.utils import Delta, ModelResponseStream, StreamingChoices

from digitalkin.community.agno.db import AgnoSession, StorageDb
from digitalkin.community.agno.hitl import AgnoHitlRunner
from digitalkin.community.agno.safe_litellm import SafeLiteLLM

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class _Storage:
    """In-memory storage: one validated record per (collection, id), like the storage service."""

    mission_id = "missions:m1"

    def __init__(self) -> None:
        self.records: dict[tuple[str, str], AgnoSession] = {}

    async def read(self, collection: str, record_id: str) -> Mock | None:
        data = self.records.get((collection, record_id))
        return None if data is None else Mock(data=data)

    async def upsert(self, collection: str, record_id: str, data: dict[str, Any]) -> None:
        self.records[collection, record_id] = AgnoSession.model_validate(data)


class _Client:
    """Stands in for litellm: one scripted stream per call, recording the messages it was sent."""

    def __init__(self, *finishes: str) -> None:
        self.finishes = list(finishes)
        self.requests: list[list[dict[str, Any]]] = []

    async def acompletion(self, **kwargs: Any) -> AsyncIterator[ModelResponseStream]:
        self.requests.append(kwargs["messages"])
        finish = self.finishes.pop(0)

        async def stream() -> AsyncIterator[ModelResponseStream]:  # ruff: ignore[unused-async] - an async generator
            yield ModelResponseStream(
                choices=[StreamingChoices(index=0, delta=Delta(content="Voici." if finish == "stop" else ""))]
            )
            yield ModelResponseStream(choices=[StreamingChoices(index=0, delta=Delta(), finish_reason=finish)])

        return stream()


def _runner(client: _Client, storage: _Storage) -> AgnoHitlRunner:
    agent = Agent(
        id="ada",
        model=SafeLiteLLM(id="claude-opus-5", client=client),
        db=StorageDb(storage=storage),  # type: ignore[arg-type]
        session_id="missions:m1",
        add_history_to_context=True,
        telemetry=False,
    )
    return AgnoHitlRunner(agent=agent, storage=storage)  # type: ignore[arg-type]


async def _send(_event: Any) -> None:  # ruff: ignore[unused-async] - the runner awaits its send callback
    return None


def _runs(storage: _Storage) -> list[dict[str, Any]]:
    return storage.records["agno_sessions", "missions:m1"].model_dump()["runs"]


@pytest.mark.regression
async def test_a_failed_first_run_leaves_its_request_for_the_retry() -> None:
    storage, client = _Storage(), _Client("content_filter", "stop")
    runner = _runner(client, storage)

    await runner.run("ORNITHORYNQUE-47 : écris le poème", send=_send, thread_id="t1")
    await runner.run("Tu as eu une erreur, réessaie.", send=_send, thread_id="t1")

    retry = " ".join(str(message.get("content")) for message in client.requests[1])
    assert "ORNITHORYNQUE-47" in retry, "the retry sees the request of the failed run"
    session = await runner._agent.db.get_session("missions:m1")  # type: ignore[union-attr]
    texts = [str(message.content) for message in session.get_messages()]  # type: ignore[union-attr]
    assert any("ORNITHORYNQUE-47" in text for text in texts), "chat history shows the request"


async def test_a_completed_run_releases_its_request() -> None:
    storage, client = _Storage(), _Client("stop")

    await _runner(client, storage).run("Bonjour", send=_send, thread_id="t1")

    runs = _runs(storage)
    assert not [run for run in runs if str(run["run_id"]).endswith(":request")]
    contents = [message["content"] for run in runs for message in run.get("messages") or []]
    assert contents.count("Bonjour") == 1, "the request is stored once, in its own run"


async def test_the_first_run_does_not_see_its_request_twice() -> None:
    storage, client = _Storage(), _Client("stop")

    await _runner(client, storage).run("Bonjour", send=_send, thread_id="t1")

    sent = [message for message in client.requests[0] if message.get("role") == "user"]
    assert len(sent) == 1


async def test_a_run_cut_before_its_end_keeps_its_request() -> None:
    """A crash or a cancel: the stream stops, the run is never saved."""
    storage, client = _Storage(), _Client("stop")
    runner = _runner(client, storage)
    original = runner._drive

    async def cut(**kwargs: Any) -> None:
        async for _ in kwargs["stream"]:
            msg = "process killed"
            raise RuntimeError(msg)

    runner._drive = cut  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        await runner.run("ORNITHORYNQUE-47", send=_send, thread_id="t1")
    runner._drive = original  # type: ignore[method-assign]

    runs = _runs(storage)
    assert [run["run_id"].endswith(":request") for run in runs] == [True]
    assert runs[0]["messages"][0]["content"] == "ORNITHORYNQUE-47"
    assert Message.model_validate(runs[0]["messages"][0]).role == "user"
