"""Agno ``AsyncBaseDb`` over a DigitalKin ``StorageStrategy``: sessions, their runs, compaction records."""

from __future__ import annotations

import asyncio
import time
import weakref
from typing import TYPE_CHECKING, Any, ClassVar

from agno.db.base import AsyncBaseDb, SessionType
from agno.db.utils import deserialize_run, deserialize_session, get_run_type
from agno.run.base import RunStatus
from pydantic import BaseModel, ConfigDict
from typing_extensions import override

from digitalkin.logger import logger

if TYPE_CHECKING:
    from agno.run.agent import RunOutput
    from agno.run.team import TeamRunOutput
    from agno.run.workflow import WorkflowRunOutput
    from agno.session import Session

    from digitalkin.services.storage.storage_strategy import StorageStrategy


class AgnoSession(BaseModel):
    """One Agno session (``AgentSession`` / ``TeamSession`` / ``WorkflowSession.to_dict()``), runs inline."""

    model_config = ConfigDict(extra="allow")

    session_id: str
    agent_id: str | None = None
    team_id: str | None = None
    user_id: str | None = None
    workflow_id: str | None = None
    session_data: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    agent_data: dict[str, Any] | None = None
    runs: list[dict[str, Any]] | None = None
    summary: dict[str, Any] | None = None
    created_at: int | None = None
    updated_at: int | None = None
    session_type: str | None = None


class AgnoCompaction(BaseModel):
    """The compaction in force for one session: the summary replacing its older history.

    One record per session, overwritten by each fold (a fold carries the previous summary forward).
    Mirrors ``agno.compaction.CompactionRecord``, plus the session's ``TokenCalibration`` samples,
    which can be stored before any fold (empty summary).
    """

    model_config = ConfigDict(extra="allow")

    session_id: str
    summary: str = ""
    token_samples: list[list[int]] | None = None
    first_kept_message_id: str | None = None
    messages_compacted: int = 0
    id: str | None = None
    run_id: str | None = None
    tokens_before: int | None = None
    tokens_after: int | None = None
    created_at: int | None = None


AGNO_STORAGE_CONFIG: dict[str, type[BaseModel]] = {  # dk-allow-global (storage config fragment)
    "agno_sessions": AgnoSession,
    "agno_compactions": AgnoCompaction,
}


class StorageDb(AsyncBaseDb):
    """Agno async db persisting sessions and compaction records through a ``StorageStrategy``.

    Register :data:`AGNO_STORAGE_CONFIG` on the module's storage service (default prefix ``agno``).

    - Sessions: one ``<prefix>_sessions`` record each, runs inline. ``upsert_run`` keeps the base
      ``NotImplementedError``, which Agno treats as "persist runs through ``upsert_session``";
      ``get_run`` / ``get_runs`` read them back from the session records. On save, every tool result
      older than the five most recent is cut to ``tool_result_chars``.
    - History safety: a session that cannot be read raises instead of reading as absent (Agno would
      start an empty session and the next save would overwrite the stored history), and a save merges
      its runs into the stored ones by ``run_id`` under a process-wide per-session lock, so a stored
      run is never dropped and a concurrent turn on the same session is not lost.
    - Compaction: one ``<prefix>_compactions`` record per session, read and written by
      ``HistoryCompactor``. Async and not named after Agno's sync ``upsert_compaction`` contract on
      purpose: Agno enables its own Agent-only compaction for a db implementing that contract.
    - Memories, metrics, knowledge, evals, learnings, traces and spans have no collection: their
      methods are stubs that store nothing and return an empty result, ``None`` or ``False``.

    Attributes:
        storage: The module's storage strategy.
        sessions_collection: Collection holding the sessions.
        compactions_collection: Collection holding the compaction records.
        tool_result_chars: Characters an old tool result keeps when a session is saved.
    """

    # One lock per stored session, shared by every StorageDb of the process: storage strategies (and
    # their record locks) are built per task, so two tasks on one mission would not exclude each other.
    _session_locks: ClassVar[weakref.WeakValueDictionary[str, asyncio.Lock]] = weakref.WeakValueDictionary()

    def __init__(
        self,
        storage: StorageStrategy,
        collection_prefix: str = "agno",
        tool_result_chars: int = 50_000,
        **kwargs: Any,
    ) -> None:
        """Bind the db to a storage strategy.

        Args:
            storage: The module's storage strategy.
            collection_prefix: Prefix of the collection names.
            tool_result_chars: Characters an old tool result keeps when a session is saved.
            **kwargs: Forwarded to ``AsyncBaseDb``.
        """
        super().__init__(**kwargs)
        self.storage = storage
        self.sessions_collection = f"{collection_prefix}_sessions"
        self.compactions_collection = f"{collection_prefix}_compactions"
        self.tool_result_chars = tool_result_chars

    @staticmethod
    def _patch_null_fields(runs: list[dict[str, Any]]) -> None:
        """Restore the keys Agno's ``UserInputField.from_dict`` indexes, which storage drops when null.

        Args:
            runs: Stored runs, patched in place.
        """
        for run in runs:
            schemas = [tool.get("user_input_schema") or [] for tool in run.get("tools") or []]
            for req in run.get("requirements") or []:
                schemas.extend((
                    req.get("user_input_schema") or [],
                    (req.get("tool_execution") or {}).get("user_input_schema") or [],
                ))
            for schema in schemas:
                for field in schema:
                    field.setdefault("value", None)
                    field.setdefault("description", None)

    @staticmethod
    def _deserialize_run(run: dict[str, Any]) -> RunOutput | TeamRunOutput | WorkflowRunOutput:
        """Rebuild a run object from its stored dict.

        Args:
            run: A run as stored inline in its session (patched in place).

        Returns:
            The agent, team or workflow run output.
        """
        StorageDb._patch_null_fields([run])
        return deserialize_run(get_run_type(run), run)

    async def _list_sessions(self) -> list[dict[str, Any]]:
        """Read every stored session, page by page (the service returns at most 100 records a call).

        Returns:
            The session dicts.
        """
        sessions: list[dict[str, Any]] = []
        while True:
            page = await self.storage.list(self.sessions_collection, limit=100, offset=len(sessions))
            sessions.extend(record.data.model_dump() for record in page)
            if len(page) < 100:  # ruff: ignore[magic-value-comparison]
                return sessions

    # --- Sessions ---
    @override
    async def delete_session(self, session_id: str, user_id: str | None = None) -> bool:
        try:
            await self.storage.remove(self.sessions_collection, session_id)
        except Exception:
            logger.exception("Deleting session %s failed", session_id)
            return False
        return True

    @override
    async def delete_sessions(self, session_ids: list[str], user_id: str | None = None) -> None:
        for session_id in session_ids:
            await self.delete_session(session_id)

    @override
    async def get_session(
        self,
        session_id: str,
        session_type: SessionType | None = None,
        user_id: str | None = None,
        deserialize: bool | None = True,
        runs_limit: int | None = None,
    ) -> Session | dict[str, Any] | None:
        """Read a session; ``None`` only when absent or owned by another user.

        Args:
            session_id: The session to read.
            session_type: Type to deserialize as; the stored type when omitted.
            user_id: When set, a session owned by another user reads as absent.
            deserialize: Return a ``Session`` rather than the stored dict.
            runs_limit: Keep only the most recent N runs. The session is one record, so the trim
                happens after the read.

        Returns:
            The session, its dict, or None.

        Raises:
            StorageServiceError: When the storage read failed. Agno lets it fail the run: reading it
                as absent would start an empty session whose save overwrites the stored history.
            Exception: When the stored session cannot be deserialized, for the same reason.
        """
        record = await self.storage.read(self.sessions_collection, session_id)
        if record is None:
            return None
        data = record.data.model_dump()
        if user_id is not None and data.get("user_id") != user_id:
            return None
        if runs_limit is not None and runs_limit >= 0:
            data["runs"] = (data.get("runs") or [])[-runs_limit:] if runs_limit else []
        if not deserialize:
            return data
        try:
            self._patch_null_fields(data.get("runs") or [])
            return deserialize_session(session_type or SessionType(data.get("session_type") or SessionType.AGENT), data)
        except Exception:
            logger.exception("Session %s is stored but cannot be deserialized", session_id)
            raise

    @override
    async def get_sessions(
        self,
        session_type: SessionType | None = None,
        user_id: str | None = None,
        component_id: str | None = None,
        session_name: str | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        limit: int | None = None,
        page: int | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        deserialize: bool | None = True,
        include_runs: bool = True,
    ) -> list[Session] | tuple[list[dict[str, Any]], int]:
        """List the sessions of one type; ``session_name`` and the timestamps are not filtered on.

        Args:
            session_type: Type of the sessions to list.
            user_id: Keep only this user's sessions.
            component_id: Keep only the sessions of this agent, team or workflow.
            session_name: Ignored.
            start_timestamp: Ignored.
            end_timestamp: Ignored.
            limit: Page size.
            page: Page number, from 1.
            sort_by: Field to sort on.
            sort_order: ``"desc"`` for descending.
            deserialize: Return ``Session`` objects rather than ``(dicts, total)``.
            include_runs: Keep the runs in the returned sessions.

        Returns:
            The sessions, or their dicts and the total before pagination.

        Raises:
            StorageServiceError: When the storage listing failed.
        """
        type_value = session_type.value if isinstance(session_type, SessionType) else session_type
        component_key = f"{type_value}_id" if component_id is not None and type_value is not None else None
        sessions = []
        for data in await self._list_sessions():
            if (
                data.get("session_type") != type_value
                or (user_id is not None and data.get("user_id") != user_id)
                or (component_key is not None and data.get(component_key) != component_id)
            ):
                continue
            if not include_runs:
                data.pop("runs", None)
            sessions.append(data)
        total = len(sessions)
        if sort_by:
            sessions.sort(key=lambda s: s.get(sort_by, 0), reverse=sort_order == "desc")
        if limit is not None:
            start = (page - 1) * limit if page else 0
            sessions = sessions[start : start + limit]
        if not deserialize:
            return sessions, total
        return [s for data in sessions if (s := deserialize_session(session_type, data)) is not None]

    @override
    async def rename_session(
        self,
        session_id: str,
        session_type: SessionType | None,
        session_name: str,
        user_id: str | None = None,
        deserialize: bool | None = True,
    ) -> Session | dict[str, Any] | None:
        """Set a session's name and ``updated_at``.

        Args:
            session_id: The session to rename.
            session_type: Type to deserialize as; detected from the record when omitted.
            session_name: The new name.
            user_id: Ignored.
            deserialize: Return a ``Session`` rather than the stored dict.

        Returns:
            The renamed session, or None when it does not exist.
        """
        session = await self.get_session(session_id, session_type, deserialize=False)
        if not isinstance(session, dict):
            return None
        session["session_name"] = session_name
        session["updated_at"] = int(time.time())
        await self.storage.update(self.sessions_collection, session_id, session)
        return deserialize_session(session_type, session) if deserialize else session

    @override
    async def upsert_session(  # ruff: ignore[complex-structure]
        self,
        session: Session,
        deserialize: bool | None = True,
    ) -> Session | dict[str, Any] | None:
        """Write a session, its runs merged into the stored ones, after cutting its old tool results.

        The stored session is read again under a process-wide per-session lock and the runs are
        merged by ``run_id``: a session Agno rebuilt with fewer runs, or a
        concurrent turn's save, never drops a stored run. Every tool result older than the five most
        recent keeps its first ``tool_result_chars`` characters and a note of its original length. A
        result already cut is left alone.

        Args:
            session: The session to write.
            deserialize: Return the session rather than the stored dict.

        Returns:
            The session or its dict; None when the write failed (logged; Agno would swallow a raise).
        """
        data = session.to_dict()
        session_id = data.get("session_id") or session.session_id
        if not session_id:
            logger.error("upsert_session: session_id is required")
            return None
        data["session_id"] = session_id
        key = f"{self.storage.mission_id}|{self.sessions_collection}:{session_id}"
        lock = self._session_locks.get(key)
        if lock is None:
            lock = self._session_locks[key] = asyncio.Lock()
        try:  # ruff: ignore[too-many-statements-in-try-clause]
            async with lock:
                stored = await self.storage.read(self.sessions_collection, session_id)
                runs = (stored.data.model_dump().get("runs") or []) if stored is not None else []
                # Merge by run_id: a stored run the save lacks stays in place, an incoming run replaces
                # its stored copy (a resumed run keeps its position), a new run is appended.
                positions = {run.get("run_id"): i for i, run in enumerate(runs) if run.get("run_id")}
                incoming = data.get("runs") or []
                incoming_ids = {run.get("run_id") for run in incoming}
                kept = [run.get("run_id") for run in runs if run.get("run_id") not in incoming_ids]
                for run in incoming:
                    position = positions.get(run.get("run_id")) if run.get("run_id") else None
                    if position is None:
                        runs.append(run)
                    else:
                        runs[position] = run
                if kept:
                    # TODO(validate): SESSION-MERGE-KEEPS-RUNS a save never drops a stored run
                    logger.warning(
                        "[VALIDATE SESSION-MERGE-KEEPS-RUNS] Session %s: save carried %d run(s), kept %d stored "
                        "run(s) it lacked: %s",
                        session_id,
                        len(incoming),
                        len(kept),
                        kept,
                    )
                data["runs"] = runs
                seen = 0
                for run in reversed(runs):
                    for msg in reversed(run.get("messages") or []):
                        if msg.get("role") != "tool":
                            continue
                        seen += 1
                        content = msg.get("content")
                        if (
                            seen <= 5  # ruff: ignore[magic-value-comparison]
                            or not isinstance(content, str)
                            or len(content) <= self.tool_result_chars
                            or content.startswith("\n\n[... truncated", self.tool_result_chars)
                        ):
                            continue
                        msg["content"] = (
                            f"{content[: self.tool_result_chars]}\n\n"
                            f"[... truncated, original tool result was {len(content)} chars]"
                        )
                        # TODO(validate): TOOL-RESULT-RETRUNCATE an old tool result is cut once, not on every save
                        logger.info(
                            "[VALIDATE TOOL-RESULT-RETRUNCATE] Session %s: old tool result %s (%s) cut from %d to %d"
                            " chars, dropped (first 100 chars): %.100s",
                            session_id,
                            msg.get("tool_call_id"),
                            msg.get("tool_name"),
                            len(content),
                            self.tool_result_chars,
                            content[self.tool_result_chars :],
                        )
                await self.storage.upsert(self.sessions_collection, session_id, data)
        except Exception:
            logger.exception("Writing session %s failed", session_id)
            return None
        logger.debug("Session %s saved with %d runs", session_id, len(data["runs"]))
        return session if deserialize else data

    # --- Compaction ---
    async def get_compaction_record(self, session_id: str) -> dict[str, Any] | None:
        """Read the compaction in force for a session.

        Args:
            session_id: The session whose record to read.

        Returns:
            The record, or None when the session was never compacted.

        Raises:
            StorageServiceError: When the read failed. Read as absent, the next write would replace
                the stored summary with a record holding none.
        """
        record = await self.storage.read(self.compactions_collection, session_id)
        return None if record is None else record.data.model_dump()

    async def upsert_compaction_record(self, session_id: str, record: dict[str, Any]) -> bool:
        """Replace the compaction in force for a session.

        Args:
            session_id: The session the record belongs to.
            record: ``CompactionRecord.to_dict()``, plus ``token_samples``.

        Returns:
            True when written. A failure costs a repeated fold on the next run, never the run, so it
            is logged rather than raised.
        """
        try:
            await self.storage.upsert(self.compactions_collection, session_id, {**record, "session_id": session_id})
        except Exception:
            logger.warning("Writing the compaction record of session %s failed", session_id, exc_info=True)
            return False
        return True

    # --- Runs (inline in the session records) ---
    async def _session_runs(self, session_id: str | None) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        """List (session, run) dict pairs, from one session or from all of them.

        Args:
            session_id: Restrict to this session; None scans every session.

        Returns:
            One pair per stored run, in session order.
        """
        if session_id is not None:
            session = await self.get_session(session_id, deserialize=False)
            sessions = [session] if isinstance(session, dict) else []
        else:
            sessions = await self._list_sessions()
        return [(session, run) for session in sessions for run in session.get("runs") or [] if isinstance(run, dict)]

    @override
    async def get_run(
        self,
        run_id: str,
        deserialize: bool | None = True,
    ) -> RunOutput | TeamRunOutput | WorkflowRunOutput | dict[str, Any] | None:
        """Find a run in any stored session.

        Args:
            run_id: The run to find.
            deserialize: Return a run object rather than the stored dict.

        Returns:
            The run, its dict, or None when no session holds it.
        """
        for _, run in await self._session_runs(None):
            if run.get("run_id") == run_id:
                return self._deserialize_run(run) if deserialize else run
        return None

    @override
    async def get_runs(
        self,
        session_id: str | None = None,
        user_id: str | None = None,
        agent_id: str | None = None,
        team_id: str | None = None,
        workflow_id: str | None = None,
        status: RunStatus | None = None,
        limit: int | None = None,
        page: int | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        deserialize: bool | None = True,
    ) -> list[RunOutput | TeamRunOutput | WorkflowRunOutput] | tuple[list[dict[str, Any]], int]:
        """List the stored runs matching every given filter.

        Args:
            session_id: Read only this session.
            user_id: Keep the runs of this user's sessions.
            agent_id: Keep this agent's runs.
            team_id: Keep this team's runs.
            workflow_id: Keep this workflow's runs.
            status: Keep the runs in this status.
            limit: Page size.
            page: Page number, from 1.
            sort_by: Field to sort on; session order when omitted.
            sort_order: ``"desc"`` for descending.
            deserialize: Return run objects rather than ``(dicts, total)``.

        Returns:
            The runs, or their dicts and the total before pagination.
        """
        try:  # ruff: ignore[too-many-statements-in-try-clause]
            expected_status = status.value if isinstance(status, RunStatus) else status
            runs = [
                run
                for session, run in await self._session_runs(session_id)
                if (user_id is None or session.get("user_id") == user_id)
                and (agent_id is None or run.get("agent_id") == agent_id)
                and (team_id is None or run.get("team_id") == team_id)
                and (workflow_id is None or run.get("workflow_id") == workflow_id)
                and (expected_status is None or run.get("status") == expected_status)
            ]
            total = len(runs)
            if sort_by:
                runs.sort(key=lambda r: r.get(sort_by, 0) or 0, reverse=sort_order == "desc")
            if limit is not None:
                start = (page - 1) * limit if page else 0
                runs = runs[start : start + limit]
            if not deserialize:
                return runs, total
            return [self._deserialize_run(run) for run in runs]
        except Exception:
            logger.exception("Listing runs failed")
            return [] if deserialize else ([], 0)

    # --- Surfaces without a collection (see the class docstring) ---
    @override
    async def table_exists(self, table_name: str) -> bool:
        return True

    @override
    async def get_latest_schema_version(self, table_name: str) -> str:
        return "2.0.0"

    @override
    async def delete_learning(self, id: str) -> bool:
        return False

    async def _none(self, *_args: Any, **_kwargs: Any) -> None:
        """Store nothing, find nothing."""

    async def _no_items(self, *_args: Any, **_kwargs: Any) -> list[Any]:  # ruff: ignore[no-self-use]
        """Find nothing.

        Returns:
            An empty list.
        """
        return []

    async def _no_page(self, *_args: Any, **_kwargs: Any) -> tuple[list[Any], int]:  # ruff: ignore[no-self-use]
        """Find nothing, paginated.

        Returns:
            An empty page and a zero total.
        """
        return [], 0

    async def _no_rows(  # ruff: ignore[no-self-use]
        self,
        *_args: Any,
        deserialize: bool | None = True,
        **_kwargs: Any,
    ) -> list[Any] | tuple[list[Any], int]:
        """Find nothing, as objects or as a page of dicts.

        Args:
            *_args: Ignored.
            deserialize: Return an empty list rather than an empty page.
            **_kwargs: Ignored.

        Returns:
            An empty list, or an empty page and a zero total.
        """
        return [] if deserialize else ([], 0)

    upsert_schema_version = clear_memories = delete_user_memory = delete_user_memories = get_user_memory = _none
    upsert_user_memory = calculate_metrics = delete_knowledge_content = get_knowledge_content = _none
    upsert_knowledge_content = create_eval_run = delete_eval_runs = get_eval_run = rename_eval_run = _none
    upsert_trace = get_trace = create_span = create_spans = get_span = get_learning = upsert_learning = _none
    get_all_memory_topics = get_spans = get_learnings = _no_items
    get_user_memory_stats = get_metrics = get_knowledge_contents = get_traces = get_trace_stats = _no_page
    get_user_memories = get_eval_runs = _no_rows
