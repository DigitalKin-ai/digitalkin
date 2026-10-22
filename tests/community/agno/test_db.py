"""StorageDb: agno's async db contract over a DigitalKin StorageStrategy."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

pytest.importorskip("agno", reason="optional agno dependency not installed")

from agno.compaction.archive import stores_compaction_records
from agno.db.base import AsyncBaseDb, SessionType
from agno.models.message import Message
from agno.run.agent import RunOutput
from agno.run.base import RunStatus
from agno.session.agent import AgentSession

from digitalkin.community.agno import db as db_module
from digitalkin.community.agno.db import AGNO_STORAGE_CONFIG, AgnoCompaction, AgnoSession, StorageDb
from digitalkin.community.agno.hitl import HITL_STORAGE_CONFIG
from digitalkin.services.storage.default_storage import DefaultStorage
from digitalkin.services.storage.exceptions import StorageServiceError


def _record(data: dict[str, Any]) -> Mock:
    """A storage record whose payload is validated like the storage service does."""
    model = AgnoCompaction if "summary" in data and isinstance(data["summary"], str) else AgnoSession
    return Mock(data=model.model_validate(data))


def _session(sid: str, user_id: str | None, runs: list[dict[str, Any]]) -> dict[str, Any]:
    return {"session_id": sid, "session_type": "agent", "user_id": user_id, "runs": runs}


def _runs(count: int) -> list[dict[str, Any]]:
    # ``agent_id`` is what AgentSession.from_dict keys on to rebuild a run as a RunOutput.
    return [{"run_id": f"r{i}", "agent_id": "a1", "session_id": "s1"} for i in range(count)]


def _db(runs: list[dict[str, Any]]) -> StorageDb:
    """A StorageDb over a stub storage holding one session with ``runs``."""
    storage = Mock()
    storage.read = AsyncMock(return_value=_record(_session("s1", None, runs)))
    return StorageDb(storage=storage)


def _write_storage(stored: dict[str, Any] | None = None) -> Mock:
    """A stub storage for saves: ``stored`` is the session currently stored (None: none yet)."""
    return Mock(
        mission_id="missions:m1",
        upsert=AsyncMock(),
        read=AsyncMock(return_value=None if stored is None else _record(stored)),
    )


def _listed_db(sessions: list[dict[str, Any]]) -> StorageDb:
    """A StorageDb over a stub storage listing ``sessions`` and reading them by id."""
    storage = Mock()
    storage.list = AsyncMock(return_value=[_record(s) for s in sessions])
    storage.read = AsyncMock(
        side_effect=lambda _collection, sid: next((_record(s) for s in sessions if s["session_id"] == sid), None)
    )
    return StorageDb(storage=storage)


class TestStorageConfig:
    def test_registers_both_collections(self) -> None:
        assert AGNO_STORAGE_CONFIG == {"agno_sessions": AgnoSession, "agno_compactions": AgnoCompaction}

    def test_merges_with_the_hitl_collections(self) -> None:
        config = {**HITL_STORAGE_CONFIG, **AGNO_STORAGE_CONFIG}
        assert set(config) == {*HITL_STORAGE_CONFIG, "agno_sessions", "agno_compactions"}

    def test_collections_follow_the_prefix(self) -> None:
        db = StorageDb(storage=Mock(), collection_prefix="kin")
        assert (db.sessions_collection, db.compactions_collection) == ("kin_sessions", "kin_compactions")


class TestGetSessionRunsLimit:
    """``runs_limit`` is agno 3's db contract: every adapter must accept it."""

    async def test_omitted_returns_every_run(self) -> None:
        data = await _db(_runs(5)).get_session("s1", deserialize=False)
        assert len(data["runs"]) == 5

    async def test_keeps_the_most_recent_n(self) -> None:
        data = await _db(_runs(5)).get_session("s1", deserialize=False, runs_limit=2)
        assert [run["run_id"] for run in data["runs"]] == ["r3", "r4"]

    async def test_zero_drops_all_runs(self) -> None:
        # `[-0:]` would return the whole list, so zero is special-cased.
        data = await _db(_runs(3)).get_session("s1", deserialize=False, runs_limit=0)
        assert data["runs"] == []

    async def test_limit_above_length_is_safe(self) -> None:
        data = await _db(_runs(1)).get_session("s1", deserialize=False, runs_limit=99)
        assert len(data["runs"]) == 1

    async def test_applies_on_the_deserialized_path_too(self) -> None:
        unlimited = await _db(_runs(5)).get_session("s1")
        limited = await _db(_runs(5)).get_session("s1", runs_limit=1)
        assert isinstance(unlimited, AgentSession)
        assert isinstance(limited, AgentSession)
        assert len(unlimited.runs or []) == 5
        assert len(limited.runs or []) == 1


class TestGetSession:
    async def test_missing_session(self) -> None:
        storage = Mock(read=AsyncMock(return_value=None))
        assert await StorageDb(storage=storage).get_session("s1") is None

    async def test_another_users_session_reads_as_absent(self) -> None:
        db = _listed_db([_session("s1", "u1", [])])
        assert await db.get_session("s1", user_id="u2") is None
        assert await db.get_session("s1", user_id="u1") is not None

    async def test_a_read_failure_raises_instead_of_reading_as_absent(self) -> None:
        """Regression: a failed read read as absent, Agno started an empty session and its save wiped the history."""
        storage = Mock(read=AsyncMock(side_effect=StorageServiceError("GetRecord failed: DEADLINE_EXCEEDED")))
        with pytest.raises(StorageServiceError):
            await StorageDb(storage=storage).get_session("s1")

    async def test_an_undeserializable_session_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A stored session Agno cannot rebuild must not read as absent (its save would overwrite it)."""

        def corrupt(*_: Any) -> None:
            msg = "corrupt run"
            raise ValueError(msg)

        monkeypatch.setattr(db_module, "deserialize_session", corrupt)
        with pytest.raises(ValueError, match="corrupt run"):
            await _db(_runs(1)).get_session("s1")

    def test_restores_user_input_keys_storage_dropped(self) -> None:
        runs = [
            {
                "tools": [{"user_input_schema": [{"name": "a"}]}],
                "requirements": [
                    {"user_input_schema": [{"name": "b"}], "tool_execution": {"user_input_schema": [{"name": "c"}]}}
                ],
            }
        ]

        StorageDb._patch_null_fields(runs)

        fields = [
            runs[0]["tools"][0]["user_input_schema"][0],
            runs[0]["requirements"][0]["user_input_schema"][0],
            runs[0]["requirements"][0]["tool_execution"]["user_input_schema"][0],
        ]
        assert all(field["value"] is None and field["description"] is None for field in fields)

    async def test_patched_session_deserializes(self) -> None:
        tool = {"tool_call_id": "c1", "tool_name": "ask", "user_input_schema": [{"name": "x", "field_type": "str"}]}
        session = await _db([{**_runs(1)[0], "tools": [tool]}]).get_session("s1")
        assert isinstance(session, AgentSession)
        assert session.runs is not None


class TestAgno3Contract:
    def test_implements_every_abstract_method(self) -> None:
        assert not StorageDb.__abstractmethods__
        assert isinstance(StorageDb(storage=Mock()), AsyncBaseDb)

    async def test_upsert_run_defers_to_upsert_session(self) -> None:
        # Agno skips the per-run write on NotImplementedError; runs persist inline via upsert_session.
        with pytest.raises(NotImplementedError):
            await StorageDb(storage=Mock()).upsert_run(run={"run_id": "r1"}, session_id="s1")

    async def test_get_sessions_without_runs(self) -> None:
        db = _listed_db([_session("s1", None, _runs(2))])
        with_runs, _ = await db.get_sessions(session_type=SessionType.AGENT, deserialize=False)
        without_runs, _ = await db.get_sessions(session_type=SessionType.AGENT, deserialize=False, include_runs=False)
        assert len(with_runs[0]["runs"]) == 2
        assert "runs" not in without_runs[0]

    async def test_get_sessions_filters_and_paginates(self) -> None:
        db = _listed_db([
            {**_session("s1", "u1", []), "agent_id": "a1", "created_at": 2},
            {**_session("s2", "u1", []), "agent_id": "a2", "created_at": 1},
            {**_session("s3", "u2", []), "agent_id": "a1", "created_at": 3},
            {**_session("t1", "u1", []), "session_type": "team"},
        ])

        rows, total = await db.get_sessions(SessionType.AGENT, user_id="u1", sort_by="created_at", deserialize=False)
        assert [row["session_id"] for row in rows] == ["s2", "s1"]
        assert total == 2
        assert [s.session_id for s in await db.get_sessions(SessionType.AGENT, component_id="a1")] == ["s1", "s3"]
        page, total = await db.get_sessions(
            SessionType.AGENT, sort_by="created_at", sort_order="desc", limit=1, page=2, deserialize=False
        )
        assert [row["session_id"] for row in page] == ["s1"]
        assert total == 3

    async def test_agnos_own_compaction_stays_off(self) -> None:
        """Agno enables its Agent-only compaction for a db with the sync record contract."""
        assert stores_compaction_records(StorageDb(storage=Mock())) is False

    @pytest.mark.parametrize(
        ("call", "expected"),
        [
            (lambda db: db.get_user_memories(), []),
            (lambda db: db.get_user_memories(deserialize=False), ([], 0)),
            (lambda db: db.get_eval_runs(deserialize=False), ([], 0)),
            (lambda db: db.get_user_memory("m1"), None),
            (lambda db: db.upsert_user_memory(Mock()), None),
            (lambda db: db.get_all_memory_topics(), []),
            (lambda db: db.get_traces(), ([], 0)),
            (lambda db: db.get_metrics(), ([], 0)),
            (lambda db: db.get_learnings(), []),
            (lambda db: db.delete_learning("l1"), False),
            (lambda db: db.table_exists("agno_sessions"), True),
            (lambda db: db.get_latest_schema_version("agno_sessions"), "2.0.0"),
        ],
    )
    async def test_surfaces_without_a_collection_store_nothing(self, call: Any, expected: Any) -> None:
        storage = Mock()
        assert await call(StorageDb(storage=storage)) == expected
        assert storage.mock_calls == []


class TestUpsertSession:
    @staticmethod
    def _tool_runs(results: list[str]) -> list[dict[str, Any]]:
        """One run per tool result, oldest first."""
        return [
            {
                "run_id": f"r{i}",
                "agent_id": "a1",
                "session_id": "s1",
                "messages": [{"role": "tool", "tool_call_id": f"c{i}", "content": content}],
            }
            for i, content in enumerate(results)
        ]

    @staticmethod
    def _session_object(runs: list[dict[str, Any]]) -> AgentSession:
        session = AgentSession.from_dict({"session_id": "s1", "agent_id": "a1", "runs": runs})
        assert session is not None
        return session

    async def test_writes_through_the_atomic_upsert(self) -> None:
        storage = _write_storage()
        session = self._session_object(_runs(2))

        assert await StorageDb(storage=storage).upsert_session(session) is session

        collection, session_id, data = storage.upsert.await_args.args
        assert (collection, session_id, data["session_id"]) == ("agno_sessions", "s1", "s1")
        assert len(data["runs"]) == 2

    async def test_returns_the_dict_without_deserialize(self) -> None:
        storage = _write_storage()
        data = await StorageDb(storage=storage).upsert_session(self._session_object([]), deserialize=False)
        assert isinstance(data, dict)
        assert data["session_id"] == "s1"

    async def test_a_write_failure_returns_none(self) -> None:
        storage = _write_storage()
        storage.upsert.side_effect = RuntimeError("storage down")
        assert await StorageDb(storage=storage).upsert_session(self._session_object([])) is None

    async def test_cuts_tool_results_older_than_the_five_most_recent(self) -> None:
        storage = _write_storage()
        results = ["x" * 50 for _ in range(7)]
        db = StorageDb(storage=storage, tool_result_chars=10)

        await db.upsert_session(self._session_object(self._tool_runs(results)))

        contents = [run["messages"][0]["content"] for run in storage.upsert.await_args.args[2]["runs"]]
        assert contents[2:] == results[2:], "the five most recent results are kept whole"
        assert contents[:2] == ["x" * 10 + "\n\n[... truncated, original tool result was 50 chars]"] * 2

    async def test_a_cut_result_is_not_cut_again(self) -> None:
        storage = _write_storage()
        db = StorageDb(storage=storage, tool_result_chars=10)
        await db.upsert_session(self._session_object(self._tool_runs(["x" * 50] * 6)))
        saved = storage.upsert.await_args.args[2]
        storage.read.return_value = _record(saved)

        await db.upsert_session(self._session_object(saved["runs"]))

        first = storage.upsert.await_args.args[2]["runs"][0]["messages"][0]["content"]
        assert first.endswith("original tool result was 50 chars]")

    async def test_the_cut_content_is_logged_capped_at_100_chars(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The dropped text may hold personal data: only its first 100 characters reach the logs."""
        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)
        storage = _write_storage()
        db = StorageDb(storage=storage, tool_result_chars=10)

        with caplog.at_level(logging.INFO, logger="digitalkin"):
            await db.upsert_session(self._session_object(self._tool_runs(["x" * 10 + "y" * 500] * 6)))

        logged = [record.getMessage() for record in caplog.records if "TOOL-RESULT-RETRUNCATE" in record.getMessage()]
        assert logged
        assert all(message.endswith("y" * 100) and "y" * 101 not in message for message in logged)

    async def test_short_results_are_left_alone(self) -> None:
        storage = _write_storage()
        await StorageDb(storage=storage).upsert_session(self._session_object(self._tool_runs(["short"] * 8)))
        assert all(run["messages"][0]["content"] == "short" for run in storage.upsert.await_args.args[2]["runs"])


class TestSessionHistorySafety:
    """A save never drops a stored run: the session-loss bug of Ada ("coupures de session")."""

    @staticmethod
    def _session_object(runs: list[dict[str, Any]]) -> AgentSession:
        session = AgentSession.from_dict({"session_id": "s1", "agent_id": "a1", "runs": runs})
        assert session is not None
        return session

    async def test_a_session_rebuilt_empty_keeps_the_stored_runs(self) -> None:
        """Regression: Agno rebuilt an empty session after a failed read and its save kept only that turn."""
        storage = _write_storage(_session("s1", None, _runs(3)))

        await StorageDb(storage=storage).upsert_session(
            self._session_object([{"run_id": "new", "agent_id": "a1", "session_id": "s1"}])
        )

        saved = storage.upsert.await_args.args[2]["runs"]
        assert [run["run_id"] for run in saved] == ["r0", "r1", "r2", "new"]

    async def test_a_resumed_run_replaces_its_stored_copy_in_place(self) -> None:
        stored = [
            {"run_id": "r0", "agent_id": "a1", "session_id": "s1", "status": "COMPLETED"},
            {"run_id": "r1", "agent_id": "a1", "session_id": "s1", "status": "PAUSED"},
        ]
        storage = _write_storage(_session("s1", None, stored))
        resumed = [*stored[:1], {**stored[1], "status": "COMPLETED"}]

        await StorageDb(storage=storage).upsert_session(self._session_object(resumed))

        saved = storage.upsert.await_args.args[2]["runs"]
        assert [(run["run_id"], run["status"]) for run in saved] == [("r0", "COMPLETED"), ("r1", "COMPLETED")]

    async def test_a_read_failure_during_a_save_writes_nothing(self) -> None:
        storage = _write_storage()
        storage.read.side_effect = StorageServiceError("GetRecord failed: UNAVAILABLE")

        assert await StorageDb(storage=storage).upsert_session(self._session_object(_runs(1))) is None
        storage.upsert.assert_not_awaited()

    async def test_concurrent_turns_on_one_mission_both_keep_their_run(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two tasks (each with its own storage strategy and StorageDb) save the same session at once."""
        monkeypatch.chdir(tmp_path)
        storages = [
            DefaultStorage("missions:m1", "setups:s", "setup_versions:v", dict(AGNO_STORAGE_CONFIG)) for _ in range(2)
        ]
        storages[1].storage = storages[0].storage  # one backing store, as the storage service would be
        base = _runs(1)
        await StorageDb(storage=storages[0]).upsert_session(self._session_object(base))
        turns = [
            self._session_object([*base, {"run_id": f"turn{i}", "agent_id": "a1", "session_id": "s1"}])
            for i in range(2)
        ]

        await asyncio.gather(*(StorageDb(storage=st).upsert_session(turn) for st, turn in zip(storages, turns)))

        saved = await StorageDb(storage=storages[0]).get_session("s1", deserialize=False)
        assert isinstance(saved, dict)
        assert sorted(run["run_id"] for run in saved["runs"]) == ["r0", "turn0", "turn1"]


class TestSessionListing:
    async def test_lists_every_page_past_the_service_cap(self) -> None:
        pages = [[_record(_session(f"s{i}", None, [])) for i in range(100)], [_record(_session("last", None, []))]]
        storage = Mock(list=AsyncMock(side_effect=pages))

        _, total = await StorageDb(storage=storage).get_sessions(SessionType.AGENT, deserialize=False)

        assert total == 101
        assert [call.kwargs for call in storage.list.await_args_list] == [
            {"limit": 100, "offset": 0},
            {"limit": 100, "offset": 100},
        ]

    async def test_a_listing_failure_raises(self) -> None:
        storage = Mock(list=AsyncMock(side_effect=StorageServiceError("ListRecords failed")))
        with pytest.raises(StorageServiceError):
            await StorageDb(storage=storage).get_sessions(SessionType.AGENT)


class TestRunReads:
    """Agno 3's run reads, served from the runs stored inline in each session."""

    @pytest.fixture
    def db(self) -> StorageDb:
        runs_s1 = [
            {"run_id": "r1", "agent_id": "a1", "session_id": "s1", "status": "COMPLETED"},
            {"run_id": "r2", "agent_id": "a2", "session_id": "s1", "status": "PAUSED"},
        ]
        runs_s2 = [{"run_id": "r3", "agent_id": "a1", "session_id": "s2", "status": "COMPLETED"}]
        return _listed_db([_session("s1", "u1", runs_s1), _session("s2", "u2", runs_s2)])

    async def test_get_run_finds_a_run_in_any_session(self, db: StorageDb) -> None:
        run = await db.get_run("r3")
        assert isinstance(run, RunOutput)
        assert run.run_id == "r3"

    async def test_get_run_raw_and_missing(self, db: StorageDb) -> None:
        assert (await db.get_run("r2", deserialize=False))["agent_id"] == "a2"
        assert await db.get_run("nope") is None

    async def test_get_runs_of_one_session(self, db: StorageDb) -> None:
        runs = await db.get_runs(session_id="s1")
        assert [run.run_id for run in runs] == ["r1", "r2"]
        db.storage.list.assert_not_awaited()

    async def test_get_runs_filters(self, db: StorageDb) -> None:
        rows, total = await db.get_runs(agent_id="a1", deserialize=False)
        assert [row["run_id"] for row in rows] == ["r1", "r3"]
        assert total == 2
        assert [run.run_id for run in await db.get_runs(user_id="u2")] == ["r3"]
        assert [run.run_id for run in await db.get_runs(status=RunStatus.paused)] == ["r2"]

    async def test_get_runs_paginates_after_counting(self, db: StorageDb) -> None:
        rows, total = await db.get_runs(limit=1, page=2, deserialize=False)
        assert [row["run_id"] for row in rows] == ["r2"]
        assert total == 3


class TestCompactionRecord:
    """One record per session, overwritten by each fold."""

    async def test_writes_through_the_atomic_upsert(self) -> None:
        storage = Mock(upsert=AsyncMock())
        db = StorageDb(storage=storage)

        assert await db.upsert_compaction_record("s1", {"summary": "S", "first_kept_message_id": "m9"})

        storage.upsert.assert_awaited_once_with(
            "agno_compactions", "s1", {"summary": "S", "first_kept_message_id": "m9", "session_id": "s1"}
        )

    async def test_reads_the_record_back(self) -> None:
        storage = Mock(read=AsyncMock(return_value=_record({"session_id": "s1", "summary": "S"})))

        record = await StorageDb(storage=storage).get_compaction_record("s1")

        assert record is not None
        assert (record["summary"], record["messages_compacted"]) == ("S", 0)

    async def test_a_session_never_compacted_has_no_record(self) -> None:
        storage = Mock(read=AsyncMock(return_value=None))
        assert await StorageDb(storage=storage).get_compaction_record("s1") is None

    async def test_a_read_failure_raises_and_a_write_failure_is_reported(self) -> None:
        """A failed read must not look like "never compacted": the next write would drop the summary."""
        storage = Mock(read=AsyncMock(side_effect=RuntimeError("down")), upsert=AsyncMock(side_effect=RuntimeError()))
        db = StorageDb(storage=storage)

        with pytest.raises(RuntimeError, match="down"):
            await db.get_compaction_record("s1")
        assert await db.upsert_compaction_record("s1", {"summary": "S"}) is False


class TestDefaultStorageRoundTrip:
    """The same contract over the SDK's local storage strategy."""

    @pytest.fixture
    def db(self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> StorageDb:
        monkeypatch.chdir(tmp_path)  # DefaultStorage persists to a JSON file in the working directory
        return StorageDb(
            storage=DefaultStorage("missions:m1", "setups:s", "setup_versions:v", dict(AGNO_STORAGE_CONFIG))
        )

    async def test_session_round_trip(self, db: StorageDb) -> None:
        run = RunOutput(run_id="r1", agent_id="a1", session_id="s1", messages=[Message(role="user", content="hi")])
        session = AgentSession(session_id="s1", agent_id="a1", user_id="u1", runs=[run])

        await db.upsert_session(session)
        session.session_data = {"session_name": "renamed later"}
        await db.upsert_session(session)
        loaded = await db.get_session("s1", SessionType.AGENT)

        assert isinstance(loaded, AgentSession)
        assert loaded.session_data == {"session_name": "renamed later"}
        assert [r.run_id for r in loaded.runs or []] == ["r1"]
        assert (await db.get_run("r1")).messages[0].content == "hi"  # type: ignore[union-attr]

    async def test_rename_and_delete(self, db: StorageDb) -> None:
        await db.upsert_session(AgentSession(session_id="s1", agent_id="a1"))

        renamed = await db.rename_session("s1", SessionType.AGENT, "Plans", deserialize=False)
        assert renamed is not None
        assert renamed["session_name"] == "Plans"
        assert await db.delete_session("s1") is True
        assert await db.get_session("s1") is None

    async def test_compaction_record_round_trip(self, db: StorageDb) -> None:
        await db.upsert_compaction_record("s1", {"summary": "", "token_samples": [[10, 20]]})
        await db.upsert_compaction_record("s1", {"summary": "S", "token_samples": [[10, 20]], "messages_compacted": 4})

        record = await db.get_compaction_record("s1")

        assert record is not None
        assert (record["summary"], record["token_samples"], record["messages_compacted"]) == ("S", [[10, 20]], 4)
