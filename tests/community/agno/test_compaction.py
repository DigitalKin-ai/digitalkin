"""HistoryCompactor: token-threshold history compaction at the model layer."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("agno", reason="optional agno dependency not installed")

from agno.compaction.prompts import DEFAULT_COMPACTION_PROMPT, LENGTH_RULE_COMPACT, SUMMARY_PREFIX
from agno.models.message import Message
from agno.utils.tokens import count_tokens

from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter
from digitalkin.community.agno.compaction import DkCompaction, HistoryCompactor, TokenCalibration
from digitalkin.community.agno.db import AGNO_STORAGE_CONFIG, StorageDb
from digitalkin.services.storage.default_storage import DefaultStorage

split_request = HistoryCompactor.split_request

# About 200 tokens a message: ten replayed runs are ~4000 tokens.
_WORDS = "word " * 200


class _Summarizer:
    """Stands in for the summarizer model: records each request and returns a scripted summary."""

    id = "gpt-5"

    def __init__(self, *summaries: str | BaseException) -> None:
        self.summaries = list(summaries) or ["## Goal\nThe summary."]
        self.requests: list[list[Message]] = []

    async def aresponse(self, messages: list[Message], **_: Any) -> SimpleNamespace:
        self.requests.append(messages)
        summary = self.summaries.pop(0) if len(self.summaries) > 1 else self.summaries[0]
        if isinstance(summary, BaseException):
            raise summary
        return SimpleNamespace(content=summary)


class _Db:
    """In-memory stand-in for the two StorageDb compaction methods."""

    def __init__(self, record: dict[str, Any] | None = None) -> None:
        self.records: dict[str, dict[str, Any]] = {"s1": record} if record else {}
        self.writes = 0

    async def get_compaction_record(self, session_id: str) -> dict[str, Any] | None:
        return self.records.get(session_id)

    async def upsert_compaction_record(self, session_id: str, record: dict[str, Any]) -> bool:
        self.writes += 1
        self.records[session_id] = {**record, "session_id": session_id}
        return True


def _history(runs: int, start: int = 0) -> list[Message]:
    messages: list[Message] = []
    for index in range(start, start + runs):
        messages.append(Message(id=f"u{index}", role="user", content=f"question {index} {_WORDS}", from_history=True))
        messages.append(
            Message(id=f"a{index}", role="assistant", content=f"answer {index} {_WORDS}", from_history=True)
        )
    return messages


def _request(history: list[Message], question: str = "current question") -> list[Message]:
    return [Message(role="system", content="You are a kin."), *history, Message(role="user", content=question)]


def _compactor(
    summarizer: _Summarizer, db: _Db, *, compact_at_tokens: int = 2000, uncompacted_runs: int = 2
) -> HistoryCompactor:
    compaction = DkCompaction(
        model=summarizer,  # type: ignore[arg-type]
        compact_at_tokens=compact_at_tokens,
        uncompacted_runs=uncompacted_runs,
        store_compacted_messages=False,
    )
    return HistoryCompactor(compaction=compaction, db=db, session_id="s1", model_id="gpt-5")  # type: ignore[arg-type]


def _ids(messages: list[Message]) -> list[str]:
    return [message.id for message in messages]


class TestThreshold:
    def test_large_window_uses_the_cap(self) -> None:
        assert HistoryCompactor.threshold(1_000_000, 600_000, 0.9) == 600_000

    def test_small_window_uses_the_ratio(self) -> None:
        assert HistoryCompactor.threshold(200_000, 600_000, 0.9) == 180_000

    def test_unknown_window_uses_the_cap(self) -> None:
        assert HistoryCompactor.threshold(0, 600_000, 0.9) == 600_000

    def test_never_below_one(self) -> None:
        assert HistoryCompactor.threshold(1, 600_000, 0.5) == 1


class TestSplitRequest:
    def test_splits_prefix_history_and_current_run(self) -> None:
        history = _history(2)
        messages = _request(history)

        prefix, replayed, current = split_request(messages)

        assert [m.role for m in prefix] == ["system"]
        assert _ids(replayed) == _ids(history)
        assert [m.content for m in current] == ["current question"]

    def test_no_history(self) -> None:
        prefix, replayed, current = split_request([
            Message(role="system", content="s"),
            Message(role="user", content="q"),
        ])

        assert len(prefix) == 1
        assert replayed == []
        assert len(current) == 1

    def test_non_contiguous_history_is_left_alone(self) -> None:
        history = _history(2)
        messages = [*history[:2], Message(role="user", content="injected"), *history[2:]]

        _, replayed, current = split_request(messages)

        assert replayed == []
        assert len(current) == len(messages)


class TestView:
    async def test_below_the_threshold_nothing_is_summarized(self) -> None:
        summarizer, db = _Summarizer(), _Db()
        messages = _request(_history(2))

        sent = await _compactor(summarizer, db).view(messages)

        assert _ids(sent) == _ids(messages)
        assert summarizer.requests == []
        assert db.writes == 0

    async def test_crossing_the_threshold_folds_the_older_history(self) -> None:
        summarizer, db = _Summarizer("## Goal\nFolded."), _Db()
        history = _history(10)
        messages = _request(history)

        sent = await _compactor(summarizer, db).view(messages)

        assert len(summarizer.requests) == 1
        assert sent[0].role == "system"
        assert sent[1].role == "user"
        assert sent[1].content.startswith(SUMMARY_PREFIX)
        assert "Folded." in sent[1].content
        assert _ids(sent[2:-1]) == _ids(history[-4:]), "the last two runs are kept verbatim"
        assert sent[-1].content == "current question", "the current run is never folded"
        assert db.records["s1"]["first_kept_message_id"] == "u8"
        assert db.records["s1"]["messages_compacted"] == 16

    async def test_agnos_messages_are_never_modified(self) -> None:
        messages = _request(_history(10))
        before = [(m.id, m.role, m.content) for m in messages]

        await _compactor(_Summarizer(), _Db()).view(messages)

        assert [(m.id, m.role, m.content) for m in messages] == before

    async def test_a_run_folds_once_however_many_model_calls_it_makes(self) -> None:
        summarizer, db = _Summarizer(), _Db()
        compactor = _compactor(summarizer, db)
        history = _history(10)

        await compactor.view(_request(history))
        sent = await compactor.view([*_request(history), Message(role="assistant", content="tool loop step")])

        assert len(summarizer.requests) == 1
        assert sent[1].content.startswith(SUMMARY_PREFIX)

    async def test_next_run_reuses_the_stored_record_without_summarizing(self) -> None:
        db = _Db()
        await _compactor(_Summarizer(), db).view(_request(_history(10)))

        later = _Summarizer()
        sent = await _compactor(later, db).view(_request(_history(11)))

        assert later.requests == []
        assert sent[1].content.startswith(SUMMARY_PREFIX)
        assert _ids(sent[2:-1]) == _ids(_history(11)[-6:]), "every run since the fold anchor is replayed"

    async def test_the_next_fold_carries_the_previous_summary_forward(self) -> None:
        db = _Db()
        await _compactor(_Summarizer("FIRST SUMMARY"), db).view(_request(_history(10)))

        second = _Summarizer("SECOND SUMMARY")
        sent = await _compactor(second, db).view(_request(_history(20)))

        assert len(second.requests) == 1
        transcript = second.requests[0][-1].content
        assert "FIRST SUMMARY" in transcript
        assert "question 7 " not in transcript, "turns the first fold covered are not summarized again"
        assert "SECOND SUMMARY" in sent[1].content

    async def test_a_record_whose_anchor_is_gone_fails_open(self) -> None:
        db = _Db({"session_id": "s1", "summary": "stale", "first_kept_message_id": "gone", "messages_compacted": 3})
        messages = _request(_history(3))

        sent = await _compactor(_Summarizer(), db, compact_at_tokens=100_000).view(messages)

        assert _ids(sent) == _ids(messages)

    async def test_a_failed_summary_sends_the_history_uncompacted(self) -> None:
        db = _Db()
        messages = _request(_history(10))

        sent = await _compactor(_Summarizer(RuntimeError("proxy down")), db).view(messages)

        assert _ids(sent) == _ids(messages)
        assert db.writes == 0

    async def test_a_failing_store_never_fails_the_request(self) -> None:
        db = _Db()

        async def broken(session_id: str) -> None:
            raise RuntimeError("storage down")

        db.get_compaction_record = broken  # type: ignore[method-assign]
        messages = _request(_history(10))

        sent = await _compactor(_Summarizer(), db).view(messages)

        assert _ids(sent) == _ids(messages)


class TestForceFold:
    async def test_folds_below_the_threshold(self) -> None:
        summarizer, db = _Summarizer(), _Db()
        history = _history(5)

        sent = await _compactor(summarizer, db, compact_at_tokens=1_000_000).force_fold(_request(history))

        assert sent is not None
        assert sent[1].content.startswith(SUMMARY_PREFIX)
        assert _ids(sent[2:-1]) == _ids(history[-4:])
        assert db.writes == 1

    async def test_nothing_to_fold_returns_none(self) -> None:
        sent = await _compactor(_Summarizer(), _Db()).force_fold(_request(_history(2)))

        assert sent is None

    async def test_no_history_returns_none(self) -> None:
        sent = await _compactor(_Summarizer(), _Db()).force_fold([Message(role="user", content="q")])

        assert sent is None


class TestSummarizerBudget:
    def test_reads_only_the_newest_messages_that_fit(self) -> None:
        compaction = DkCompaction(compact_at_tokens=1000, summarize_char_budget=2500)
        history = _history(5)

        kept = compaction._trim_for_summary(history)

        assert 0 < len(kept) < len(history)
        assert kept[-1].id == history[-1].id


def _build(context_window: int = 1_000_000, summary_context_window: int = 1_000_000) -> HistoryCompactor:
    return HistoryCompactor.build(
        summarizer=_Summarizer(),  # type: ignore[arg-type]
        context_window=context_window,
        summary_context_window=summary_context_window,
        db=_Db(),  # type: ignore[arg-type]
        session_id="s1",
        model_id="gpt-5",
        max_tokens=600_000,
        window_ratio=0.9,
        uncompacted_runs=5,
    )


class TestBuild:
    def test_sized_from_the_window(self) -> None:
        compactor = _build()

        assert compactor.compaction.compact_at_tokens == 600_000
        assert compactor.compaction.uncompacted_runs == 5
        assert compactor.compaction.store_compacted_messages is False
        assert compactor.compaction.search_compacted_messages is False
        assert compactor.compaction.summarize_char_budget == 2_000_000

    def test_the_summarizer_read_is_sized_from_its_own_window(self) -> None:
        compactor = _build(summary_context_window=200_000)

        assert compactor.compaction.compact_at_tokens == 600_000, "the threshold follows the head model"
        assert compactor.compaction.summarize_char_budget == 400_000

    def test_the_summarizer_read_never_drops_below_agnos_budget(self) -> None:
        from agno.compaction.compaction import DEFAULT_SUMMARIZE_CHAR_BUDGET

        assert _build(summary_context_window=1_000).compaction.summarize_char_budget == DEFAULT_SUMMARIZE_CHAR_BUDGET


class TestTokenCalibration:
    """``provider = slope * estimate + overhead``, fitted on recent runs."""

    def test_uncalibrated_is_the_identity(self) -> None:
        assert TokenCalibration().fit() == (1.0, 0.0)

    def test_one_sample_gives_the_overhead(self) -> None:
        calibration = TokenCalibration([(2_000, 19_000)])

        assert calibration.fit() == (1.0, 17_000.0)
        assert calibration.provider_tokens(10_000) == 27_000

    def test_samples_across_a_fold_give_slope_and_overhead(self) -> None:
        """The two requests measured on the magicbox, before and after the fold."""
        slope, overhead = TokenCalibration([(20_058, 48_431), (3_214, 20_216)]).fit()

        assert slope == pytest.approx(1.675, abs=0.01)
        assert overhead == pytest.approx(14_830, abs=100)

    def test_close_samples_keep_slope_one_and_the_newest_overhead(self) -> None:
        calibration = TokenCalibration([(20_000, 48_000), (21_000, 50_000)])

        assert calibration.fit() == (1.0, 29_000.0)

    def test_an_absurd_slope_is_clamped(self) -> None:
        slope, _ = TokenCalibration([(1_000, 10_000), (10_000, 500_000)]).fit()

        assert slope == 3.0

    def test_threshold_maps_back_into_estimate_units(self) -> None:
        calibration = TokenCalibration([(20_058, 48_431), (3_214, 20_216)])

        estimate = calibration.estimated_tokens(600_000)

        assert calibration.provider_tokens(estimate) == pytest.approx(600_000, abs=5)

    def test_keeps_the_recent_samples(self) -> None:
        calibration = TokenCalibration()
        for index in range(1, 7):
            calibration.add(index * 1_000, index * 2_000)

        assert calibration.samples[0] == (3_000, 6_000)
        assert len(calibration.samples) == 4

    def test_malformed_storage_is_ignored(self) -> None:
        calibration = TokenCalibration.from_stored([[1_000, 2_000], "x", [0, 5], [3]])

        assert calibration.samples == [(1_000, 2_000)]


class TestCalibratedCompactor:
    @staticmethod
    def _estimate(messages: list[Message]) -> int:
        return count_tokens(messages, None, "gpt-5")

    async def test_a_history_under_the_estimate_but_over_the_provider_count_folds_next_run(self) -> None:
        db = _Db()
        messages = _request(_history(4))
        estimate = self._estimate(messages)
        threshold = estimate + 5_000  # the estimate alone stays under it

        first = _Summarizer()
        first_run = _compactor(first, db, compact_at_tokens=threshold)
        await first_run.view(messages)
        await first_run.observe(messages, None, estimate + 15_000)  # the provider adds a 15k overhead

        assert first.requests == [], "the run was decided before the provider's count was known"
        assert db.records["s1"]["token_samples"] == [[estimate, estimate + 15_000]]
        assert not db.records["s1"].get("summary"), "the samples are stored before any fold"

        second = _Summarizer()
        sent = await _compactor(second, db, compact_at_tokens=threshold).view(_request(_history(4)))

        assert len(second.requests) == 1
        assert sent[1].content.startswith(SUMMARY_PREFIX)
        assert db.records["s1"]["token_samples"] == [[estimate, estimate + 15_000]], "a fold keeps the samples"

    async def test_threshold_is_expressed_in_estimate_units(self) -> None:
        db = _Db({"session_id": "s1", "summary": "", "token_samples": [[20_058, 48_431], [3_214, 20_216]]})
        compactor = _compactor(_Summarizer(), db, compact_at_tokens=600_000)

        await compactor.view(_request(_history(1)))

        assert compactor.threshold_tokens == 600_000
        assert compactor.compaction.compact_at_tokens == pytest.approx((600_000 - 14_830) / 1.675, rel=0.01)

    async def test_calibrates_once_per_run(self) -> None:
        db = _Db()
        compactor = _compactor(_Summarizer(), db)
        messages = _request(_history(1))
        estimate = self._estimate(messages)

        await compactor.observe(messages, None, estimate * 2)
        await compactor.observe(messages, None, estimate * 4)

        assert db.writes == 1
        assert compactor._calibration.samples == [(estimate, estimate * 2)]

    async def test_a_failing_store_never_fails_the_run(self) -> None:
        db = _Db()

        async def broken(session_id: str, record: dict[str, Any]) -> bool:
            raise RuntimeError("storage down")

        db.upsert_compaction_record = broken  # type: ignore[method-assign]
        messages = _request(_history(1))

        await _compactor(_Summarizer(), db).observe(messages, None, 50_000)

    async def test_an_unreadable_record_is_never_overwritten(self) -> None:
        """Regression: a failed read cached "no record" and the calibration write dropped the summary."""
        db = _Db({"session_id": "s1", "summary": "S", "first_kept_message_id": "m1"})
        stored = dict(db.records["s1"])
        read = db.get_compaction_record

        async def down(session_id: str) -> dict[str, Any] | None:
            raise RuntimeError("storage down")

        db.get_compaction_record = down  # type: ignore[method-assign]
        compactor = _compactor(_Summarizer(), db)
        messages = _request(_history(1))

        assert await compactor.view(messages) == messages, "sent uncompacted"
        await compactor.observe(messages, None, 50_000)
        assert db.records["s1"] == stored
        assert db.writes == 0

        db.get_compaction_record = read  # type: ignore[method-assign]
        assert await compactor._load() is not None, "the next call reads the record again"

    async def test_calibration_keeps_a_fold_another_turn_stored(self) -> None:
        db = _Db()
        compactor = _compactor(_Summarizer(), db)
        messages = _request(_history(1))
        await compactor.view(messages)
        db.records["s1"] = {"session_id": "s1", "summary": "newer fold", "first_kept_message_id": "m1"}

        await compactor.observe(messages, None, 50_000)

        assert db.records["s1"]["summary"] == "newer fold"
        assert db.records["s1"]["token_samples"]

    def test_built_with_the_provider_threshold(self) -> None:
        assert _build(context_window=200_000).threshold_tokens == 180_000


class TestSummarizerTranscript:
    """The summarizer reads tool results as the agent saw them, not clipped at Agno's 20k."""

    @staticmethod
    def _delegation(report: str) -> list[Message]:
        call = {"id": "c1", "type": "function", "function": {"name": "delegate_task_to_member", "arguments": "{}"}}
        return [
            Message(id="u0", role="user", content="Ask the Analyst for the report", from_history=True),
            Message(id="a0", role="assistant", content=None, tool_calls=[call], from_history=True),
            Message(
                id="t0",
                role="tool",
                content=report,
                tool_call_id="c1",
                tool_name="delegate_task_to_member",
                from_history=True,
            ),
            Message(id="a1", role="assistant", content="Here is the report.", from_history=True),
        ]

    async def test_a_long_member_result_reaches_the_summarizer_whole(self) -> None:
        report = "x" * 39_000 + " SLA 99.9% END-OF-REPORT"
        summarizer = _Summarizer()
        history = [*self._delegation(report), *_history(2, start=1)]

        await _compactor(summarizer, _Db(), compact_at_tokens=1_000_000, uncompacted_runs=1).force_fold(
            _request(history)
        )

        transcript = summarizer.requests[0][-1].content
        assert "END-OF-REPORT" in transcript
        assert "## tool (delegate_task_to_member)" in transcript
        assert "clipped" not in transcript

    async def test_a_runaway_result_is_clipped_where_the_agent_was(self) -> None:
        summarizer = _Summarizer()
        history = [*self._delegation("y" * 160_000), *_history(2, start=1)]

        await _compactor(summarizer, _Db(), compact_at_tokens=1_000_000, uncompacted_runs=1).force_fold(
            _request(history)
        )

        assert "[clipped, 10000 more characters]" in summarizer.requests[0][-1].content

    async def test_the_prompt_is_agnos(self) -> None:
        summarizer = _Summarizer()

        await _compactor(summarizer, _Db(), compact_at_tokens=1_000_000, uncompacted_runs=1).force_fold(
            _request(_history(3))
        )

        system, transcript = summarizer.requests[0]
        assert system.content == DEFAULT_COMPACTION_PROMPT.format(length_rule=LENGTH_RULE_COMPACT)
        assert transcript.role == "user"
        assert transcript.content.startswith("## user")


class TestOverStorageDb:
    """The compactor over the real StorageDb and the SDK's local storage strategy."""

    async def test_a_fold_is_stored_and_reused_by_the_next_run(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)  # DefaultStorage persists to a JSON file in the working directory
        db = StorageDb(storage=DefaultStorage("missions:m1", "setups:s", "setup_versions:v", dict(AGNO_STORAGE_CONFIG)))

        await _compactor(_Summarizer("## Goal\nFolded."), db).view(_request(_history(10)))  # type: ignore[arg-type]
        later = _Summarizer()
        sent = await _compactor(later, db).view(_request(_history(11)))  # type: ignore[arg-type]

        record = await db.get_compaction_record("s1")
        assert record is not None
        assert (record["first_kept_message_id"], record["messages_compacted"]) == ("u8", 16)
        assert later.requests == []
        assert "Folded." in sent[1].content


class TestAdapterEvents:
    """Agno's own compaction events reach the DigitalKin stream as custom events."""

    def test_compaction_completed(self) -> None:
        from agno.run.agent import CompactionCompletedEvent

        event = CompactionCompletedEvent(
            run_id="r1", agent_id="a1", messages_compacted=3, tokens_before=900, tokens_after=100, archived=False
        )

        [out] = AgnoStreamAdapter().to_digitalkin_events(event)

        assert out.name == "compaction_completed"
        assert out.value == {"messages_compacted": 3, "tokens_before": 900, "tokens_after": 100, "archived": False}

    def test_compaction_started(self) -> None:
        from agno.run.agent import CompactionStartedEvent

        [out] = AgnoStreamAdapter().to_digitalkin_events(CompactionStartedEvent(run_id="r1", agent_id="a1"))

        assert (out.name, out.value) == ("compaction_started", {})
