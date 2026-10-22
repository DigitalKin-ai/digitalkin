"""Token-threshold history compaction at the model layer, for Agno agents and team leaders alike.

Once a request's replayed history crosses a token threshold, its older part is folded into a
summary and only the summary and a recent tail are sent; the summary is rewritten only on the next
crossing. The persisted session is never touched: the fold lives in one compaction record per
session (``StorageDb``) and is applied to each request on its way to the model (``SafeLiteLLM``).

Agno's own ``Agent(compaction=...)`` covers neither ``Team`` nor an async db, so the fold reuses
Agno's ``Compaction`` for everything but storage: the pair-safe cut, the summarizer prompt carrying
the previous summary forward, and the view swapping the folded messages for the summary. Agno tags
every replayed message ``from_history``, which splits a request into the system prefix, the history
to fold, and the current run, which is never folded.

The fold is decided on Agno's local (tiktoken) token estimate, calibrated on the provider's count:
a provider such as Claude adds a fixed tool-definition overhead and tokenizes some content less
densely, so ``provider = slope * estimate + overhead`` is fitted per session and the threshold is
mapped into estimate units, the units Agno sizes the kept tail in.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any, ClassVar

from agno.compaction import Compaction, CompactionRecord
from agno.compaction.archive import render_message
from agno.compaction.compaction import DEFAULT_SUMMARIZE_CHAR_BUDGET
from agno.utils.tokens import count_tokens
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import override

from digitalkin.community.agno.safe_litellm import SafeLiteLLM
from digitalkin.community.litellm import LiteLLMRegistry
from digitalkin.logger import logger

if TYPE_CHECKING:
    from agno.models.base import Model
    from agno.models.message import Message
    from agno.run.agent import RunOutput

    from digitalkin.community.agno.db import StorageDb
    from digitalkin.community.litellm import ModelSpec


class CompactionSettings(BaseSettings):
    """When a head model folds its replayed history into a summary, and which model writes it."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_COMPACTION_", case_sensitive=False)

    enabled: bool = Field(default=True, description="Compact the replayed history at the token threshold.")
    max_tokens: int = Field(
        default=600_000, gt=0, description="Context size (tokens) that triggers compaction on large-window models."
    )
    window_ratio: float = Field(
        default=0.9,
        gt=0,
        le=1,
        description="Share of the model's input budget the threshold is capped at on smaller models.",
    )
    uncompacted_runs: int = Field(default=5, ge=0, description="Recent runs kept verbatim after a compaction.")
    summary_model: str = Field(
        default="claude-haiku-5.5",
        description="LiteLLM proxy model that writes the summaries. Empty uses the agent's model.",
    )


@lru_cache(maxsize=1)
def get_compaction_settings() -> CompactionSettings:  # dk-allow-global (settings factory)
    """Process-wide ``CompactionSettings`` singleton.

    Returns:
        The shared ``CompactionSettings`` instance.
    """
    return CompactionSettings()


@dataclass
class TokenCalibration:
    """Maps a local token estimate to the provider's count: ``slope * estimate + overhead``.

    Attributes:
        samples: ``(estimated, provider)`` token counts of recent requests, oldest first.
    """

    # Recent enough to follow a change of tools, enough to span a fold.
    max_samples: ClassVar[int] = 4

    samples: list[tuple[int, int]] = field(default_factory=list)

    @classmethod
    def from_stored(cls, value: Any) -> TokenCalibration:
        """Rebuild from the stored ``token_samples``, ignoring malformed entries.

        Args:
            value: The stored list of ``[estimated, provider]`` pairs, or anything else.

        Returns:
            The calibration; empty when nothing usable is stored.
        """
        samples = [
            (int(pair[0]), int(pair[1]))
            for pair in value or []
            if isinstance(pair, list | tuple) and len(pair) == 2 and pair[0] > 0 and pair[1] > 0  # ruff: ignore[magic-value-comparison]
        ]
        return cls(samples[-cls.max_samples :])

    def add(self, estimated: int, provider: int) -> None:
        """Record one request's two counts.

        Args:
            estimated: The local estimate of the request.
            provider: The prompt size the provider reported for it.
        """
        self.samples = [*self.samples, (estimated, provider)][-self.max_samples :]

    def fit(self) -> tuple[float, float]:
        """Least-squares ``(slope, overhead)``; slope 1 until the samples span 25% of the largest.

        Closer samples make the slope noise, so only the overhead is taken, from the newest sample
        (it carries the current tool list). A fitted slope outside [0.8, 3] is clamped.

        Returns:
            The slope and overhead; ``(1.0, 0.0)`` with no sample.
        """
        if not self.samples:
            return 1.0, 0.0
        estimates = [e for e, _ in self.samples]
        providers = [p for _, p in self.samples]
        if len(self.samples) < 2 or max(estimates) - min(estimates) < 0.25 * max(estimates):  # ruff: ignore[magic-value-comparison]
            return 1.0, float(providers[-1] - estimates[-1])
        mean_e = sum(estimates) / len(estimates)
        mean_p = sum(providers) / len(providers)
        variance = sum((e - mean_e) ** 2 for e in estimates)
        covariance = sum((e - mean_e) * (p - mean_p) for e, p in self.samples)
        slope = min(max(covariance / variance, 0.8), 3.0)
        return slope, mean_p - slope * mean_e

    def provider_tokens(self, estimated: int) -> int:
        """The provider's count predicted for an estimate.

        Args:
            estimated: A local estimate.

        Returns:
            The predicted provider count.
        """
        slope, overhead = self.fit()
        return int(slope * estimated + overhead)

    def estimated_tokens(self, provider: int) -> int:
        """The local estimate that corresponds to a provider count.

        Args:
            provider: A provider token count, such as the threshold.

        Returns:
            The matching estimate, at least 1.
        """
        slope, overhead = self.fit()
        return max(1, int((provider - overhead) / slope))


@dataclass
class DkCompaction(Compaction):
    """Agno ``Compaction`` whose summarizer reads the fold as the agent saw it.

    Agno clips every tool result to 20k characters for the summarizer and reads at most 400k
    characters of the transcript, so a long delegated report reached the summary half cut. Tool
    results are clipped here where ``SafeLiteLLM`` clips them for the agent, and the read budget
    is a field, sized from the summarizer's window by ``HistoryCompactor.build``.

    Attributes:
        summarize_char_budget: Characters of the folded transcript the summarizer reads, newest first.
        tool_result_chars: A tool result's share of the transcript; ``SafeLiteLLM.max_tool_result_chars``.
    """

    summarize_char_budget: int = DEFAULT_SUMMARIZE_CHAR_BUDGET
    tool_result_chars: int = 150_000

    def _render(self, message: Message) -> str:
        """Render one message as Agno does, clipping tool results at ``tool_result_chars``.

        Args:
            message: A folded message.

        Returns:
            Its markdown block.
        """
        if message.role != "tool":
            return render_message(message)
        heading = f"## tool ({message.tool_name})" if message.tool_name else "## tool"
        content = message.get_content_string()
        if len(content) > self.tool_result_chars:
            hidden = len(content) - self.tool_result_chars
            content = f"{content[: self.tool_result_chars]}\n... [clipped, {hidden} more characters]"
        return f"{heading}\n\n{content}" if content else heading

    @override
    def _trim_for_summary(self, messages: list[Message]) -> list[Message]:
        """Drop the oldest messages that do not fit ``summarize_char_budget``.

        Args:
            messages: The folded span.

        Returns:
            Its newest messages that fit the budget, oldest first; at least one.
        """
        kept: list[Message] = []
        budget = self.summarize_char_budget
        for message in reversed(messages):
            size = len(self._render(message)) + 2  # + the block separator
            if budget - size < 0 and kept:
                break
            budget -= size
            kept.append(message)
        return list(reversed(kept))

    @override
    def _summary_messages(self, messages: list[Message], previous: str | None, archived: bool = False) -> list[Message]:
        """Agno's summary request, over a transcript rendered by ``_render``.

        Agno's prompt is built from an empty transcript and reused as is; only the transcript it
        would have clipped is replaced.

        Args:
            messages: The folded span.
            previous: The summary this fold carries forward, if any.
            archived: Whether the folded messages are archived.

        Returns:
            The system prompt and the transcript message.
        """
        system, transcript_message = super()._summary_messages([], None, archived)
        transcript = "\n\n".join(self._render(message) for message in self._trim_for_summary(messages))
        if previous:
            transcript = (
                f"Summary of the conversation before this point:\n{previous}\n\nConversation since then:\n{transcript}"
            )
        return [system, transcript_message.model_copy(update={"content": transcript})]


class HistoryCompactor:
    """Applies, and when the threshold is crossed renews, one session's compaction.

    Built per task with its model, so the cached record and the "already measured" set live as
    long as the task's requests. The threshold is measured once per distinct history, i.e. once
    per run: within a run the replayed history does not change, only the current run grows.

    Attributes:
        compaction: The fold policy, with its summarizer model.
        db: Storage of the session's compaction record.
        session_id: The session whose history is folded.
        model_id: Id of the model the requests go to, which picks the token encoding.
        threshold_tokens: Fold point in provider tokens.
    """

    def __init__(
        self,
        *,
        compaction: DkCompaction,
        db: StorageDb,
        session_id: str,
        model_id: str,
        threshold_tokens: int | None = None,
    ) -> None:
        """Bind the compactor to a session.

        Args:
            compaction: The fold policy, with its summarizer model set.
            db: Storage of the session's compaction record.
            session_id: The session whose history is folded.
            model_id: Id of the model the requests go to.
            threshold_tokens: Fold point in provider tokens; ``compaction.compact_at_tokens`` when
                omitted. Mapped into estimate units as the calibration is learned.
        """
        self.compaction = compaction
        self.db = db
        self.session_id = session_id
        self.model_id = model_id
        self.threshold_tokens = (
            threshold_tokens if threshold_tokens is not None else int(compaction.compact_at_tokens or 0)
        )
        self._record: CompactionRecord | None = None
        self._calibration = TokenCalibration()
        self._calibrated = False
        self._loaded = False
        self._measured: set[tuple[Any, ...]] = set()
        # One model can serve concurrent calls; two of them must not both fold the same history.
        self._lock = asyncio.Lock()

    @staticmethod
    def threshold(context_window: int, max_tokens: int, window_ratio: float) -> int:
        """Context size, in tokens, at which the replayed history is folded.

        ``max_tokens`` on large-window models, ``window_ratio`` of the window on smaller ones, so a
        fold always comes before the provider's limit.

        Args:
            context_window: The model's input window; 0 when unknown.
            max_tokens: The threshold cap.
            window_ratio: Share of the window the threshold may reach.

        Returns:
            The threshold in tokens.
        """
        if context_window <= 0:
            return max_tokens
        return max(1, min(max_tokens, int(context_window * window_ratio)))

    @staticmethod
    def split_request(messages: list[Message]) -> tuple[list[Message], list[Message], list[Message]]:
        """Split a request into the system prefix, the replayed history and the current run.

        Args:
            messages: The messages Agno sends for one model call.

        Returns:
            ``(prefix, history, current)``. History is empty when the request replays none, or when
            the replayed messages are not one contiguous block after the system prefix: a shape
            this class does not understand is sent as is.
        """
        lead = 0
        while lead < len(messages) and messages[lead].role in {"system", "developer"}:
            lead += 1
        indexes = [index for index, message in enumerate(messages) if message.from_history]
        if not indexes:
            return messages[:lead], [], messages[lead:]
        start, end = indexes[0], indexes[-1] + 1
        if start < lead or end - start != len(indexes):
            return messages[:lead], [], messages[lead:]
        return messages[:start], messages[start:end], messages[end:]

    @classmethod
    def build(
        cls,
        *,
        summarizer: Model,
        context_window: int,
        summary_context_window: int,
        db: StorageDb,
        session_id: str,
        model_id: str,
        max_tokens: int,
        window_ratio: float,
        uncompacted_runs: int,
        max_output: int = 0,
    ) -> HistoryCompactor:
        """The compactor for a head model, its threshold sized from the model's window.

        The summary is stored alone: the persisted session stays complete for history tools.

        Args:
            summarizer: Model writing the summary. Must not carry a compactor itself.
            context_window: Window of the head model, which sets the threshold.
            summary_context_window: Input window of the summarizer, which sizes what it reads
                (two characters per window token, never below Agno's own budget).
            db: Storage of the session's compaction record.
            session_id: The session whose history is folded.
            model_id: Id of the head model.
            max_tokens: Threshold cap (see ``threshold``).
            window_ratio: Share of a smaller window the threshold may reach.
            uncompacted_runs: Recent runs kept verbatim after a fold.
            max_output: Output the head model may request, reserved in its window: the provider
                rejects a request whose input plus ``max_tokens`` exceeds it.

        Returns:
            The compactor.
        """
        threshold_tokens = cls.threshold(context_window - max_output, max_tokens, window_ratio)
        compaction = DkCompaction(
            model=summarizer,
            compact_at_tokens=threshold_tokens,
            uncompacted_runs=uncompacted_runs,
            store_compacted_messages=False,
            summarize_char_budget=max(DEFAULT_SUMMARIZE_CHAR_BUDGET, 2 * summary_context_window),
        )
        return cls(
            compaction=compaction,
            db=db,
            session_id=session_id,
            model_id=model_id,
            threshold_tokens=threshold_tokens,
        )

    @staticmethod
    async def load_summary_model() -> ModelSpec | None:
        """Load the spec of the model writing the summaries (``DIGITALKIN_COMPACTION_SUMMARY_MODEL``).

        Loaded on its own rather than with the setup's models: a summary model the proxy stops
        serving must cost the summaries their dedicated model, never the module its setup. Loaded
        with compaction off too: an archetype may have it write Agno's session summary instead.

        Returns:
            The spec, or None when no summary model is configured or the proxy cannot serve it, in
            which case the agent's own model writes the summaries.
        """
        settings = get_compaction_settings()
        if not settings.summary_model:
            return None
        registry = LiteLLMRegistry.shared()
        try:
            await registry.initialize_many([settings.summary_model])
        except Exception:
            logger.warning(
                "Summary model %s unavailable from the LiteLLM proxy, the agent's model will write summaries",
                settings.summary_model,
                exc_info=True,
            )
            return None
        return registry.try_get(settings.summary_model)

    @staticmethod
    def summarizer(
        model_spec: ModelSpec,
        *,
        extra_headers: dict[str, str],
        session_id: str,
        mission_id: str,
        cancel_event: asyncio.Event | None = None,
    ) -> SafeLiteLLM:
        """The model writing a session's summaries: low temperature, no compactor of its own.

        It shares the head's trace headers and mission metadata, so its cost is booked with the run.

        Args:
            model_spec: The summary model, or the agent's own when none is served.
            extra_headers: The head model's proxy headers.
            session_id: The session whose history is summarized.
            mission_id: The mission its cost is booked to.
            cancel_event: Stops the summarizer with the run.

        Returns:
            The summarizer model.
        """
        # Low, so the summary restates the folded history rather than embellishing it.
        return SafeLiteLLM.from_spec(
            model_spec,
            extra_headers=extra_headers,
            session_id=session_id,
            temperature=0.2,
            mission_id=mission_id,
            cancel_event=cancel_event,
        )

    @classmethod
    def for_model(
        cls,
        model_spec: ModelSpec,
        *,
        summary_spec: ModelSpec | None,
        db: StorageDb,
        session_id: str,
        mission_id: str,
        extra_headers: dict[str, str],
        cancel_event: asyncio.Event | None = None,
    ) -> HistoryCompactor | None:
        """The compactor of a head model, under ``CompactionSettings``.

        Args:
            model_spec: The head model.
            summary_spec: The summary model (``load_summary_model``); None uses the head's.
            db: Storage of the session's compaction record.
            session_id: The session whose history is folded.
            mission_id: The mission the summaries' cost is booked to.
            extra_headers: The head model's proxy headers.
            cancel_event: Stops the summarizer with the run.

        Returns:
            The compactor, or None when ``DIGITALKIN_COMPACTION_ENABLED`` is off.
        """
        settings = get_compaction_settings()
        if not settings.enabled:
            return None
        summary_spec = summary_spec or model_spec
        return cls.build(
            summarizer=cls.summarizer(
                summary_spec,
                extra_headers=extra_headers,
                session_id=session_id,
                mission_id=mission_id,
                cancel_event=cancel_event,
            ),
            context_window=model_spec.capacity.context_window,
            summary_context_window=summary_spec.capacity.context_window,
            db=db,
            session_id=session_id,
            model_id=model_spec.name,
            max_tokens=settings.max_tokens,
            window_ratio=settings.window_ratio,
            uncompacted_runs=settings.uncompacted_runs,
            max_output=model_spec.capacity.context_window - model_spec.capacity.input_budget,
        )

    @staticmethod
    def history_kwargs() -> dict[str, Any]:
        """History settings of an Agno Agent or Team whose model compacts.

        Compaction owns the replay window: every run since the last fold is sent and the token
        threshold is the limit, so a message or run window or a tool-call filter would only starve
        it. Old tool results are cut when the session is saved (``StorageDb.tool_result_chars``).
        Agno's session summary would put a second summary of the same history in the context.

        Returns:
            Agent or Team kwargs.
        """
        return {
            "num_history_messages": None,
            # Agno replays only the last 3 runs when no limit is set: past any session's length.
            "num_history_runs": 10_000,
            "max_tool_calls_from_history": None,
            "enable_session_summaries": False,
            "add_session_summary_to_context": False,
        }

    async def view(
        self,
        messages: list[Message],
        tools: list[Any] | None = None,
        run_response: RunOutput | None = None,
    ) -> list[Message]:
        """The messages to send: the history behind the record replaced by its summary.

        Folds first when the replayed history has crossed the threshold. Never raises: a request
        that cannot be compacted is sent as it came.

        Args:
            messages: The messages Agno sends for one model call.
            tools: The request's tool definitions, counted against the threshold.
            run_response: The run being served, credited with the summarizer's usage.

        Returns:
            A new list for this request only; ``messages`` and the persisted session are untouched.
        """
        prefix, history, current = self.split_request(messages)
        if not history:
            return messages
        try:  # ruff: ignore[too-many-statements-in-try-clause]
            async with self._lock:
                record = await self._load()
                signature = (history[0].id, history[-1].id, len(history), record.id if record else None)
                if signature not in self._measured:
                    self._measured.add(signature)
                    in_context = self._apply(history, record)
                    tokens = await asyncio.to_thread(count_tokens, [*prefix, *in_context], tools, self.model_id)
                    logger.info(
                        "History measured: session=%s model=%s estimated=%d provider=%d messages=%d threshold=%d",
                        self.session_id,
                        self.model_id,
                        tokens,
                        self._calibration.provider_tokens(tokens),
                        len(history),
                        self.threshold_tokens,
                    )
                    if self.compaction.should_compact(in_context, context_tokens=tokens):
                        record = await self._fold(prefix, history, in_context, tools, tokens, run_response) or record
        except Exception:
            logger.exception(
                "History compaction failed, sending the history uncompacted: session=%s model=%s",
                self.session_id,
                self.model_id,
            )
            return messages
        return [*prefix, *self._apply(history, record), *current]

    async def force_fold(
        self,
        messages: list[Message],
        tools: list[Any] | None = None,
        run_response: RunOutput | None = None,
    ) -> list[Message] | None:
        """Fold regardless of the threshold, after the provider rejected a request as too long.

        Args:
            messages: The messages Agno sends for one model call.
            tools: The request's tool definitions.
            run_response: The run being served.

        Returns:
            The messages to resend, or None when nothing could be folded, so retrying is pointless.
        """
        prefix, history, current = self.split_request(messages)
        if not history:
            return None
        try:
            async with self._lock:
                in_context = self._apply(history, await self._load())
                new_record = await self._fold(prefix, history, in_context, tools, None, run_response)
        except Exception:
            logger.exception("Forced history compaction failed: session=%s model=%s", self.session_id, self.model_id)
            return None
        if new_record is None:
            return None
        return [*prefix, *self._apply(history, new_record), *current]

    async def observe(self, sent: list[Message], tools: list[Any] | None, provider_tokens: int) -> None:
        """Calibrate the estimate on the provider's count of a request that was just sent.

        Once per compactor, i.e. once per run: the first response is enough, and estimating every
        request of a long tool loop would cost a full tokenization each time. Never raises.

        Args:
            sent: The messages of the request, as sent.
            tools: The request's tool definitions.
            provider_tokens: The prompt size the provider reported for it.
        """
        if self._calibrated or provider_tokens <= 0:
            return
        self._calibrated = True
        try:  # ruff: ignore[too-many-statements-in-try-clause]
            estimated = await asyncio.to_thread(count_tokens, sent, tools, self.model_id)
            if estimated <= 0:
                return
            async with self._lock:
                await self._load()
                self._calibration.add(estimated, provider_tokens)
                self._scale()
                # Only the samples change: the record is read again so a fold another turn stored
                # meanwhile is kept, never replaced by this compactor's older copy.
                stored = await self.db.get_compaction_record(self.session_id) or {}
                stored["token_samples"] = [list(sample) for sample in self._calibration.samples]
                await self.db.upsert_compaction_record(self.session_id, stored)
            logger.info(
                "Token estimate calibrated: session=%s model=%s estimated=%d provider=%d slope_overhead=%s",
                self.session_id,
                self.model_id,
                estimated,
                provider_tokens,
                self._calibration.fit(),
            )
        except Exception:
            logger.warning(
                "Token estimate calibration failed: session=%s model=%s",
                self.session_id,
                self.model_id,
                exc_info=True,
            )

    def _scale(self) -> None:
        """Express the threshold in estimate units, through the session's calibration."""
        self.compaction.compact_at_tokens = self._calibration.estimated_tokens(self.threshold_tokens)

    def _stored(self) -> dict[str, Any]:
        """The session's record: the compaction in force, if any, and the calibration samples.

        Returns:
            What ``upsert_compaction_record`` stores.
        """
        data = self._record.to_dict() if self._record is not None else {}
        if self._calibration.samples:
            data["token_samples"] = [list(sample) for sample in self._calibration.samples]
        return data

    def _apply(self, history: list[Message], record: CompactionRecord | None) -> list[Message]:
        """The history as sent under ``record``; the full history when its anchor is gone.

        Args:
            history: The replayed history.
            record: The compaction in force, if any.

        Returns:
            The summary followed by the kept tail, or ``history`` itself.
        """
        return history if record is None else self.compaction.apply_record(history, record)

    async def _load(self) -> CompactionRecord | None:
        """The session's stored record, read once per compactor.

        Returns:
            The compaction in force, or None when the session was never compacted.

        Raises:
            StorageServiceError: When the record cannot be read; the next call reads it again.
        """
        if not self._loaded:
            data = await self.db.get_compaction_record(self.session_id) or {}
            self._loaded = True
            if data.get("summary"):
                self._record = CompactionRecord.from_dict(data)
            if not self._calibration.samples:
                self._calibration = TokenCalibration.from_stored(data.get("token_samples"))
            self._scale()
        return self._record

    async def _fold(
        self,
        prefix: list[Message],
        history: list[Message],
        in_context: list[Message],
        tools: list[Any] | None,
        tokens_before: int | None,
        run_response: RunOutput | None,
    ) -> CompactionRecord | None:
        """Summarize the history ahead of the kept tail, then store and cache the new record.

        Args:
            prefix: The system prefix, which shares the threshold with the kept tail.
            history: The full replayed history; the stored anchor is resolved in it.
            in_context: The history as it was about to be sent.
            tools: The request's tool definitions.
            tokens_before: The measured context size, when known.
            run_response: The run being served.

        Returns:
            The new record, or None when there was nothing to fold or the summary failed.
        """
        overhead = await asyncio.to_thread(count_tokens, prefix, tools, self.model_id)
        new_record = await self.compaction.acompact(
            history,
            session_id=self.session_id,
            db=None,
            previous=self._record,
            run_metrics=run_response.metrics if run_response is not None else None,
            tokens_before=tokens_before,
            run_id=run_response.run_id if run_response is not None else None,
            context_prefix=prefix,
            overhead_tokens=overhead,
            sent_messages=in_context,
        )
        if new_record is None:
            return None
        self._record = new_record
        stored = await self.db.upsert_compaction_record(self.session_id, self._stored())
        logger.info(
            "History compacted: session=%s model=%s messages=%d tokens %s -> %s stored=%s",
            self.session_id,
            self.model_id,
            new_record.messages_compacted,
            new_record.tokens_before,
            new_record.tokens_after,
            stored,
        )
        return new_record
