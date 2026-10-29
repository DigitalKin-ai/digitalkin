"""Tests for the ``mission_title`` custom event, emitted by a task started on ``RUN_STARTED``.

The trigger proposes a title through ``AgUiMixin.mission_title``; ``AgUiRunState.title`` reads
the mission record, asks the hook until the mission has a title and emits a ``CUSTOM`` frame
alongside the run. ``RUN_FINISHED`` waits for the task, ``RUN_ERROR`` cancels it.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from digitalkin.mixins import agui_mixin
from digitalkin.mixins.agui_mixin import AgUiMixin, AgUiRunState
from digitalkin.models.events import (
    AgentRunEvent,
    MissionTitle,
    MissionTitleEvent,
    RunCompletedEvent,
    RunStartedEvent,
)
from digitalkin.models.services.services import ServicesMode
from digitalkin.models.services.storage import MissionRecord
from digitalkin.models.settings.agui import AgUiSettings
from digitalkin.services.services_config import ServicesConfig

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


class _Untitled(AgUiMixin):
    """Trigger that keeps the default hook."""


class _Titled(AgUiMixin):
    """Trigger that proposes a fixed title and counts its calls."""

    def __init__(self, title: str | None) -> None:
        super().__init__()
        self.title = title
        self.calls = 0

    async def mission_title(self, context: Any) -> str | None:
        self.calls += 1
        return self.title


class _Gated(AgUiMixin):
    """Trigger whose hook blocks until ``release`` is set."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def mission_title(self, context: Any) -> str | None:
        self.entered.set()
        await self.release.wait()
        return "Late title"


class _Broken(AgUiMixin):
    """Trigger whose hook raises."""

    async def mission_title(self, context: Any) -> str | None:
        msg = "title model down"
        raise RuntimeError(msg)


def _ctx(mission: MissionRecord | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.callbacks.send_message = AsyncMock()
    ctx.session.current_ids = MagicMock(return_value={})
    ctx.storage.read = AsyncMock(return_value=None if mission is None else MagicMock(data=mission))
    ctx.storage.upsert = AsyncMock()
    ctx.agui_run = None
    return ctx


def _frames(ctx: MagicMock) -> list[dict[str, Any]]:
    return [call.args[0].model_dump(mode="json")["root"] for call in ctx.callbacks.send_message.await_args_list]


def _protocols(ctx: MagicMock) -> list[str]:
    return [frame["protocol"] for frame in _frames(ctx)]


async def _start(mixin: AgUiMixin, ctx: MagicMock) -> None:
    await mixin.send_message(ctx, RunStartedEvent(event=AgentRunEvent.RUN_STARTED, run_id="r1", thread_id="t1"))


async def _titled(mixin: AgUiMixin, ctx: MagicMock) -> None:
    await _start(mixin, ctx)
    await ctx.agui_run.title_task


async def _complete(mixin: AgUiMixin, ctx: MagicMock) -> None:
    await mixin.send_message(ctx, RunCompletedEvent(event=AgentRunEvent.RUN_COMPLETED, run_id="r1"))


class TestMissionTitleEmission:
    async def test_default_hook_starts_no_task(self) -> None:
        ctx = _ctx()

        await _start(_Untitled(), ctx)

        assert ctx.agui_run.title_task is None
        assert _protocols(ctx) == ["agui_run_started", "agui_state_snapshot"]
        ctx.storage.read.assert_not_awaited()

    async def test_first_title_is_emitted_and_stored(self) -> None:
        ctx = _ctx()

        await _titled(_Titled("Quarterly report"), ctx)

        frames = _frames(ctx)
        assert [frame["protocol"] for frame in frames] == ["agui_run_started", "agui_state_snapshot", "agui_custom"]
        assert frames[-1]["event"] == {
            "type": "CUSTOM",
            "name": "mission_title",
            "value": {"title": "Quarterly report"},
        }
        ctx.storage.read.assert_awaited_once_with("mission", "mission")
        ctx.storage.upsert.assert_awaited_once_with(
            "mission", "mission", {"title": "Quarterly report", "title_attempts": 1}
        )

    async def test_titled_mission_never_calls_hook(self) -> None:
        ctx = _ctx(MissionRecord(title="Kept title", title_attempts=1))
        mixin = _Titled("Other title")

        await _titled(mixin, ctx)

        assert mixin.calls == 0
        assert "agui_custom" not in _protocols(ctx)
        ctx.storage.upsert.assert_not_awaited()

    @pytest.mark.parametrize("title", [None, ""])
    async def test_empty_title_counts_attempt_and_emits_nothing(self, title: str | None) -> None:
        ctx = _ctx(MissionRecord(title_attempts=1))

        await _titled(_Titled(title), ctx)

        assert "agui_custom" not in _protocols(ctx)
        ctx.storage.upsert.assert_awaited_once_with("mission", "mission", {"title": None, "title_attempts": 2})

    async def test_untitled_mission_without_record_counts_first_attempt(self) -> None:
        ctx = _ctx()

        await _titled(_Titled(None), ctx)

        ctx.storage.upsert.assert_awaited_once_with("mission", "mission", {"title": None, "title_attempts": 1})

    @pytest.mark.parametrize("title", [None, ""])
    async def test_last_attempt_emits_null_title(self, title: str | None) -> None:
        ctx = _ctx(MissionRecord(title_attempts=2))

        await _titled(_Titled(title), ctx)

        assert _frames(ctx)[-1]["event"] == {"type": "CUSTOM", "name": "mission_title", "value": {"title": None}}
        ctx.storage.upsert.assert_awaited_once_with("mission", "mission", {"title": None, "title_attempts": 3})

    async def test_exhausted_mission_never_calls_hook(self) -> None:
        ctx = _ctx(MissionRecord(title_attempts=3))
        mixin = _Titled("Late title")

        await _titled(mixin, ctx)

        assert mixin.calls == 0
        assert "agui_custom" not in _protocols(ctx)
        ctx.storage.upsert.assert_not_awaited()

    async def test_failing_hook_counts_attempt(self) -> None:
        ctx = _ctx(MissionRecord(title_attempts=1))

        await _titled(_Broken(), ctx)

        assert "agui_custom" not in _protocols(ctx)
        ctx.storage.upsert.assert_awaited_once_with("mission", "mission", {"title": None, "title_attempts": 2})

    async def test_failing_hook_on_last_attempt_emits_null_title(self) -> None:
        ctx = _ctx(MissionRecord(title_attempts=2))

        await _titled(_Broken(), ctx)

        assert _frames(ctx)[-1]["event"] == {"type": "CUSTOM", "name": "mission_title", "value": {"title": None}}
        ctx.storage.upsert.assert_awaited_once_with("mission", "mission", {"title": None, "title_attempts": 3})

    async def test_invalid_title_counts_attempt(self) -> None:
        ctx = _ctx()

        await _titled(_Titled(42), ctx)  # type: ignore[arg-type]

        assert "agui_custom" not in _protocols(ctx)
        ctx.storage.upsert.assert_awaited_once_with("mission", "mission", {"title": None, "title_attempts": 1})

    async def test_slow_hook_times_out_and_counts_attempt(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(agui_mixin, "get_agui_settings", lambda: AgUiSettings(mission_title_timeout=0.01))
        ctx = _ctx()
        mixin = _Gated()

        await _start(mixin, ctx)
        await _complete(mixin, ctx)

        assert ctx.agui_run.title_task.exception() is None
        assert "agui_custom" not in _protocols(ctx)
        assert _protocols(ctx)[-1] == "agui_run_finished"
        ctx.storage.upsert.assert_awaited_once_with("mission", "mission", {"title": None, "title_attempts": 1})


class TestMissionTitleTask:
    async def test_run_started_does_not_wait_for_the_hook(self) -> None:
        ctx = _ctx()
        mixin = _Gated()

        await _start(mixin, ctx)
        await mixin.entered.wait()

        assert _protocols(ctx) == ["agui_run_started", "agui_state_snapshot"]
        assert not ctx.agui_run.title_task.done()
        mixin.release.set()
        await ctx.agui_run.title_task

    async def test_run_finished_waits_for_the_title(self) -> None:
        ctx = _ctx()
        mixin = _Gated()
        await _start(mixin, ctx)
        await mixin.entered.wait()

        completing = asyncio.create_task(_complete(mixin, ctx))
        await asyncio.sleep(0)
        assert "agui_run_finished" not in _protocols(ctx)
        mixin.release.set()
        await completing

        protocols = _protocols(ctx)
        assert protocols.index("agui_custom") < protocols.index("agui_run_finished")
        assert ctx.agui_run.state["run"]["status"] == "completed"

    async def test_run_error_cancels_the_title(self) -> None:
        ctx = _ctx()
        mixin = _Gated()
        await _start(mixin, ctx)
        await mixin.entered.wait()

        await ctx.agui_run.fail(ctx, "failed", "boom", "module_error")

        with pytest.raises(asyncio.CancelledError):
            await ctx.agui_run.title_task
        assert "agui_custom" not in _protocols(ctx)
        ctx.storage.upsert.assert_not_awaited()

    async def test_failing_hook_does_not_fail_the_run(self) -> None:
        ctx = _ctx()
        mixin = _Broken()

        await _start(mixin, ctx)
        await _complete(mixin, ctx)

        assert ctx.agui_run.title_task.exception() is None
        assert "agui_custom" not in _protocols(ctx)
        assert _protocols(ctx)[-1] == "agui_run_finished"
        assert ctx.agui_run.state["run"]["status"] == "completed"

    async def test_new_run_cancels_the_previous_title(self) -> None:
        ctx = _ctx()
        mixin = _Gated()
        await _start(mixin, ctx)
        await mixin.entered.wait()
        previous = ctx.agui_run.title_task

        await _start(mixin, ctx)

        with pytest.raises(asyncio.CancelledError):
            await previous
        assert ctx.agui_run.title_task is not previous
        mixin.release.set()
        await ctx.agui_run.title_task

    async def test_run_without_title_task_still_completes(self) -> None:
        ctx = _ctx()

        await _start(_Untitled(), ctx)
        await _complete(_Untitled(), ctx)

        assert not ctx.agui_run.open
        assert ctx.agui_run.state["run"]["status"] == "completed"
        assert _protocols(ctx)[-1] == "agui_run_finished"


class TestMissionTitleModels:
    def test_event_defaults_to_custom_mission_title(self) -> None:
        event = MissionTitleEvent(value=MissionTitle(title="Quarterly report"))

        assert event.event == AgentRunEvent.CUSTOM
        assert event.name == "mission_title"

    def test_empty_title_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            MissionTitle(title="")

    def test_null_title_marks_attempts_exhausted(self) -> None:
        assert MissionTitle(title=None).model_dump(mode="json") == {"title": None}

    def test_mission_record_defaults_to_untitled(self) -> None:
        assert MissionRecord() == MissionRecord(title=None, title_attempts=0)

    def test_mission_record_rejects_negative_attempts(self) -> None:
        with pytest.raises(ValidationError):
            MissionRecord(title_attempts=-1)

    def test_settings_defaults(self) -> None:
        settings = AgUiSettings()

        assert settings.mission_title_attempts == 3
        assert settings.mission_title_timeout == pytest.approx(15.0)


class TestMissionTitleStorage:
    """Round trip through the local storage strategy a module actually gets from ``ServicesConfig``."""

    @pytest.fixture
    def ctx(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        monkeypatch.chdir(tmp_path)
        ctx = _ctx()
        ctx.storage = ServicesConfig(mode=ServicesMode.LOCAL).init_strategy(
            "storage", "missions:m1", "setups:s1", "setup_versions:sv1"
        )
        return ctx

    async def test_title_is_kept_across_runs(self, ctx: MagicMock) -> None:
        first = _Titled("Quarterly report")
        await _titled(first, ctx)
        second = _Titled("Other title")
        await _titled(second, ctx)

        record = await ctx.storage.read("mission", "mission")
        assert record.data == MissionRecord(title="Quarterly report", title_attempts=1)
        assert (first.calls, second.calls) == (1, 0)
        assert [frame["event"]["value"] for frame in _frames(ctx) if frame["protocol"] == "agui_custom"] == [
            {"title": "Quarterly report"}
        ]

    async def test_attempts_accumulate_until_null_title(self, ctx: MagicMock) -> None:
        mixin = _Titled(None)
        for _ in range(4):
            await _titled(mixin, ctx)

        record = await ctx.storage.read("mission", "mission")
        assert record.data == MissionRecord(title=None, title_attempts=3)
        assert mixin.calls == 3
        assert [frame["event"]["value"] for frame in _frames(ctx) if frame["protocol"] == "agui_custom"] == [
            {"title": None}
        ]
