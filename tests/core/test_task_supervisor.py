"""Unit tests for ``log_unhandled`` — the shared task-supervisor helper."""

from __future__ import annotations

import asyncio

import pytest

from digitalkin.core.resilience.task_supervisor import log_unhandled

pytestmark = [pytest.mark.timeout(10)]


async def test_logs_unhandled_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """A monitored task that raises must produce a logged error line."""
    from digitalkin.core.resilience import task_supervisor as ts_mod

    calls: list[str] = []
    monkeypatch.setattr(
        ts_mod.logger,
        "error",
        lambda msg, *args, **_kw: calls.append(msg % args if args else msg),
    )

    async def _boom() -> None:
        raise RuntimeError("kaboom")

    task = asyncio.create_task(_boom(), name="boom_task")
    task.add_done_callback(log_unhandled)
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0)

    assert any("boom_task" in m and "kaboom" in m for m in calls), (
        f"expected error log mentioning task name + exception, got: {calls}"
    )
    # done-callback already retrieved the exception → no asyncio warning fires.
    assert task.exception() is not None


async def test_silent_on_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cancelled tasks are routine — no error log."""
    from digitalkin.core.resilience import task_supervisor as ts_mod

    calls: list[str] = []
    monkeypatch.setattr(
        ts_mod.logger,
        "error",
        lambda msg, *args, **_kw: calls.append(msg % args if args else msg),
    )

    async def _wait_forever() -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(_wait_forever(), name="cancel_task")
    task.add_done_callback(log_unhandled)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0)

    assert not calls, f"cancellation should be silent, got: {calls}"


async def test_silent_on_clean_return(monkeypatch: pytest.MonkeyPatch) -> None:
    """A task that returns normally produces no log."""
    from digitalkin.core.resilience import task_supervisor as ts_mod

    calls: list[str] = []
    monkeypatch.setattr(
        ts_mod.logger,
        "error",
        lambda msg, *args, **_kw: calls.append(msg % args if args else msg),
    )

    async def _ok() -> None:
        return None

    task = asyncio.create_task(_ok(), name="ok_task")
    task.add_done_callback(log_unhandled)
    await task
    await asyncio.sleep(0)

    assert not calls, f"clean return should be silent, got: {calls}"
