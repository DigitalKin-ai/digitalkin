"""Phase 7.C — TaskProfiler rotates profile output files."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import patch


def test_rotation_keeps_n_most_recent(tmp_path: Path) -> None:
    """Older files are deleted; the N most recent (by mtime) survive."""
    from digitalkin.core.profiling.task_profiler import _rotate_profiles

    # Create 7 fake .html files with increasing mtime stamps.
    files = []
    for i in range(7):
        p = tmp_path / f"task-{i}_2026.html"
        p.write_text("<html/>")
        # Force a unique mtime per file (older files first).
        ts = time.time() - (7 - i) * 60
        p.touch()
        # Set explicit access/mod times so the test is deterministic.
        import os as _os
        _os.utime(p, (ts, ts))
        files.append(p)

    _rotate_profiles(str(tmp_path), keep_n=3, suffixes=(".html",))

    survivors = sorted(p.name for p in tmp_path.iterdir())
    # Most recent 3 = task-4, task-5, task-6.
    assert survivors == ["task-4_2026.html", "task-5_2026.html", "task-6_2026.html"]


def test_rotation_disabled_when_keep_n_zero(tmp_path: Path) -> None:
    from digitalkin.core.profiling.task_profiler import _rotate_profiles

    for i in range(5):
        (tmp_path / f"f-{i}.html").write_text("<html/>")
    _rotate_profiles(str(tmp_path), keep_n=0, suffixes=(".html",))
    assert sum(1 for _ in tmp_path.iterdir()) == 5


def test_rotation_only_targets_matching_suffix(tmp_path: Path) -> None:
    from digitalkin.core.profiling.task_profiler import _rotate_profiles

    for i in range(5):
        (tmp_path / f"f-{i}.html").write_text("<html/>")
    keep = tmp_path / "f-keep.json"
    keep.write_text("{}")

    _rotate_profiles(str(tmp_path), keep_n=2, suffixes=(".html",))

    # The .json file is preserved regardless; only .html is trimmed.
    names = sorted(p.suffix for p in tmp_path.iterdir())
    assert names.count(".html") == 2
    assert names.count(".json") == 1


def test_pyinstrument_save_triggers_rotation(tmp_path: Path) -> None:
    """End-to-end: TaskProfiler.stop in PYINSTRUMENT mode invokes rotation."""
    from digitalkin.core.profiling.task_profiler import ProfilerMode, TaskProfiler

    # Pre-populate the dir with old .html files.
    for i in range(5):
        (tmp_path / f"old-{i}.html").write_text("<html/>")

    profiler = TaskProfiler(task_id="task-rot", mode=ProfilerMode.PYINSTRUMENT, output_dir=str(tmp_path))

    # Stub out the actual pyinstrument backend so the test is hermetic.
    class _FakeProfiler:
        def start(self) -> None: ...
        def stop(self) -> None: ...
        def output_html(self) -> str: return "<html>profile</html>"
        def output_text(self) -> str: return "summary"

    profiler._profiler = _FakeProfiler()  # noqa: SLF001

    with patch("digitalkin.core.profiling.task_profiler.PROFILER_KEEP_N", 3):
        profiler.stop()

    # The new file plus 2 of the old ones (3 total) survive.
    surviving = list(tmp_path.glob("*.html"))
    assert len(surviving) == 3
