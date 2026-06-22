"""Per-task profiling wrapper for VizTracer, Yappi, and Pyinstrument."""

import datetime
import io
import os
from pathlib import Path
from typing import Any

from digitalkin.logger import logger
from digitalkin.models.settings.profiling import ProfilerMode, get_profiling_settings


class TaskProfiler:
    """Per-task profiling wrapper. Zero-cost when mode is NONE.

    Wraps VizTracer, Yappi, or Pyinstrument around a task's lifecycle.
    All exceptions are caught internally — profiler failure never crashes a task.

    Yappi profiles the entire process, not individual tasks. When multiple tasks
    run concurrently, yappi stats reflect all of them.
    """

    def __init__(self, task_id: str, mode: ProfilerMode, output_dir: str) -> None:
        """Initialize the task profiler.

        Args:
            task_id: Unique identifier for the task being profiled.
            mode: Which profiler backend to use.
            output_dir: Directory to write profiling output files.
        """
        self._task_id = task_id
        self._mode = mode
        self._output_dir = output_dir
        self._profiler: Any = None
        self._yappi_started: bool = False

    @staticmethod
    def _rotate_profiles(output_dir: str, keep_n: int, suffixes: tuple[str, ...]) -> None:
        """Trim ``output_dir`` to the most recent ``keep_n`` files by mtime.

        Args:
            output_dir: Directory containing profile files.
            keep_n: Number of files to keep. ``<= 0`` disables rotation.
            suffixes: File extensions to include in rotation (e.g. ``(".html",)``).
        """
        if keep_n <= 0:
            return
        try:
            candidates = [p for p in Path(output_dir).iterdir() if p.is_file() and p.suffix in suffixes]
        except OSError:
            return
        if len(candidates) <= keep_n:
            return
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for stale in candidates[keep_n:]:
            try:
                stale.unlink()
            except OSError:  # noqa: PERF203
                logger.debug("Profiler rotation: could not delete %s", stale)

    def start(self) -> None:
        """Start the profiler. No-op when mode is NONE."""
        if self._mode == ProfilerMode.NONE:
            return

        try:  # noqa: PLW0717
            os.makedirs(self._output_dir, exist_ok=True)

            if self._mode == ProfilerMode.VIZTRACER:
                from viztracer import VizTracer

                self._profiler = VizTracer(output_file="", verbose=0)
                self._profiler.start()

            elif self._mode == ProfilerMode.YAPPI:
                import yappi

                yappi.start()
                self._yappi_started = True

            elif self._mode == ProfilerMode.PYINSTRUMENT:
                from pyinstrument import Profiler

                self._profiler = Profiler(async_mode="enabled")
                self._profiler.start()

        except ImportError:
            logger.warning("Profiler %s requested but package not installed, skipping", self._mode.value)
            self._profiler = None
            self._yappi_started = False
        except Exception:
            logger.exception("Failed to start profiler %s for task %s", self._mode.value, self._task_id)
            self._profiler = None
            self._yappi_started = False

    def stop(self) -> None:
        """Stop the profiler, log summary, and save output. No-op when mode is NONE."""
        if self._mode == ProfilerMode.NONE:
            return
        if self._profiler is None and not self._yappi_started:
            return

        try:  # noqa: PLW0717
            timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%f")
            base = f"{self._task_id}_{timestamp}"

            if self._mode == ProfilerMode.VIZTRACER:
                self._profiler.stop()
                path = os.path.join(self._output_dir, f"{base}.json")
                self._profiler.save(path)
                event_count = self._profiler.parse()
                logger.info("VizTracer profile saved: %s (%d events)", path, event_count)

            elif self._mode == ProfilerMode.YAPPI:
                import yappi

                yappi.stop()
                stats = yappi.get_func_stats()
                path = os.path.join(self._output_dir, f"{base}.pstats")
                stats.save(path, type="pstat")
                logger.info("Yappi profile saved: %s", path)
                buf = io.StringIO()
                stats.sort("ttot", "desc").print_all(
                    out=buf,
                    columns={0: ("name", 60), 1: ("ncall", 10), 2: ("ttot", 8), 3: ("tsub", 8)},
                )
                output = buf.getvalue()
                if output.strip():
                    logger.info("Yappi top functions:\n%s", output.rstrip())
                yappi.clear_stats()

            elif self._mode == ProfilerMode.PYINSTRUMENT:
                self._profiler.stop()
                path = os.path.join(self._output_dir, f"{base}.html")
                Path(path).write_text(self._profiler.output_html(), encoding="utf-8")
                logger.info("Pyinstrument profile saved: %s", path)
                logger.info("Pyinstrument summary:\n%s", self._profiler.output_text())
                self._rotate_profiles(self._output_dir, get_profiling_settings().profiler_keep_n, (".html",))

        except Exception:
            logger.exception("Failed to stop/save profiler %s for task %s", self._mode.value, self._task_id)
        finally:
            self._profiler = None
            self._yappi_started = False
