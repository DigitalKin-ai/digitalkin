"""Per-task profiling wrapper for VizTracer, Yappi, and Pyinstrument."""

import datetime
import io
import logging
import os
from enum import Enum
from pathlib import Path
from typing import Any

from digitalkin.logger import logger


class ProfilerMode(str, Enum):
    """Profiler backend selection."""

    NONE = "none"
    VIZTRACER = "viztracer"
    YAPPI = "yappi"
    PYINSTRUMENT = "pyinstrument"


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

    def start(self) -> None:
        """Start the profiler. No-op when mode is NONE."""
        if self._mode == ProfilerMode.NONE:
            return

        try:
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

        try:
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

        except Exception:
            logger.exception("Failed to stop/save profiler %s for task %s", self._mode.value, self._task_id)
        finally:
            self._profiler = None
            self._yappi_started = False


class _LogWriter:
    """Adapter to redirect yappi print_all output to a logger."""

    def __init__(self, target_logger: logging.Logger, level: int) -> None:
        """Initialize the log writer.

        Args:
            target_logger: Logger to write to.
            level: Logging level for output.
        """
        self._logger = target_logger
        self._level = level
        self._buffer: list[str] = []

    def write(self, text: str) -> None:
        """Buffer text lines for logging.

        Args:
            text: Text to write.
        """
        if text and text.strip():
            self._buffer.append(text.rstrip())

    def flush(self) -> None:
        """Flush buffered lines to the logger."""
        if self._buffer:
            self._logger.log(self._level, "Yappi top functions:\n%s", "\n".join(self._buffer))
            self._buffer.clear()
