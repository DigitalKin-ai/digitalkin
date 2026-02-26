"""Tests for TaskProfiler."""

import os
import sys
import time
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from digitalkin.core.profiling.task_profiler import ProfilerMode, TaskProfiler


class TestProfilerMode:
    """Tests for ProfilerMode enum."""

    def test_none_is_default(self):
        assert ProfilerMode("none") == ProfilerMode.NONE

    def test_all_modes_from_string(self):
        assert ProfilerMode("viztracer") == ProfilerMode.VIZTRACER
        assert ProfilerMode("yappi") == ProfilerMode.YAPPI
        assert ProfilerMode("pyinstrument") == ProfilerMode.PYINSTRUMENT

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError):
            ProfilerMode("invalid")


class TestTaskProfilerNoneMode:
    """Tests for zero-cost NONE mode."""

    def test_no_profiler_created(self):
        p = TaskProfiler("task_1", ProfilerMode.NONE, "/tmp/profiles")
        assert p._profiler is None

    def test_start_is_noop(self):
        p = TaskProfiler("task_1", ProfilerMode.NONE, "/tmp/profiles")
        p.start()
        assert p._profiler is None

    def test_stop_is_noop(self):
        p = TaskProfiler("task_1", ProfilerMode.NONE, "/tmp/profiles")
        p.stop()
        assert p._profiler is None


class TestTaskProfilerImportError:
    """Tests for graceful degradation when profiler packages are missing."""

    def test_viztracer_import_error(self):
        with patch.dict(sys.modules, {"viztracer": None}):
            p = TaskProfiler("task_1", ProfilerMode.VIZTRACER, "/tmp/profiles")
            p.start()
            assert p._profiler is None

    def test_yappi_import_error(self):
        with patch.dict(sys.modules, {"yappi": None}):
            p = TaskProfiler("task_1", ProfilerMode.YAPPI, "/tmp/profiles")
            p.start()
            assert p._profiler is None
            assert p._yappi_started is False

    def test_pyinstrument_import_error(self):
        with patch.dict(sys.modules, {"pyinstrument": None}):
            p = TaskProfiler("task_1", ProfilerMode.PYINSTRUMENT, "/tmp/profiles")
            p.start()
            assert p._profiler is None


class TestTaskProfilerVizTracer:
    """Tests for VizTracer profiler mode with mocked imports."""

    def test_start_and_stop(self, tmp_path):
        mock_tracer = MagicMock()
        mock_tracer.parse.return_value = 42

        mock_module = ModuleType("viztracer")
        mock_module.VizTracer = MagicMock(return_value=mock_tracer)

        with patch.dict(sys.modules, {"viztracer": mock_module}):
            p = TaskProfiler("task_viz", ProfilerMode.VIZTRACER, str(tmp_path))
            p.start()

            assert p._profiler is mock_tracer
            mock_tracer.start.assert_called_once()

            p.stop()
            mock_tracer.stop.assert_called_once()
            mock_tracer.save.assert_called_once()
            mock_tracer.parse.assert_called_once()
            saved_path = mock_tracer.save.call_args[0][0]
            assert saved_path.startswith(str(tmp_path))
            assert saved_path.endswith(".json")
            assert p._profiler is None


class TestTaskProfilerYappi:
    """Tests for Yappi profiler mode with mocked imports."""

    def test_start_and_stop(self, tmp_path):
        mock_stats = MagicMock()
        mock_stats.sort.return_value = mock_stats

        mock_yappi = ModuleType("yappi")
        mock_yappi.start = MagicMock()
        mock_yappi.stop = MagicMock()
        mock_yappi.get_func_stats = MagicMock(return_value=mock_stats)
        mock_yappi.clear_stats = MagicMock()

        with patch.dict(sys.modules, {"yappi": mock_yappi}):
            p = TaskProfiler("task_yap", ProfilerMode.YAPPI, str(tmp_path))
            p.start()

            assert p._yappi_started is True
            assert p._profiler is None  # yappi module NOT stored as _profiler
            mock_yappi.start.assert_called_once()

            p.stop()
            mock_yappi.stop.assert_called_once()
            mock_yappi.get_func_stats.assert_called_once()
            mock_stats.save.assert_called_once()
            saved_path = mock_stats.save.call_args[0][0]
            assert saved_path.endswith(".pstats")
            mock_yappi.clear_stats.assert_called_once()
            assert p._yappi_started is False

    def test_no_clear_stats_on_start(self, tmp_path):
        """Verify clear_stats is NOT called during start (race condition fix)."""
        mock_yappi = ModuleType("yappi")
        mock_yappi.start = MagicMock()
        mock_yappi.stop = MagicMock()
        mock_yappi.clear_stats = MagicMock()
        mock_yappi.get_func_stats = MagicMock(return_value=MagicMock())

        with patch.dict(sys.modules, {"yappi": mock_yappi}):
            p = TaskProfiler("task_race", ProfilerMode.YAPPI, str(tmp_path))
            p.start()
            mock_yappi.clear_stats.assert_not_called()


class TestTaskProfilerPyinstrument:
    """Tests for Pyinstrument profiler mode with mocked imports."""

    def test_start_and_stop(self, tmp_path):
        mock_profiler = MagicMock()
        mock_profiler.output_html.return_value = "<html>profile</html>"
        mock_profiler.output_text.return_value = "summary"

        mock_module = ModuleType("pyinstrument")
        mock_module.Profiler = MagicMock(return_value=mock_profiler)

        with patch.dict(sys.modules, {"pyinstrument": mock_module}):
            p = TaskProfiler("task_pi", ProfilerMode.PYINSTRUMENT, str(tmp_path))
            p.start()

            assert p._profiler is mock_profiler
            mock_profiler.start.assert_called_once()

            p.stop()
            mock_profiler.stop.assert_called_once()
            # Verify HTML file was written
            files = list(tmp_path.iterdir())
            assert len(files) == 1
            assert files[0].suffix == ".html"
            assert files[0].read_text() == "<html>profile</html>"
            assert p._profiler is None


class TestTaskProfilerOutputDir:
    """Tests for output directory creation."""

    def test_creates_output_dir_on_start(self, tmp_path):
        """Verify output dir is created during start(), not stop()."""
        output_dir = str(tmp_path / "nested" / "dir")
        mock_profiler = MagicMock()
        mock_profiler.output_html.return_value = "<html></html>"
        mock_profiler.output_text.return_value = ""

        mock_module = ModuleType("pyinstrument")
        mock_module.Profiler = MagicMock(return_value=mock_profiler)

        with patch.dict(sys.modules, {"pyinstrument": mock_module}):
            p = TaskProfiler("task_dir", ProfilerMode.PYINSTRUMENT, output_dir)
            p.start()
            # Dir exists after start, before stop
            assert os.path.isdir(output_dir)
            p.stop()

        assert os.path.isdir(output_dir)


class TestTaskProfilerTimestampUniqueness:
    """Tests for microsecond timestamp resolution."""

    def test_microsecond_timestamps_differ(self, tmp_path):
        """Two rapid stop() calls produce different filenames."""
        mock_profiler1 = MagicMock()
        mock_profiler1.output_html.return_value = "<html>1</html>"
        mock_profiler1.output_text.return_value = ""
        mock_profiler2 = MagicMock()
        mock_profiler2.output_html.return_value = "<html>2</html>"
        mock_profiler2.output_text.return_value = ""

        mock_module = ModuleType("pyinstrument")
        mock_module.Profiler = MagicMock(side_effect=[mock_profiler1, mock_profiler2])

        with patch.dict(sys.modules, {"pyinstrument": mock_module}):
            p1 = TaskProfiler("task_ts", ProfilerMode.PYINSTRUMENT, str(tmp_path))
            p1.start()
            p1.stop()

            # Small delay to ensure different microsecond
            time.sleep(0.001)

            p2 = TaskProfiler("task_ts", ProfilerMode.PYINSTRUMENT, str(tmp_path))
            p2.start()
            p2.stop()

        files = sorted(tmp_path.iterdir())
        assert len(files) == 2
        assert files[0].name != files[1].name


class TestTaskProfilerExceptionSafety:
    """Tests that profiler exceptions never propagate."""

    def test_start_exception_caught(self):
        mock_module = ModuleType("viztracer")
        mock_module.VizTracer = MagicMock(side_effect=RuntimeError("boom"))

        with patch.dict(sys.modules, {"viztracer": mock_module}):
            p = TaskProfiler("task_err", ProfilerMode.VIZTRACER, "/tmp/profiles")
            p.start()  # Should not raise
            assert p._profiler is None

    def test_stop_exception_caught(self, tmp_path):
        mock_profiler = MagicMock()
        mock_profiler.stop.side_effect = RuntimeError("save boom")

        p = TaskProfiler("task_err", ProfilerMode.VIZTRACER, str(tmp_path))
        p._profiler = mock_profiler
        p.stop()  # Should not raise
        assert p._profiler is None


class TestYappiLogOutput:
    """Tests that yappi stats are actually logged via StringIO."""

    def test_stats_logged_via_stringio(self, tmp_path):
        """Verify yappi stats output is captured and logged."""
        mock_stats = MagicMock()
        mock_stats.sort.return_value = mock_stats

        def fake_print_all(out, columns):
            out.write("func1  10  0.5  0.3\n")
            out.write("func2   5  0.2  0.1\n")

        mock_stats.print_all = fake_print_all

        mock_yappi = ModuleType("yappi")
        mock_yappi.start = MagicMock()
        mock_yappi.stop = MagicMock()
        mock_yappi.get_func_stats = MagicMock(return_value=mock_stats)
        mock_yappi.clear_stats = MagicMock()

        with (
            patch.dict(sys.modules, {"yappi": mock_yappi}),
            patch("digitalkin.core.profiling.task_profiler.logger") as mock_logger,
        ):
            p = TaskProfiler("task_log", ProfilerMode.YAPPI, str(tmp_path))
            p.start()
            p.stop()

            # Find the call that logs yappi top functions
            info_calls = [c for c in mock_logger.info.call_args_list if "Yappi top functions" in str(c)]
            assert len(info_calls) == 1
            logged_text = info_calls[0][0][1]
            assert "func1" in logged_text
            assert "func2" in logged_text
