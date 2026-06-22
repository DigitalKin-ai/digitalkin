"""ModuleRunner reads profiling config at call time, not import time.

Regression guard for the P2.4 fix that replaced the module-level
``_PROFILING = ProfilingSettings()`` (frozen at import) with
``get_profiling_settings()`` (read on every ``run``). The env override
below is set *after* import; under the old frozen global the profiler
would have been ``ProfilerMode.NONE``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.protobuf import struct_pb2

from digitalkin.models.settings.profiling import ProfilerMode, get_profiling_settings
from tests.gateway.test_dial_consumer import SKIP_NO_FAKEREDIS, _FakeRedisClient


@SKIP_NO_FAKEREDIS
class TestModuleRunnerProfilingOverride:
    async def test_env_override_honored_at_runtime(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.core.task_manager.module_runner import ModuleRunner

        monkeypatch.setenv("DIGITALKIN_PROFILER", "pyinstrument")
        get_profiling_settings.cache_clear()

        redis = _FakeRedisClient()
        try:
            servicer = MagicMock()
            servicer.resolve_setup = AsyncMock(side_effect=RuntimeError("stop early"))

            runner = ModuleRunner(redis_client=redis, servicer=servicer)  # type: ignore[arg-type]

            async def _on_fatal(code: str, message: str) -> None:
                return

            with patch("digitalkin.core.task_manager.module_runner.TaskProfiler") as profiler_cls:
                await runner.run(
                    struct_pb2.Struct(),
                    task_id="task_prof",
                    setup_id="setups:s1",
                    mission_id="missions:m1",
                    on_fatal=_on_fatal,
                )

            profiler_cls.assert_called_once()
            assert profiler_cls.call_args.kwargs["mode"] == ProfilerMode.PYINSTRUMENT
        finally:
            await redis.close()

    async def test_default_mode_is_none_without_env(self) -> None:
        from digitalkin.core.task_manager.module_runner import ModuleRunner

        get_profiling_settings.cache_clear()

        redis = _FakeRedisClient()
        try:
            servicer = MagicMock()
            servicer.resolve_setup = AsyncMock(side_effect=RuntimeError("stop early"))

            runner = ModuleRunner(redis_client=redis, servicer=servicer)  # type: ignore[arg-type]

            async def _on_fatal(code: str, message: str) -> None:
                return

            with patch("digitalkin.core.task_manager.module_runner.TaskProfiler") as profiler_cls:
                await runner.run(
                    struct_pb2.Struct(),
                    task_id="task_prof",
                    setup_id="setups:s1",
                    mission_id="missions:m1",
                    on_fatal=_on_fatal,
                )

            assert profiler_cls.call_args.kwargs["mode"] == ProfilerMode.NONE
        finally:
            await redis.close()
