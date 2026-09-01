"""M4 regression: the producer writes ``seq`` + ``maxlen`` on every output xadd.

``ModuleRunner._on_output`` must carry a monotonic ``seq`` on each data entry
(so ``ProtoStreamReader`` can detect gaps) and bound the stream via ``maxlen``,
then write a single ``eos`` sentinel on ``stream.end``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from google.protobuf import struct_pb2

from digitalkin.core.task_manager.module_runner import ModuleRunner
from digitalkin.models.settings.gateway import get_gateway_settings


class _RecordingRedis:
    """Minimal async Redis double that records xadd/expire calls."""

    def __init__(self) -> None:
        self.xadds: list[tuple[str, dict[str, Any], int | None]] = []
        self.expires: list[tuple[str, int]] = []

    async def xadd(self, name: str, fields: dict[str, Any], *, maxlen: int | None = None) -> bytes:
        self.xadds.append((name, fields, maxlen))
        return b"0-1"

    async def expire(self, name: str, seconds: int) -> bool:
        self.expires.append((name, seconds))
        return True


class _Out:
    """Stand-in for a module output model exposing ``model_dump``."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def model_dump(self, mode: str = "json") -> dict[str, Any]:  # noqa: ARG002
        return self._payload


async def test_on_output_writes_seq_and_maxlen() -> None:
    get_gateway_settings.cache_clear()
    redis = _RecordingRedis()

    setup_version = MagicMock(content={}, setup_id="setups:s1", id="setup_versions:v1")
    servicer = MagicMock()
    servicer.resolve_setup = AsyncMock(return_value=setup_version)
    servicer.module_class.create_setup_model = AsyncMock(return_value=MagicMock())
    servicer.get_tool_cache = MagicMock(return_value=MagicMock())  # non-None → skip cache build
    servicer.module_class.create_input_model = MagicMock(return_value=MagicMock())

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: ARG001
        # Hand back the _on_output callback unchanged so run_instance drives it.
        return MagicMock(), kwargs["job_id"], kwargs["callback"]

    async def _run_instance(*, callback: Any, **_: Any) -> None:
        await callback(_Out({"root": {"protocol": "data", "value": 1}}))
        await callback(_Out({"root": {"protocol": "data", "value": 2}}))
        await callback(_Out({"root": {"protocol": "stream.end"}}))

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = _run_instance

    runner = ModuleRunner(redis_client=redis, servicer=servicer)  # type: ignore[arg-type]

    async def _on_fatal(code: str, message: str) -> None:  # noqa: ARG001
        return

    with patch("digitalkin.core.task_manager.module_runner.TaskProfiler"):
        await runner.run(
            struct_pb2.Struct(),
            task_id="t-m4",
            setup_id="setups:s1",
            mission_id="missions:m1",
            on_fatal=_on_fatal,
        )

    maxlen = get_gateway_settings().stream.redis_stream_maxlen
    data_xadds = [x for x in redis.xadds if "pb" in x[1]]
    # Two data outputs, each seq'd 1..N and bounded by maxlen.
    assert [fields["seq"] for _, fields, _ in data_xadds] == ["1", "2"]
    assert all(ml == maxlen for _, _, ml in data_xadds)
    assert all(isinstance(fields["pb"], bytes) for _, fields, _ in data_xadds)
    # stream.end writes exactly one eos sentinel (no seq, unbounded).
    eos = [x for x in redis.xadds if x[1].get("eos") == b"true"]
    assert len(eos) == 1
    assert eos[0][2] is None


async def test_servicer_setup_is_borrowed_into_module_context() -> None:
    """Constraint: the runner hands the servicer's setup service to preload_instance.

    The wiring must happen inside preload_instance (before prepare()/initialize()
    builds the toolkits), so the runner passes the strategy + invalidation hook
    as arguments instead of assigning context.setup after the fact.
    """
    get_gateway_settings.cache_clear()
    redis = _RecordingRedis()

    setup_version = MagicMock(content={}, setup_id="setups:s1", id="setup_versions:v1")
    servicer = MagicMock()
    servicer.resolve_setup = AsyncMock(return_value=setup_version)
    servicer.module_class.create_setup_model = AsyncMock(return_value=MagicMock())
    servicer.get_tool_cache = MagicMock(return_value=MagicMock())
    servicer.module_class.create_input_model = MagicMock(return_value=MagicMock())

    module = MagicMock()
    preload_kwargs: dict[str, Any] = {}

    async def _preload(setup_data: Any, **kwargs: Any) -> tuple[Any, str, Any]:  # noqa: ARG001
        preload_kwargs.update(kwargs)
        return module, kwargs["job_id"], kwargs["callback"]

    async def _run_instance(**_: Any) -> None:
        return

    servicer.job_manager.preload_instance = _preload
    servicer.job_manager.run_instance = _run_instance

    runner = ModuleRunner(redis_client=redis, servicer=servicer)  # type: ignore[arg-type]

    async def _on_fatal(code: str, message: str) -> None:  # noqa: ARG001
        return

    with patch("digitalkin.core.task_manager.module_runner.TaskProfiler"):
        await runner.run(
            struct_pb2.Struct(),
            task_id="t-setup",
            setup_id="setups:s1",
            mission_id="missions:m1",
            on_fatal=_on_fatal,
        )

    assert preload_kwargs["setup"] is servicer.setup
    assert preload_kwargs["invalidate_setup"] is servicer.invalidate_setup_cache


async def test_preload_wires_setup_before_prepare() -> None:
    """SetupTools depends on context.setup being visible inside initialize().

    ``prepare()`` (which runs ``initialize()``) must observe the borrowed setup
    strategy and the invalidation callback — wiring them after preload would
    silently drop SetupTools from every agent built in initialize().
    """
    from types import SimpleNamespace

    from digitalkin.core.job_manager.single_job_manager import SingleJobManager

    mgr = SingleJobManager.__new__(SingleJobManager)
    mgr.module_class = MagicMock()
    mgr._redis_task_manager = MagicMock()

    module = MagicMock()
    module.context = SimpleNamespace(callbacks=SimpleNamespace(), setup=None, task_manager=None)
    seen: dict[str, Any] = {}

    async def _prepare(setup_data: Any, callback: Any) -> None:  # noqa: ARG001
        seen["setup"] = module.context.setup
        seen["invalidate"] = vars(module.context.callbacks).get("invalidate_setup")

    module.prepare = _prepare
    setup_strategy = object()
    invalidate = MagicMock()

    with patch("digitalkin.core.job_manager.single_job_manager.ModuleFactory") as factory:
        factory.create_module_instance.return_value = module
        await mgr.preload_instance(
            MagicMock(),
            mission_id="missions:m1",
            setup_id="setups:s1",
            setup_version_id="setup_versions:v1",
            callback=AsyncMock(),
            setup=setup_strategy,
            invalidate_setup=invalidate,
        )

    assert seen["setup"] is setup_strategy
    assert seen["invalidate"] is invalidate
