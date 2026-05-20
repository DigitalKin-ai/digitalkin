"""Module runner — invoked by the dial-back orchestrator.

Receives the consumer's first input Struct (the query), resolves the
setup, builds the input/setup models, looks up the tool cache, and
starts the module via the job manager. Each module output goes to the
task's Redis Stream via the ``_on_output`` callback so the gateway's
``_consume_from_redis`` can drain it back to the consumer.

This used to live inline in ``TaskDispatcher._handle_dispatch``. The
dispatcher (separate XADD/XREAD bus) is gone in embedded mode — the
dial-back BiDi handler is the sole orchestrator and calls into here
directly when the consumer's first reply lands.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format, struct_pb2
from pydantic import ValidationError

from digitalkin.core.exceptions import BackpressureTimeoutError
from digitalkin.core.profiling.step_timer import StepTimer
from digitalkin.core.profiling.task_profiler import TaskProfiler
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.stream_error_codes import StreamErrorCode
from digitalkin.models.settings.profiling import ProfilerMode, ProfilingSettings

# Singleton — read profiler env once at import.
_PROFILING = ProfilingSettings()

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.grpc_servers.module_servicer import ModuleServicer


class ModuleRunner:
    """Run one task end-to-end: setup → module instance → output drain.

    Stateless aside from the injected ``servicer`` and ``redis_client``;
    one instance is shared per ``GatewayServicer`` (created in
    ``module_server._register_gateway_servicer`` and passed to the
    gateway).
    """

    _redis_client: RedisClient
    _servicer: ModuleServicer

    def __init__(self, redis_client: RedisClient, servicer: ModuleServicer) -> None:
        """Initialize the runner.

        Args:
            redis_client: Redis used to write module outputs to the task stream.
            servicer: ModuleServicer for setup resolution and job management.
        """
        self._redis_client = redis_client
        self._servicer = servicer

    async def run(
        self,
        query: struct_pb2.Struct,
        *,
        task_id: str,
        setup_id: str,
        mission_id: str,
        on_fatal: Callable[[str, str], Awaitable[None]],
    ) -> None:
        """Execute one module task to completion.

        Args:
            query: The first input Struct received from the consumer
                (delivered via the dial-back BiDi).
            task_id: Task identifier; output stream is ``task:{task_id}:stream``.
            setup_id: Setup identifier (used for setup resolution + tool cache).
            mission_id: Mission identifier (carried for logging context).
            on_fatal: Async callback invoked on any unhandled exception
                with ``(StreamErrorCode value, message)``. The caller is
                responsible for emitting the corresponding ``stream.error``
                + EOS to the task's Redis stream.
        """
        log_extra = {"task_id": task_id, "setup_id": setup_id, "mission_id": mission_id}
        stream_key = f"task:{task_id}:stream"
        timer = StepTimer()

        # Per-task profiler — zero-cost when DIGITALKIN_PROFILER=none.
        # Runs over the whole module lifecycle so the saved profile shows
        # setup/init/run/output broken down with line-level resolution.
        profiler_mode = (
            ProfilerMode(_PROFILING.profiler)
            if _PROFILING.profiler in {p.value for p in ProfilerMode}
            else ProfilerMode.NONE
        )
        profiler = TaskProfiler(task_id=task_id, mode=profiler_mode, output_dir=_PROFILING.profile_output_dir)
        profiler.start()

        try:
            timer.mark("entry")

            setup_version = await self._servicer._resolve_setup(setup_id, mission_id)  # noqa: SLF001
            timer.mark("setup_resolve")

            setup_data = await self._servicer.module_class.create_setup_model(setup_version.content)
            timer.mark("setup_model")

            tool_cache = self._servicer.get_tool_cache(setup_version.setup_id)
            timer.mark("tool_cache_lookup")

            # L8: measure latency from runner entry to first producer output.
            runner_start_ns = time.perf_counter_ns()
            first_logged = False

            async def _on_output(output_data: Any) -> None:
                nonlocal first_logged
                data = output_data.model_dump(mode="json")
                if data.get("root", {}).get("protocol") == "stream.end":
                    t_eos_write_start = time.perf_counter_ns()
                    await self._redis_client.xadd(stream_key, {"eos": b"true"})
                    await self._redis_client.expire(stream_key, 60)
                    t_eos_write_end = time.perf_counter_ns()
                    logger.info(
                        "[close-debug] producer_eos_write: xadd_expire=%.2fms t_done_ns=%d task_id=%s",
                        (t_eos_write_end - t_eos_write_start) / 1e6,
                        t_eos_write_end,
                        task_id,
                    )
                    return
                s = struct_pb2.Struct()
                s.update(data)
                await self._redis_client.xadd(stream_key, {"pb": s.SerializeToString()})
                # Stream-key TTL safety net: arm an EXPIRE on first XADD so a
                # producer crash before stream.end doesn't leak the key. The
                # final EXPIRE on stream.end shortens it to the post-EOS TTL.
                if not first_logged:
                    elapsed_ms = (time.perf_counter_ns() - runner_start_ns) / 1e6
                    logger.info(
                        "[lat-audit] producer_first_byte_to_redis: %.1fms task_id=%s",
                        elapsed_ms,
                        task_id,
                        extra=log_extra,
                    )
                    await self._redis_client.expire(stream_key, 600)
                    first_logged = True

            # Convert input first (sync, sub-ms), then preload directly.
            # preload_instance runs the module's idempotent prepare() so the
            # eventual start() short-circuits past initialize().
            top_level_keys = list(query.fields.keys())
            query_byte_size = query.ByteSize()
            logger.info(
                "[input-debug] inbound Struct: top_keys=%s wire_bytes=%d",
                top_level_keys,
                query_byte_size,
                extra=log_extra,
            )
            input_dict = json_format.MessageToDict(query)
            timer.mark("struct_to_dict")
            logger.debug(
                "[input-debug] input_dict (truncated 4KB): %s",
                repr(input_dict)[:4096],
                extra=log_extra,
            )

            input_data = self._servicer.module_class.create_input_model(input_dict)
            timer.mark("pydantic_input")

            module, job_id, callback = await self._servicer.job_manager.preload_instance(
                setup_data,
                mission_id=mission_id,
                setup_id=setup_version.setup_id,
                setup_version_id=setup_version.id,
                request_metadata={"x-task-id": task_id},
                job_id=task_id,
                tool_cache=tool_cache,
                callback=_on_output,
            )
            timer.mark("preload_join")

            await self._servicer.job_manager.run_preloaded(
                module=module,
                job_id=job_id,
                mission_id=mission_id,
                input_data=input_data,
                setup_data=setup_data,
                callback=callback,
            )
            timer.mark("create_job")
            timer.log("ModuleRunner", task_id)

        except ValidationError as exc:
            input_format_cls = (
                self._servicer.module_class._extended_input_format  # noqa: SLF001
                or self._servicer.module_class.input_format
            )
            model_name = input_format_cls.__name__ if input_format_cls is not None else "<unknown>"
            dict_repr = repr(input_dict)[:4096] if "input_dict" in locals() else "<unbuilt>"
            try:
                errors_json = json.dumps(exc.errors(include_url=False), default=str)
            except (TypeError, ValueError):
                errors_json = repr(exc.errors())
            missing_paths = [".".join(str(p) for p in e["loc"]) for e in exc.errors() if e["type"] == "missing"]
            logger.error(
                "[input-debug] ValidationError on input model %s\n"
                "  module_class=%s top_keys=%s wire_bytes=%d missing=%s\n"
                "  errors=%s\n"
                "  input_dict=%s",
                model_name,
                self._servicer.module_class.__name__,
                top_level_keys,
                query_byte_size,
                missing_paths,
                errors_json,
                dict_repr,
                extra=log_extra,
            )
            missing_summary = f" missing_fields={missing_paths}" if missing_paths else ""
            await on_fatal(
                StreamErrorCode.INPUT_VALIDATION_ERROR.value,
                f"input validation failed for {model_name}: top_keys={top_level_keys}{missing_summary}",
            )
        except BackpressureTimeoutError as exc:
            logger.exception("ModuleRunner: backpressure timeout", extra=log_extra)
            await on_fatal(StreamErrorCode.BACKPRESSURE_TIMEOUT.value, str(exc))
        except Exception as exc:
            logger.exception("ModuleRunner: module job failed", extra=log_extra)
            await on_fatal(
                StreamErrorCode.MODULE_RUNTIME_ERROR.value,
                f"module execution failed: {type(exc).__name__}: {exc}",
            )
        finally:
            profiler.stop()
