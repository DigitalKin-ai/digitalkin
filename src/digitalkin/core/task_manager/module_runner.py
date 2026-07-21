"""Module runner invoked by the dial-back orchestrator."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format, struct_pb2
from pydantic import ValidationError
from redis.exceptions import RedisError

from digitalkin.core.exceptions import BackpressureTimeoutError
from digitalkin.core.profiling.step_timer import StepTimer
from digitalkin.core.profiling.task_profiler import TaskProfiler
from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.grpc_servers.interceptors.request_ids import RequestContext
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.stream_error_codes import StreamErrorCode
from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.models.settings.profiling import ProfilerMode, get_profiling_settings

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.grpc_servers.module_servicer import ModuleServicer


class ModuleRunner:
    """Run one task end-to-end: setup → module instance → output drain."""

    _redis_client: RedisClient
    _servicer: ModuleServicer

    def __init__(self, redis_client: RedisClient, servicer: ModuleServicer) -> None:
        """Initialize the runner.

        Args:
            redis_client: Redis used to write module outputs.
            servicer: ModuleServicer for setup and job management.
        """
        self._redis_client = redis_client
        self._servicer = servicer

    async def run(  # noqa: C901, PLR0914, PLR0915
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
            query: First input Struct received from the consumer.
            task_id: Task identifier (stream key ``task:{task_id}:stream``).
            setup_id: Setup identifier.
            mission_id: Mission identifier (logging context).
            on_fatal: Async callback ``(code, message)`` invoked on
                unhandled exception; the caller writes ``stream.error`` + EOS.
        """
        log_extra = {"task_id": task_id, "setup_id": setup_id, "mission_id": mission_id}
        stream_key = f"task:{task_id}:stream"
        timer = StepTimer()
        # Bind IDs so every downstream gRPC call (registry/storage/cost/...) and
        # log record made during this task carries them via the interceptors/filter.
        ctx_token = RequestContext.bind(task_id=task_id, setup_id=setup_id, mission_id=mission_id)

        # Construct profiler outside try so finally can always stop it.
        profiling = get_profiling_settings()
        profiler_mode = (
            ProfilerMode(profiling.profiler)
            if profiling.profiler in {p.value for p in ProfilerMode}
            else ProfilerMode.NONE
        )
        profiler = TaskProfiler(task_id=task_id, mode=profiler_mode, output_dir=profiling.profile_output_dir)

        try:  # noqa: PLW0717
            timer.mark("entry")
            profiler.start()

            setup_version = await self._servicer.resolve_setup(setup_id, mission_id)
            timer.mark("setup_resolve")

            setup_data = await self._servicer.module_class.create_setup_model(setup_version.content)
            timer.mark("setup_model")

            tool_cache = self._servicer.get_tool_cache(setup_version.setup_id)
            if tool_cache is None:
                registry = self._servicer._get_registry()  # noqa: SLF001
                communication = self._servicer._get_communication()  # noqa: SLF001
                if registry is not None and communication is not None:
                    tool_cache = await self._servicer.get_or_build_tool_cache(
                        setup_version.setup_id,
                        lambda: setup_data.build_tool_cache(registry, communication),
                    )
            timer.mark("tool_cache_lookup")

            runner_start_ns = time.perf_counter_ns()
            first_logged = False
            seq = 0
            stream_settings = get_gateway_settings().stream
            stream_maxlen = stream_settings.redis_stream_maxlen

            async def _on_output(output_data: Any) -> None:
                nonlocal first_logged, seq
                data = output_data.model_dump(mode="json")
                if data.get("root", {}).get("protocol") == "stream.end":
                    t_eos_write_start = time.perf_counter_ns()
                    await self._redis_client.xadd(stream_key, {"eos": b"true"})
                    await self._redis_client.expire(stream_key, stream_settings.redis_stream_ttl)
                    t_eos_write_end = time.perf_counter_ns()
                    logger.info(
                        "[close-debug] producer_eos_write: xadd_expire=%.2fms t_done_ns=%d task_id=%s",
                        (t_eos_write_end - t_eos_write_start) / 1e6,
                        t_eos_write_end,
                        task_id,
                    )
                    return

                seq += 1
                s = struct_pb2.Struct()
                s.update(data)
                # M4: carry seq (enables reader gap-detection) + bound the stream (maxlen).
                await self._redis_client.xadd(
                    stream_key, {"pb": s.SerializeToString(), "seq": str(seq)}, maxlen=stream_maxlen
                )
                # Arm a TTL on first XADD; final EXPIRE on stream.end shortens it.
                if not first_logged:
                    elapsed_ms = (time.perf_counter_ns() - runner_start_ns) / 1e6
                    logger.debug(
                        "[perf] producer_first_byte_to_redis: %.1fms task_id=%s",
                        elapsed_ms,
                        task_id,
                        extra=log_extra,
                    )
                    await self._redis_client.expire(stream_key, stream_settings.redis_stream_initial_ttl)
                    first_logged = True

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

            await self._servicer.job_manager.run_instance(
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
        except RedisError as exc:
            await on_fatal(
                StreamErrorCode.REDIS_UNAVAILABLE.value,
                f"redis unavailable: {type(exc).__name__}: {exc}",
            )
        except PermissionDeniedError as exc:
            logger.warning("ModuleRunner: setup access denied: %s", exc, extra=log_extra)
            await on_fatal(StreamErrorCode.SETUP_ACCESS_DENIED.value, str(exc))
        except Exception as exc:
            logger.exception("ModuleRunner: module job failed", extra=log_extra)
            await on_fatal(
                StreamErrorCode.MODULE_RUNTIME_ERROR.value,
                f"module execution failed: {type(exc).__name__}: {exc}",
            )
        finally:
            profiler.stop()
            RequestContext.reset(ctx_token)
