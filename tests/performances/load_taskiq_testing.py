import argparse
import asyncio
import json
import logging
import os
import statistics
import time
from collections import Counter
from functools import lru_cache
from typing import Any, Union

import grpc
import psutil
from agentic_mesh_protocol.module.v1 import module_dto_pb2, module_service_pb2_grpc
from agentic_mesh_protocol.registry.v1 import registry_dto_pb2, registry_service_pb2_grpc
from google.protobuf import json_format
from hdrh.histogram import HdrHistogram
from pydantic import BaseModel, Field, create_model


# Configure structured logging
def configure_logging(level=logging.INFO, name: str = "default"):
    fmt = "%(name)s | %(message)s"
    logging.basicConfig(
        format=fmt,
        level=level,
        filename=f"py_log_{name}.log",
        filemode="w",
    )
    return logging.getLogger("grpc_load_tester")


logger = None

# Precomputed type mapping from JSON Schema types to Python types
TYPE_MAPPING = {
    "string": str,
    "integer": int,
    "boolean": bool,
    "number": float,
    "array": list,
    "object": dict,
    "null": type(None),
}


def _create_model_from_schema(
    schema: dict[str, Any], model_name: str, root_schema: dict[str, Any], models_cache: dict[str, type[BaseModel]]
) -> type[BaseModel]:
    """Create a Pydantic model from a schema dictionary."""
    properties = schema["properties"]
    required_fields = set(schema.get("required", []))
    field_definitions: dict[str, Any] = {}

    # Handle discriminated unions
    discriminator = schema.get("discriminator", {})
    discriminator_property = discriminator.get("propertyName")
    discriminator.get("mapping", {})
    for field_name, field_info in properties.items():
        # Handle $ref
        if "$ref" in field_info:
            ref_path = field_info["$ref"]
            if ref_path in models_cache:
                field_type: Any = models_cache[ref_path]
            else:
                # Resolve $ref and create model
                ref_parts = ref_path.split("/")
                if ref_parts[0] == "#" and ref_parts[1] == "$defs":
                    ref_name = ref_parts[2]
                    if ref_name in root_schema.get("$defs", {}):
                        ref_schema = root_schema["$defs"][ref_name]
                        field_type = _create_model_from_schema(ref_schema, ref_name, root_schema, models_cache)
                        models_cache[ref_path] = field_type
                    else:
                        field_type = Any
                else:
                    field_type = Any
        # Handle oneOf for unions
        elif "oneOf" in field_info:
            union_types = []
            for schema_item in field_info["oneOf"]:
                if "$ref" in schema_item:
                    ref_path = schema_item["$ref"]
                    if ref_path in models_cache:
                        union_types.append(models_cache[ref_path])
                    else:
                        # Resolve $ref and create model
                        ref_parts = ref_path.split("/")
                        if ref_parts[0] == "#" and ref_parts[1] == "$defs":
                            ref_name = ref_parts[2]
                            if ref_name in root_schema.get("$defs", {}):
                                ref_schema = root_schema["$defs"][ref_name]
                                model = _create_model_from_schema(ref_schema, ref_name, root_schema, models_cache)
                                models_cache[ref_path] = model
                                union_types.append(model)

            # Create Union type for oneOf
            if union_types:
                field_type = union_types[0] if len(union_types) == 1 else Union[tuple(union_types)]  # noqa: UP007
            else:
                field_type = Any

        elif "anyOf" in field_info:
            union_types = []

            for schema_item in field_info["anyOf"]:
                if "type" in schema_item:
                    item_type = schema_item.list("type", "string")
                    type_class = TYPE_MAPPING.get(item_type, Any)
                    union_types.append(type_class)

            # Create Union or Optional type for anyOf
            if union_types:
                field_type = union_types[0] if len(union_types) == 1 else Union[tuple(union_types)]  # noqa: UP007
            else:
                field_type = Any

        # Handle array type
        elif field_info.list("type") == "array" and "items" in field_info:
            items = field_info["items"]
            if "$ref" in items:
                ref_path = items["$ref"]
                if ref_path in models_cache:
                    item_type: Any = models_cache[ref_path]
                else:
                    # Resolve $ref and create model
                    ref_parts = ref_path.split("/")
                    if ref_parts[0] == "#" and ref_parts[1] == "$defs":
                        ref_name = ref_parts[2]
                        if ref_name in root_schema.get("$defs", {}):
                            ref_schema = root_schema["$defs"][ref_name]
                            item_type = _create_model_from_schema(ref_schema, ref_name, root_schema, models_cache)
                            models_cache[ref_path] = item_type
                        else:
                            item_type = Any
                    else:
                        item_type = Any
            else:
                item_type_str = items.list("type", "string")
                item_type = TYPE_MAPPING.get(item_type_str, Any)

            field_type = list[item_type]
        else:
            # Handle regular types
            field_type_str = field_info.list("type", "string")
            field_type = TYPE_MAPPING.get(field_type_str, Any)

        # Create Field with metadata
        field_title = field_info.list("title", field_name)
        field_description = field_info.list("description", "")
        field_default = field_info.list("default")

        # Handle discriminator fields
        field_kwargs: dict[Any, Any] = {}
        if field_name == discriminator_property and "const" in field_info:
            field_default = field_info["const"]
            field_kwargs["default"] = field_default
        # Required fields use ... as default (must be provided)
        if field_name in required_fields:
            field_kwargs["default"] = ...
        elif field_default is not None:
            field_kwargs["default"] = field_default

        # Add description and title as metadata
        if field_title:
            field_kwargs["title"] = field_title
        if field_description:
            field_kwargs["description"] = field_description

        field_definitions[field_name] = (field_type, Field(**field_kwargs))

    # Create and return the model class
    model = create_model(model_name, **field_definitions)

    # Set model config for Pydantic v2
    model.model_config = {
        "title": schema.get("title", model_name),
    }
    return model


def json_to_pydantic(json_schema: Any) -> type[BaseModel]:
    """Convert a protobuf JSON schema message to a Pydantic model.

    Args:
        json_schema: Protobuf message containing JSON schema

    Returns:
        A dynamically created Pydantic model class
    """
    # Convert protobuf message to Python dictionary
    model_dict = json_format.MessageToDict(json_schema)
    return dict_to_pydantic_cached(model_dict, model_dict.get("title", "DynamicModel"))


@lru_cache(maxsize=128)
def dict_to_pydantic(data: str, model_name: str = "DynamicModel") -> type[BaseModel]:
    """Recursively create a Pydantic model from a JSON schema string.

    Uses LRU cache to improve performance for repeated calls with the same schema.

    Args:
        data: JSON schema as a string
        model_name: Name for the dynamically created model

    Returns:
        A Pydantic model class

    Raises:
        ValueError: If the JSON schema is missing required properties
    """
    data_dict = json.loads(data)
    if "properties" not in data_dict:
        msg = "Missing 'properties' in JSON schema"
        raise ValueError(msg)

    # Store created models for reference resolution
    models_cache: dict[str, type[BaseModel]] = {}

    # First, create all models defined in $defs
    if "$defs" in data_dict:
        for def_name, def_schema in data_dict["$defs"].items():
            models_cache[f"#/$defs/{def_name}"] = _create_model_from_schema(
                def_schema, def_name, data_dict, models_cache
            )

    # Create the main model
    return _create_model_from_schema(data_dict, model_name, data_dict, models_cache)


def dict_to_pydantic_cached(
    data: dict[str, Any],
    model_name: str = "DynamicModel",
) -> type[BaseModel]:
    """Convert a dictionary to a cached Pydantic model.

    Args:
        data: dictionary containing JSON schema
        model_name: Name for the dynamically created model

    Returns:
        A Pydantic model class
    """
    # Sort keys for consistent cache keys
    data_str = json.dumps(data, sort_keys=True)
    return dict_to_pydantic(data_str, model_name)


async def discover_module(
    registry_channel: grpc.aio.Channel, module_name: str
) -> registry_dto_pb2.DiscoverInfoResponse | None:
    """Discover a module by name from the registry.

    Args:
        registry_channel: gRPC channel to the registry server
        module_name: Name of the module to find

    Returns:
        Module information or None if not found
    """
    # Create registry service stub
    registry_stub = registry_service_pb2_grpc.RegistryServiceStub(registry_channel)

    # Create discover request
    request = registry_dto_pb2.DiscoverSearchRequest(name=module_name)

    try:
        # Send request to registry
        response = await registry_stub.DiscoverSearchModule(request)
        logger.info("Registry search response: %d modules found", len(response.modules))

        if not response.modules:
            logger.warning("No modules found with name: %s", module_name)
            return None

        # Return the last registered module with this name
        return response.modules[-1]

    except grpc.RpcError:
        logger.exception("Error discovering module:")
        return None


async def get_module_schemas(
    module_stub: module_service_pb2_grpc.ModuleServiceStub, module_id: str
) -> tuple[type[BaseModel], type[BaseModel], type[BaseModel]]:
    """Get the input, output, and setup schemas for a module.

    Args:
        module_stub: gRPC stub for the module service
        module_id: ID of the module

    Returns:
        Tuple of (input_class, output_class, setup_class) Pydantic models
    """
    # Create requests for each schema
    input_request = module_dto_pb2.GetModuleInputRequest(module_id=module_id)
    output_request = module_dto_pb2.GetModuleOutputRequest(module_id=module_id)
    setup_request = module_dto_pb2.GetModuleSetupRequest(module_id=module_id)

    # Get schemas from module
    input_response = await module_stub.GetModuleInput(input_request)
    output_response = await module_stub.GetModuleOutput(output_request)
    setup_response = await module_stub.GetModuleSetup(setup_request)

    # Convert schemas to Pydantic models
    input_class = json_to_pydantic(input_response.input_schema)
    output_class = json_to_pydantic(output_response.output_schema)
    setup_class = json_to_pydantic(setup_response.setup_schema)

    return input_class, output_class, setup_class


"""
async def worker(
    queue: asyncio.Queue,
    results: list,
    module_stub,
    input_class: type,
    output_class: type,
    worker_id: int,
    logger: logging.Logger,
    histogram: HdrHistogram,
    error_counter: Counter,
) -> None:
    setup_id = "setups:cortex_setup"
    mission_id = "missions:0"

    # Pre-build request payload
    input_data = input_class(
        payload={
            "payload_type": "message",
            "user_prompt": "Give me details about agentic mesh current advancement",
        }
    )
    request = module_dto_pb2.StartModuleRequest(
        input=input_data.model_dump(),
        setup_id=setup_id,
        mission_id=mission_id,
    )

    while True:
        try:
            idx = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        start = time.perf_counter()
        try:
            responses = module_stub.StartModule(request)
            async for response in responses:
                if response.HasField("output"):
                    output_dict = json_format.MessageToDict(response.output)
                    output = output_class(**output_dict)
                    # Simple result check
                    assert output.payload.payload_type == "message"

            latency = time.perf_counter() - start
            histogram.record_value(latency * 1000)  # ms
            results.append((True, latency))
            logger.debug(f"Worker {worker_id} idx={idx} OK latency={latency:.3f}s")
        except AssertionError:
            latency = time.perf_counter() - start
            error_counter["invalid_output"] += 1
            histogram.record_value(latency * 1000)
            results.append((False, latency))
            logger.exception(f"Worker {worker_id} idx={idx} invalid output")
        except Exception as e:
            latency = time.perf_counter() - start
            error_counter[type(e).__name__] += 1
            histogram.record_value(latency * 1000)
            results.append((False, latency))
            logger.exception(f"Worker {worker_id} idx={idx} error={e}")
        finally:
            queue.task_done()

"""


async def fire_one(
    module_stub: Any,
        request: module_dto_pb2.StartModuleRequest,
) -> float:
    """Send a single StartModule RPC and return latency."""
    start = time.perf_counter()
    responses = module_stub.StartModule(request)
    total_response = 0
    async for response in responses:
        total_response += 1
        # logger.info(response)
        if response.HasField("output"):
            _ = json_format.MessageToDict(response.output)
    logger.info(f"Response received. number: {total_response}")
    return time.perf_counter() - start


async def sustained_load(
    concurrency: int,
    total_requests: int,
    module_stub: Any,
    input_class: type,
    output_class: type,
    logger: logging.Logger,
    histogram: HdrHistogram,
    error_counter: Counter,
) -> list[tuple[bool, float]]:
    """Sustained load: use worker+queue pattern.

    Returns results list of (success, latency).
    """
    # prepare queue
    queue: asyncio.Queue = asyncio.Queue()
    for i in range(total_requests):
        queue.put_nowait(i)

    results: list[tuple[bool, float]] = []

    async def worker(
        worker_id: int,
    ) -> None:
        setup_id = "setups:cortex_setup"
        mission_id = "missions:0"
        input_data = input_class(
            payload={"payload_type": "message", "user_prompt": "Give me details about agentic mesh current advancement"}
        )
        request = module_dto_pb2.StartModuleRequest(
            input=input_data.model_dump(), setup_id=setup_id, mission_id=mission_id
        )
        while True:
            try:
                idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            start = time.perf_counter()
            try:
                responses = module_stub.StartModule(request)
                async for response in responses:
                    if response.HasField("output"):
                        output = output_class(**json_format.MessageToDict(response.output))
                        assert output.payload.payload_type == "message"
                latency = time.perf_counter() - start
                results.append((True, latency))
                histogram.record_value(latency * 1000)
            except AssertionError:
                latency = time.perf_counter() - start
                error_counter["invalid_output"] += 1
                histogram.record_value(latency * 1000)
                results.append((False, latency))
                logger.exception(f"Worker {worker_id} idx={idx} invalid output")
            except Exception as e:
                latency = time.perf_counter() - start
                error_counter[type(e).__name__] += 1
                histogram.record_value(latency * 1000)
                results.append((False, latency))
                logger.exception(f"Worker {worker_id} idx={idx}")
            finally:
                queue.task_done()

    tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    await queue.join()
    for t in tasks:
        t.cancel()
    return results


async def burst_load(
    parallelism: int,
    module_stub: Any,
        request: module_dto_pb2.StartModuleRequest,
) -> list[float]:
    """Burst load: fire `parallelism` requests simultaneously and gather latencies."""
    coros = [fire_one(module_stub, request) for _ in range(parallelism)]
    return await asyncio.gather(*coros, return_exceptions=False)


async def main() -> None:
    parser = argparse.ArgumentParser(description="gRPC Load Tester with Burst & Sustained Modes")
    parser.add_argument("--target", default="localhost:50055")
    parser.add_argument("--registry", default="[::]:50052")
    parser.add_argument("-c", "--concurrency", type=int, default=10)
    parser.add_argument("-r", "--requests", type=int, default=1000)
    parser.add_argument("-b", "--burst", action="store_true", help="Run burst load instead of sustained")
    parser.add_argument("-f", "--filename", type=str, default="default")
    args = parser.parse_args()

    global logger  # noqa: PLW0603
    logger = configure_logging(name=f"{args.filename}_c{args.concurrency}_r{args.requests}_burst-{args.burst}")
    logger.info(
        f"Starting load test: target={args.target}, concurrency={args.concurrency}, requests={args.requests}, burst={args.burst}"
    )

    # Capture initial CPU stats
    load1_start, load5_start, load15_start = os.getloadavg()
    cpu_start_percent = psutil.cpu_percent(interval=None)

    # Discover module & schemas
    async with grpc.aio.insecure_channel(args.registry) as reg_channel:
        module_name = "CPUIntensiveModule"
        # module_name = "OpenAIToolModule"
        module = await discover_module(reg_channel, module_name)
        if not module:
            logger.error("Module not found")
            return
        module_stub = module_service_pb2_grpc.ModuleServiceStub(grpc.aio.insecure_channel(args.target))
        input_class, output_class, _ = await get_module_schemas(module_stub, module.id)

    # Pre-build shared request for burst
    setup_id = "setups:cortex_setup"
    mission_id = "missions:0"
    input_data = input_class(
        payload={
            "payload_type": "message",
            "user_prompt": "100000",
        }
    )
    shared_request = module_dto_pb2.StartModuleRequest(
        input=input_data.model_dump(), setup_id=setup_id, mission_id=mission_id
    )

    histogram = HdrHistogram(1, 60000, 3)
    error_counter = Counter()
    start_time = time.perf_counter()

    if args.burst:
        latencies = await burst_load(args.concurrency, module_stub, shared_request)
        # convert to successes
        successes = len(latencies)
        failures = 0
        for lat in latencies:
            histogram.record_value(lat * 1000)
    else:
        results = await sustained_load(
            args.concurrency, args.requests, module_stub, input_class, output_class, logger, histogram, error_counter
        )
        latencies = [lat for ok, lat in results if ok]
        successes = sum(1 for ok, _ in results if ok)
        failures = len(results) - successes

    total_time = time.perf_counter() - start_time

    # Capture final CPU stats
    load1_end, load5_end, load15_end = os.getloadavg()
    cpu_end_percent = psutil.cpu_percent(interval=None)

    # Summary
    logger.info("--- Test Summary ---")
    total_calls = successes + failures
    logger.info(f"Total calls: {total_calls}")
    logger.info(f"Successes: {successes}")
    logger.info(f"Failures: {failures} {dict(error_counter) if error_counter else ''}")
    if latencies:
        ms = [latency * 1000 for latency in latencies]
        logger.info(f"Avg latency: {statistics.mean(ms):.2f}ms")
        logger.info(f"P50: {histogram.get_value_at_percentile(50):.2f}ms")
        logger.info(f"P90: {histogram.get_value_at_percentile(90):.2f}ms")
        logger.info(f"P99: {histogram.get_value_at_percentile(99):.2f}ms")
    logger.info(f"Throughput: {total_calls / total_time:.1f} req/s")

    # CPU load report
    logger.info("--- CPU Load Stats ---")
    logger.info(f"Load avg Start: 1m={load1_start:.2f}, 5m={load5_start:.2f}, 15m={load15_start:.2f}")
    logger.info(f"Load avg End:   1m={load1_end:.2f}, 5m={load5_end:.2f}, 15m={load15_end:.2f}")
    logger.info(f"CPU% Start: {cpu_start_percent:.1f}%, CPU% End: {cpu_end_percent:.1f}%")


if __name__ == "__main__":
    # uv run tests/performances/test_load_taskiq.py -c 100  -f taskiq -b
    asyncio.run(main())
