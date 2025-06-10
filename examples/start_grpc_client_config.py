"""DigitalKin Module Client Example.

This example demonstrates how to:
1. Connect to a registry server to discover modules
2. Query module schemas (input, output, setup)
3. Start a module with custom input
4. Receive streaming results from the module

To run: uv run module_client_example.py

Requirements:
- DigitalKin SDK and proto files installed
- Registry server running (default: localhost:50052)
- Module server running (default: localhost:50051)
"""

import asyncio
from base64 import b64encode
import json
import logging
from functools import lru_cache
from typing import Any

import grpc

# Import gRPC protobuf generated classes
from digitalkin_proto.digitalkin.module.v2 import information_pb2, lifecycle_pb2, module_service_pb2_grpc
from digitalkin_proto.digitalkin.module_registry.v2 import discover_pb2, module_registry_service_pb2_grpc
from digitalkin_proto.digitalkin.setup.v2 import setup_pb2
from google.protobuf import json_format, struct_pb2
from google.protobuf.message import Message
from pydantic import BaseModel, create_model

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Precomputed type mapping from JSON Schema types to Python types
TYPE_MAPPING = {
    "string": str,
    "integer": int,
    "boolean": bool,
    "number": float,
    "array": list,
    "object": dict,
}


def json_to_pydantic(json_schema: Message) -> type[BaseModel]:
    """Convert a protobuf JSON schema message to a Pydantic model.

    Args:
        json_schema: Protobuf message containing JSON schema

    Returns:
        A dynamically created Pydantic model class
    """
    # Convert protobuf message to Python dictionary
    model_dict = json_format.MessageToDict(json_schema)

    # Use cached version to avoid recreating the same model multiple times
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

    properties = data_dict["properties"]
    required_fields = set(data_dict.get("required", []))
    field_definitions = {}

    # Create field definitions for the Pydantic model
    for field_name, field_info in properties.items():
        field_type_str = field_info.get("type", "string")
        python_type = TYPE_MAPPING.get(field_type_str, Any)

        # Mark required fields with ellipsis (...) as required
        field_definitions[field_name] = (python_type, ... if field_name in required_fields else None)

    # Create and return the model class
    return create_model(model_name, **field_definitions)  # type: ignore


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
) -> discover_pb2.DiscoverInfoResponse | None:
    """Discover a module by name from the registry.

    Args:
        registry_channel: gRPC channel to the registry server
        module_name: Name of the module to find

    Returns:
        Module information or None if not found
    """
    # Create registry service stub
    registry_stub = module_registry_service_pb2_grpc.ModuleRegistryServiceStub(registry_channel)

    # Create discover request
    request = discover_pb2.DiscoverSearchRequest(name=module_name)

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
    input_request = information_pb2.GetModuleInputRequest(module_id=module_id)
    output_request = information_pb2.GetModuleOutputRequest(module_id=module_id)
    setup_request = information_pb2.GetModuleSetupRequest(module_id=module_id)

    # Get schemas from module
    input_response = await module_stub.GetModuleInput(input_request)
    output_response = await module_stub.GetModuleOutput(output_request)
    setup_response = await module_stub.GetModuleSetup(setup_request)

    # Convert schemas to Pydantic models
    input_class = json_to_pydantic(input_response.input_schema)
    output_class = json_to_pydantic(output_response.output_schema)
    setup_class = json_to_pydantic(setup_response.setup_schema)

    return input_class, output_class, setup_class


async def run_client_llm() -> None:
    """Run the client application to interact with the module."""
    # Connect to registry server
    async with grpc.aio.insecure_channel("localhost:50052") as registry_channel:
        logger.info("Connecting to registry server at localhost:50052")

        # Find the module
        module = await discover_module(registry_channel, "OpenAIToolModule")
        if not module:
            logger.error("Module not found. Make sure the module server is running.")
            return

        logger.info("Found module: %s (ID: %s)", module.metadata.name, module.module_id)

        # Connect to module server
        async with grpc.aio.insecure_channel("localhost:50055") as module_channel:
            logger.info("Connecting to module server at localhost:50055")

            # Create module service stub
            module_stub = module_service_pb2_grpc.ModuleServiceStub(module_channel)

            # Get module schemas
            input_class, output_class, setup_class = await get_module_schemas(module_stub, module.module_id)

            logger.info(
                "Retrieved module schemas: %s, %s and %s",
                input_class.__name__,
                output_class.__name__,
                setup_class.__name__,
            )

            mission_id = "missions:0"

            setup_version_data = setup_class(
                model_name="yes model",
                developer_prompt="prompt yes",
                temperature=1.0,
                max_tokens=1000,
            )

            config_setup_request = information_pb2.GetConfigSetupModuleRequest(module_id=module.module_id)
            config_setup_response = await module_stub.GetConfigSetupModule(config_setup_request)
            config_setup_class = json_to_pydantic(config_setup_response.config_setup_schema)

            content = config_setup_class(
                rag_files=[
                    b64encode(b"1111").decode("utf-8"),
                    b64encode(b"2222").decode("utf-8"),
                ]
            ).model_dump()

            request = lifecycle_pb2.ConfigSetupModuleRequest(
                setup_version=setup_pb2.SetupVersion(
                    id="setup_versions:0",
                    setup_id="setups:0",
                    version="0.1.0",
                    content=json_format.ParseDict(
                        setup_version_data.model_dump(),
                        message=struct_pb2.Struct(),
                        ignore_unknown_fields=True,
                    ),
                ),
                content=content,
                mission_id=mission_id,
            )

            logger.info(
                "Starting config setup module with setup_version: %s | content: %s",
                setup_version_data.model_dump(),
                content,
            )

            try:
                response = await module_stub.ConfigSetupModule(request)
                logger.info("Module response %s", response)
            except grpc.RpcError:
                logger.exception("Error running module:")


if __name__ == "__main__":
    fn = run_client_llm
    asyncio.run(fn())
