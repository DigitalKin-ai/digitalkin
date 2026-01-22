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
import json
import logging
import sys
from functools import lru_cache
from typing import Any

import grpc
# Import gRPC protobuf generated classes
from agentic_mesh_protocol.module.v1 import module_dto_pb2, module_service_pb2_grpc
from agentic_mesh_protocol.registry.v1 import registry_dto_pb2, registry_service_pb2_grpc
from google.protobuf import json_format
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
    required_fields = set(data_dict.list("required", []))
    field_definitions = {}

    # Create field definitions for the Pydantic model
    for field_name, field_info in properties.items():
        field_type_str = field_info.list("type", "string")
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
) -> registry_dto_pb2.GetModuleResponse | None:
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
    request = registry_dto_pb2.SearchModulesRequest(name=module_name)

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
    input_request = module_dto_pb2.GetModuleInputRequest(id=module_id)
    output_request = module_dto_pb2.GetModuleOutputRequest(id=module_id)
    setup_request = module_dto_pb2.GetModuleSetupRequest(id=module_id)

    # Get schemas from module
    input_response = await module_stub.GetModuleInput(input_request)
    output_response = await module_stub.GetModuleOutput(output_request)
    setup_response = await module_stub.GetModuleSetup(setup_request)

    # Convert schemas to Pydantic models
    input_class = json_to_pydantic(input_response.input_schema)
    output_class = json_to_pydantic(output_response.output_schema)
    setup_class = json_to_pydantic(setup_response.setup_schema)

    return input_class, output_class, setup_class


async def run_client_text_transform() -> None:
    """Run the client application to interact with the module."""
    # Connect to registry server
    async with grpc.aio.insecure_channel("localhost:50052") as registry_channel:
        logger.info("Connecting to registry server at localhost:50052")

        # Find the module
        module = await discover_module(registry_channel, "Text_Transform_Module")
        if not module:
            logger.error("Module not found. Make sure the module server is running.")
            return

        logger.info("Found module: %s (ID: %s)", module.result.module_descriptor.name, module.result.module_descriptor.id)

        # Connect to module server
        async with grpc.aio.insecure_channel("localhost:50051") as module_channel:
            logger.info("Connecting to module server at localhost:50051")

            # Create module service stub
            module_stub = module_service_pb2_grpc.ModuleServiceStub(module_channel)

            # Get module schemas
            input_class, output_class, setup_class = await get_module_schemas(module_stub, module.result.module_descriptor.id)

            logger.info(
                "Retrieved module schemas: %s, %s and %s",
                input_class.__name__,
                output_class.__name__,
                setup_class.__name__,
            )

            # Create setup data (we'll use default setup_id for this example)
            # In a real application, you might create and store a setup configuration first
            setup_id = "setups:0"
            mission_id = "missions:0"

            # Create input data using the schema
            input_data = input_class(
                text="Hello DigitalKin",
                transform_count=5,
            )

            # Create start module request
            request = module_dto_pb2.StartModuleRequest(
                input=input_data.model_dump(), setup_id=setup_id, mission_id=mission_id
            )

            logger.info("Starting module with input: %s", input_data.model_dump())

            # Start the module and process streaming responses
            try:
                responses = module_stub.StartModule(request)
                async for response in responses:
                    # Process each output message
                    if response.HasField("output"):
                        # Convert output data
                        output_dict = json_format.MessageToDict(response.output)
                        output = output_class(**output_dict)

                        logger.info("Received transformation %s: '%s'", output.iteration, output.transformed_text)
                logger.info("Module execution completed successfully")

            except grpc.RpcError:
                logger.exception("Error running module:")


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

        logger.info("Found module: %s (ID: %s)", module.result.module_descriptor.name, module.result.module_descriptor.id)

        # Connect to module server
        async with grpc.aio.insecure_channel("localhost:50055") as module_channel:
            logger.info("Connecting to module server at localhost:50055")

            # Create module service stub
            module_stub = module_service_pb2_grpc.ModuleServiceStub(module_channel)

            # Get module schemas
            input_class, output_class, setup_class = await get_module_schemas(module_stub, module.result.module_descriptor.id)

            logger.info(
                "Retrieved module schemas: %s, %s and %s",
                input_class.__name__,
                output_class.__name__,
                setup_class.__name__,
            )

            # Create setup data (we'll use default setup_id for this example)
            # In a real application, you might create and store a setup configuration first
            setup_id = "setups:0"
            mission_id = "missions:0"

            # Create input data using the schema
            input_data = input_class(prompt="Give me details about agentic mesh current advancement")

            # Create start module request
            module_dto_pb2.StartModuleRequest(input=input_data.model_dump(), setup_id=setup_id, mission_id=mission_id)

            logger.info("Starting module with input: %s", input_data.model_dump())

            # Start the module and process streaming responses
            try:
                async for response in responses:
                    # Process each output message
                    if response.HasField("output"):
                        # Convert output data
                        output_dict = json_format.MessageToDict(response.output)
                        output = output_class(**output_dict)

                        logger.info("Received answer %s", output.response)

                logger.info("Module execution completed successfully")

            except grpc.RpcError:
                logger.exception("Error running module:")


if __name__ == "__main__":
    fn = run_client_text_transform
    if len(sys.argv) > 1 and "llm" in sys.argv:
        fn = run_client_llm
    asyncio.run(fn())
