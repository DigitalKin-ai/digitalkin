# DigitalKin gRPC Module System

This repository contains a SDK for creating, discovering, and interacting with DigitalKin's modules using gRPC. The DigitalKin module system allows you to build extensible applications with standardized communication between components.

## Overview

The DigitalKin module system consists of three main components:

1. **Registry Server**: A central service discovery mechanism that keeps track of available modules
2. **Module Servers**: Individual services that implement specific functionality
3. **Clients**: Applications that discover and use modules through the registry

This architecture enables a plug-and-play approach to building distributed systems, where modules can be added, removed, or upgraded without changing client code.

## Getting Started

### Prerequisites

- Python 3.10+
- DigitalKin SDK and proto files
- gRPC tools

Install the required packages:

```bash
task setup-dev
```

### Running the Example

1. Start the Registry Server:

```bash
uv run examples/start_grpc_server_registry.py
```

2. Start the Module Server:

```bash
uv run examples/module_server_example.py
```

3. Run the Client:

```bash
uv run examples/module_client_example.py
```

## Architecture

### Registry Server

The Registry Server acts as a central directory of available modules. Modules register themselves with the registry, providing metadata that clients can use to discover them.

- **Port**: Default in example 50052
- **Key Functions**: Module registration, module discovery, heartbeat monitoring

### Module Server

A Module Server hosts one or more modules that provide specific functionality. Each module defines its input and output schemas, as well as business logic.

- **Port**: Default in example 50051
- **Key Components**: Module definition, schema specification, business logic implementation

### Client

Clients discover modules through the registry and interact with them directly using the standardized gRPC interfaces.

- **Key Operations**: Module discovery, schema retrieval, module execution

## Module Structure

A typical module implements three key methods:

1. **initialize()**: Set up the module's capabilities and resources
2. **run()**: Execute the module's core functionality
3. **cleanup()**: Release resources when the module is stopped

## API Reference

### Registry API

#### DiscoverSearchModule

Finds modules based on search criteria.

```python
request = discover_pb2.DiscoverSearchRequest(name="Module_Name")
response = await registry_stub.DiscoverSearchModule(request)
```

Returns a list of matching modules with their metadata and connection information.

### Module API

#### GetModuleInput

Retrieves the input schema for a module.

```python
request = information_pb2.GetModuleInputRequest(module_id=module_id)
response = await module_stub.GetModuleInput(request)
```

#### GetModuleOutput

Retrieves the output schema for a module.

```python
request = information_pb2.GetModuleOutputRequest(module_id=module_id)
response = await module_stub.GetModuleOutput(request)
```

#### GetModuleSetup

Retrieves the setup configuration schema for a module.

```python
request = information_pb2.GetModuleSetupRequest(module_id=module_id)
response = await module_stub.GetModuleSetup(request)
```

#### StartModule

Starts module execution with the provided input data.

```python
request = lifecycle_pb2.StartModuleRequest(
    input=input_data.model_dump(),
    setup_id=setup_id
)
responses = module_stub.StartModule(request)
```

This is a streaming RPC that returns multiple responses as the module processes data.

#### StopModule

Stops module execution with the provided job_id.

```python
request = lifecycle_pb2.StopModuleRequest(job_id=job_id)
response = module_stub.StopModule(request)
```

## Creating a Module

### Step 1: Define Schema Models

Use Pydantic models to define the expected input, output, and setup configurations:

```python
class TextTransformInput(BaseModel):
    """Input model defining what data the module expects."""

    text: str
    transform_count: int = 1


class TextTransformOutput(BaseModel):
    """Output model defining what data the module produces."""

    transformed_text: str
    iteration: int


class TextTransformSetup(BaseModel):
    """Setup model defining module configuration parameters."""

    shift_amount: int = 1
    uppercase: bool = False
```

### Step 2: Implement the Module Class

Create a class that inherits from `BaseModule` and implements the required methods:

```python
class TextTransformModule(BaseModule[TextTransformInput, TextTransformOutput, TextTransformSetup]):
    """A text transformation module that demonstrates streaming capabilities.

    This module takes text input and performs multiple transformations on it,
    sending back each transformation as a separate output message.
    """

    # Define the schema formats for the module
    input_format = TextTransformInput
    output_format = TextTransformOutput
    setup_format = TextTransformSetup

    # Define module metadata for discovery
    metadata: ClassVar[dict[str, Any]] = {
        "name": "Text_Transform_Module",
        "description": "Transforms input text using Caesar cipher with streaming output",
        "version": "1.0.0",
        "tags": ["text", "transformation", "encryption", "streaming"],
    }

    async def initialize(self) -> None:
        ...

    async def run(
        self,
        input_data: dict[str, Any],
        setup_data: dict[str, Any],
        callback: Callable,
    ) -> None:
        ...

    async def cleanup(self) -> None:
        ...

```

### Step 3: Start the Module Server

From the module server, use the Module class just created, as long as the Pydantic BaseModel norms are followed, the module will communicate via the DigitalKin SDK api.

## Creating a Client

### Step 1: Discover a Module

Connect to the registry and find the desired module, see `start_grpc_client::discover_module`

### Step 2: Retrieve Module Schemas

Get the input, output, and setup schemas, see `start_grpc_client::get_module_schemas`

### Step 3: Start the Module

Create input data and start module execution:

```python
# Get module schemas
input_class, output_class, setup_class = await get_module_schemas(module_stub, module.module_id)

# Create input data using the schema
input_data = input_class(
    text="Hello DigitalKin",
    transform_count=5,
)
```

## Streaming Responses

Modules can stream multiple responses back to clients using the callback mechanism:

```python
responses = module_stub.StartModule(request)
async for response in responses:
    print(response)
```

The client receives these as separate responses in the response stream.

## Best Practices

1. **Schema Design**:
   - Use clear, descriptive field names
   - Provide default values for optional fields
   - Add helpful docstrings to models

2. **Error Handling**:
   - Implement proper try/except blocks around gRPC calls
   - Log detailed error information
   - Clean up resources in finally blocks

3. **Module Implementation**:
   - Keep modules focused on specific functionality
   - Use clear logging throughout module execution
   - Implement proper cleanup for resources

4. **Security**:
   - Use secure connections in production (SecurityMode.TLS)
   - Implement proper authentication mechanisms
   - Validate all input data

## Schema Conversion

The system includes utilities to convert between protocol buffer schema definitions and Pydantic models:

```python
def json_to_pydantic(json_schema: Message) -> type[BaseModel]:
    """Convert a protobuf JSON schema message to a Pydantic model."""
    model_dict = json_format.MessageToDict(json_schema)
    return dict_to_pydantic_cached(model_dict, model_dict.get("title", "DynamicModel"))
```

This allows dynamic creation of appropriate models for interacting with modules.

## Conclusion

The DigitalKin gRPC module system provides a powerful framework for building distributed, modular applications. By following the conventions and patterns described in this README, you can create robust, maintainable, and extensible systems.

For more information, refer to the examples provided and the API documentation.
