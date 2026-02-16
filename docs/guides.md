# Guides

Practical guides for building modules with the DigitalKin SDK.

## Creating Custom Modules

Learn how to subclass `BaseModule`, `ToolModule`, or `ArchetypeModule` to build your own modules. Covers defining the four generic type parameters (input, output, setup, secret), setting up module metadata, and configuring service dependencies.

See: [`examples/modules/text_transform_module.py`](https://github.com/DigitalKin-ai/digitalkin/tree/main/examples/modules/text_transform_module.py)

## Working with Trigger Handlers

Trigger handlers are the primary mechanism for processing inputs. Each handler declares a `protocol` class variable (e.g., `"message"`, `"file"`) and implements `handle()`. Handlers are automatically discovered and registered by `ModuleDiscoverer`, so the module dispatches incoming requests to the correct handler based on the input protocol.

See: [`src/digitalkin/modules/trigger_handler.py`](https://github.com/DigitalKin-ai/digitalkin/tree/main/src/digitalkin/modules/trigger_handler.py)

## Service Strategies (Local vs Remote)

The SDK uses a strategy pattern for services such as storage, filesystem, cost, and registry. Each service has a **local** implementation (e.g., `DefaultStorage`) for single-server deployments and a **remote** implementation (e.g., `GrpcStorage`) that communicates via gRPC. Configure which strategy to use via `services_config_strategies` and `services_config_params` on your module class.

See: [`examples/services/`](https://github.com/DigitalKin-ai/digitalkin/tree/main/examples/services/)

## Dynamic Schema Configuration

Setup models can include fields whose allowed values are fetched at runtime from external sources. Use the `Dynamic` metadata class with async fetcher functions to populate enum values, ranges, or other schema properties dynamically. Call `SetupModel.get_clean_model(force=True)` to trigger resolution.

See: [`examples/modules/dynamic_setup_module.py`](https://github.com/DigitalKin-ai/digitalkin/tree/main/examples/modules/dynamic_setup_module.py), [Dynamic Schema API](api/dynamic_schema.md)

## Module Lifecycle

A module goes through a well-defined lifecycle: `CREATED` -> `STARTING` -> `RUNNING` -> `STOPPING` -> `STOPPED` (or `FAILED` / `CANCELLED`). During execution, three concurrent tasks run inside a `TaskSession`: the main module coroutine, a heartbeat generator (reports liveness to SurrealDB every 2 seconds), and a signal listener (handles pause/resume/cancel). Understanding this lifecycle is essential for implementing proper `initialize()` and `cleanup()` methods.

See: [SDK Flow](architecture/sdk-flow.md)
