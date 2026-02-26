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

A module goes through a well-defined lifecycle: `CREATED` -> `STARTING` -> `RUNNING` -> `STOPPING` -> `STOPPED` (or `FAILED` / `CANCELLED`). During execution, two concurrent tasks run inside a `TaskSession`: the main module coroutine and a signal listener (handles stop/cancel via TaskManagerStrategy). Understanding this lifecycle is essential for implementing proper `initialize()` and `cleanup()` methods.

See: [SDK Flow](architecture/sdk-flow.md)

## Architecture: Resilience & Concurrency

In-depth documentation of the SDK's fault tolerance and concurrency control systems, based on real production incident analysis.

### Retry & Fault Tolerance

Three independent retry layers protect against transient gRPC failures: channel-level service config, application-level `exec_grpc_query()`, and batch-level `_SharedSendBuffer._flush()` with exponential backoff and jitter. Includes retryable vs non-retryable error classification and before/after comparisons.

See: [architecture/resilience.md](architecture/resilience.md)

### Admission Queue (Concurrency Control)

Two-phase admission model replacing hard rejection under burst load. Phase 1 (system gate) fast-rejects when the system is truly full. Phase 2 (task slot) queues admitted tasks patiently until an execution slot frees up. Includes capacity planning guidelines and log analysis findings.

See: [architecture/admission-queue.md](architecture/admission-queue.md)

### Concurrency Model

Full system view of the three-layer architecture: gRPC server → Task Manager → Signal I/O. Covers the complete request lifecycle, shared resources (_SharedPoller, _SharedSendBuffer, channel cache), and event loop budget analysis.

See: [architecture/concurrency-model.md](architecture/concurrency-model.md)

### gRPC Tuning Guide

Comprehensive environment variable reference for all three layers, with recommended configurations for small/medium/large instances.

See: [grpc-tuning.md](grpc-tuning.md)
