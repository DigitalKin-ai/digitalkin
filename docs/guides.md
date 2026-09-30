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

## Files and the Canonical File Format

Every file in the SDK is a `FileMetadata`: six fields, one definition — `id`, `name`, `type`, `content_type`, `size_bytes`, `file_url`. It is the payload the front-end file widget stores in form data, so a file selected in a setup form round-trips: what is submitted is what is persisted, and the widget repopulates itself when the form is reopened. `FilesystemRecord` inherits it and adds only the service-side fields (`checksum`, `storage_uri`, `status`, `visibility`, `content`), so a record placed in a `list[FileMetadata]` still serialises to exactly the six.

`type` is a `FileType` enum whose values are the protocol's own wire names (`FILE_TYPE_DOCUMENT`, `FILE_TYPE_IMAGE`, …). Compare with the enum rather than a string — `record.type is FileType.IMAGE`, never `record.file_type == "FILE_TYPE_IMAGE"`. Use `FileType.from_content_type("application/pdf")` to classify a MIME type instead of writing your own mapping.

See: [`src/digitalkin/models/services/filesystem.py`](https://github.com/DigitalKin-ai/digitalkin/tree/main/src/digitalkin/models/services/filesystem.py)

### Uploading Files

`FilesystemMixin.upload_file()` wraps a single upload and infers the category from the MIME type when you do not pass one. The `metadata` on an upload is validated against a pydantic model rather than being a free-form dict: `FileUploadMetadata` by default, or your own registered per module the same way storage collections are — `services_config_params = {"filesystem": {"config": {"metadata_model": MyFileMeta}}}`. Validation runs before anything reaches disk or the wire, and only the keys you actually set are sent.

See: [`src/digitalkin/mixins/filesystem_mixin.py`](https://github.com/DigitalKin-ai/digitalkin/tree/main/src/digitalkin/mixins/filesystem_mixin.py)

### Accepting Files in a Setup

`knowledge_files_input()` declares a setup field that takes uploaded files, together with the formats the module accepts:

```python
class MySetup(SetupModel):
    knowledge_files: knowledge_files_input(extensions=[".json"], config=False) = Field(
        default_factory=list,
        title="Knowledge Files",
        description="Files imported at config-setup time.",
    )
```

The allowed formats reach the widget as `ui:options.accept` and are enforced again on submit, since the widget's filter is only a hint. Pass `config=False`: a field marked `config=True` is stripped by `create_setup_model`, so the persisted setup version loses the file list and the form reopens empty. Call `FilesystemMixin.resolve_files()` in `run_config_setup` to backfill entries submitted as a bare id.

See: [`src/digitalkin/models/module/knowledge_files.py`](https://github.com/DigitalKin-ai/digitalkin/tree/main/src/digitalkin/models/module/knowledge_files.py)

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
