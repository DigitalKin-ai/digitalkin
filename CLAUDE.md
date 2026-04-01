# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DigitalKin is a Python SDK for building and managing agents within multi-agent systems. It provides a modular framework with gRPC communication, flexible service strategies, and support for both single-server and distributed deployments.

## Development Commands

### Environment Setup
```bash
# Initial setup (includes venv, dependencies, tests deps, and pre-commit hooks)
task setup-dev
source .venv/bin/activate

# Install only project dependencies
task install-deps

# Install development dependencies
task dev-deps

# Install test dependencies
task tests-deps
```

### Testing
```bash
# Run all tests with coverage
task run-tests

# Run specific test file
uv run pytest tests/path/to/test_file.py

# Run specific test function
uv run pytest tests/path/to/test_file.py::test_function_name

# Run tests with verbose output
uv run pytest -v

# Run tests matching a pattern
uv run pytest -k "pattern"
```

### Code Quality
```bash
# Format and lint code (runs ruff format, import sorting, and checks)
task linter

# Run type checking
uv run mypy src/digitalkin

# Run pre-commit hooks manually
uv run pre-commit run --all-files
```

### Building and Publishing
```bash
# Build package
task build-package

# Clean build artifacts
task clean

# Bump version (major, minor, patch, pre_l, or pre_n)
task bump-version -- patch

# Test package installation
task test-package
```

### Documentation
```bash
# Serve docs locally
uv run mkdocs serve

# Build docs
uv run mkdocs build

# Deploy docs with versioning (using mike)
uv run mike deploy --push --update-aliases 0.3 latest
```

## Architecture Overview

### Core Components

**Module System** (`src/digitalkin/modules/`)
- `BaseModule`: Abstract base class for all modules, using generics `[InputModelT, OutputModelT, SetupModelT, SecretModelT]`
- `ToolModule`: Specialized for utility/tool modules
- `ArchetypeModule`: Specialized for AI agent modules
- Modules don't implement `run()` directly; instead they register `TriggerHandler` subclasses

**Trigger System** (`modules/trigger_handler.py`)
- Protocol-based input dispatching via `TriggerHandler` classes
- Each handler declares a `protocol` class variable (e.g., "message", "file")
- Handlers automatically discovered and registered by `ModuleDiscoverer`
- Handlers inherit from `BaseMixin` for access to common functionality (cost tracking, chat history, file handling, logging)

**gRPC Servers** (`src/digitalkin/grpc_servers/`)
- `BaseServer`: Abstract foundation supporting sync/async and secure/insecure modes
- `ModuleServer`: Wraps modules and exposes them via gRPC, registers with RegistryServer
- `RegistryServer`: Central registry for module discovery
- `ModuleServicer`: Implements gRPC service interface (ConfigSetupModule, StartModule, StopModule, etc.)

**Job Management** (`src/digitalkin/core/job_manager/`)
- `BaseJobManager`: Abstract base extending TaskManager
- `SingleJobManager`: In-memory execution for single-server deployments
- Jobs stream output via asyncio.Queue and callbacks

**Task Management** (`src/digitalkin/core/task_manager/`)
- `TaskManager`: Lower-level task lifecycle management with concurrent task limits (semaphore-based waiting pool)
- `TaskSession`: Represents running task state with signal listening via TaskManagerStrategy
- Each task runs 2 concurrent sub-tasks: main coroutine and signal listener
- `TaskExecutor`: Supervisor pattern for task lifecycle (main + signal listener)

**Service Strategies** (`src/digitalkin/services/`)
- Strategy pattern with dependency injection
- `ServicesConfig`: Central configuration for local vs remote service implementations
- Services include: storage, cost, snapshot, registry, filesystem, agent, identity
- Each service has local (e.g., `DefaultStorage`) and remote (e.g., `GrpcStorage`) implementations

**Models** (`src/digitalkin/models/`)
- `DataTrigger`: Base for input/output trigger types with protocol-based discriminated union
- `DataModel[DataTriggerT]`: Generic wrapper containing trigger + annotations
- `SetupModel`: Module configuration with field filtering via `json_schema_extra` (`{"config": True}` for initial config, `{"hidden": True}` for runtime-only)
- `ModuleContext`: Context object carrying all dependencies (services, session data, callbacks, metadata)

### Key Design Patterns

1. **Generic Type Parameters**: Modules use 4 generic types for type-safe input/output/setup/secret handling
2. **Protocol-Based Dispatching**: Input protocol field routes to appropriate TriggerHandler
3. **Streaming via Callbacks**: Modules stream results through `context.callbacks.send_message()`
4. **Service Strategy Injection**: Services configured at module class level, instantiated per job, passed via ModuleContext
5. **Lifecycle State Machines**: Clear state transitions (CREATED → STARTING → RUNNING → STOPPING → STOPPED/FAILED/CANCELLED)
6. **Discovery and Registration**: Automatic discovery of trigger handlers, module registration with registry server

### Data Flow

```
gRPC Client
  → ModuleServicer.StartModule()
    → JobManager.create_module_instance_job()
      → TaskManager.create_task()
        → Module.start()
          → Module.initialize()
          → Module.run()
            → TriggerHandler.handle()
              → callbacks.send_message(output)
                → Queue → Stream → gRPC Response
```

### Signal Flow

```
TaskSession
  → Signal Listener → TaskManagerStrategy (gRPC polling or local)
  → Status Updates → TaskManager
```

## Important Conventions

### Code Philosophy (MANDATORY)
Every code change must be evaluated against these principles:

1. **Minimal Memory Footprint**: Write code as if every byte matters. No unnecessary variables, no redundant storage, no bloated data structures.
2. **Minimal CPU Cycles**: Avoid unnecessary computation. No extra method calls, no redundant operations, no over-abstraction.
3. **No Standalone Functions**: All functions must be methods inside classes. No module-level functions.
4. **No Global Variables**: No module-level constants or variables. Hardcode defaults inline only when used once alongside an env var (e.g., `os.environ.get("VAR", "default")`).
5. **No Over-Engineering**: No extra abstractions, no helper methods unless truly reused, no wrapper functions for single operations.
6. **Direct Code**: Prefer inline code over method extraction when the code is used once. Keep the call stack shallow.

Before writing any code, ask: "Is this the most minimal way to achieve this? Can I remove anything?"

### Prohibited Patterns
The following patterns are **strictly prohibited** in this codebase:

1. **No `hasattr()`, `getattr()`, `setattr()`**: These indicate poor type design. Use explicit type checks (`is None`, `is not None`) or proper type annotations instead. If an attribute might not exist, the class design is wrong.

### Docstring Standard (Google Style)
All docstrings must follow Google style with these sections (when applicable):

```python
def method(self, param1: str, param2: int) -> bool:
    """Brief one-line description.

    Longer description if needed (optional).

    Args:
        param1: Description of param1.
        param2: Description of param2.

    Returns:
        Description of return value.

    Raises:
        ValueError: When validation fails.

    Yields:
        Description of yielded values (for generators).
    """
```

Keep docstrings lean and professional. No flowery language, no numbered steps, no obvious explanations.

### Code Organization
- **Encapsulation**: Keep related functionality together within classes
- **Private methods**: Only create private methods (`_method_name`) if the code is reused within the class
- **No ClassVar for single-use**: Don't create class attributes for values used only once

### IDs
IDs flow through the entire system: `job_id`, `mission_id`, `setup_id`, `setup_version_id`. Always propagate these correctly.

### Pydantic Models
All data models use Pydantic for validation and serialization. JSON schemas are generated for module introspection.

### Async-First
Most operations are async/await. Use `async def` for handlers and module methods.

### Type Annotations
Comprehensive type hints are used throughout. Always add type annotations to new code.

### Structured Logging
The `extra` parameter is **only for global context IDs** that help correlate logs across the system (e.g., `job_id`, `mission_id`, `setup_id`, `setup_version_id`, `task_id`). These IDs are typically available via `self.session_ids` or `context.session.current_ids()`.

**Local-scope variables go in the log message, not in `extra`:**
```python
# GOOD: Global IDs in extra, local vars in message
logger.info("Task started (attempt %d/%d)", attempt, max_retries, extra=self.session_ids)
logger.error("Connection failed: %s", error_msg, extra=self.session_ids)

# BAD: Local vars in extra
logger.info("Task started", extra={"attempt": attempt, "error": error_msg})
```

If no global context is available, omit `extra` entirely and put everything in the message.

### Error Handling
Exceptions are properly caught and converted to gRPC status codes. Use appropriate error types from `grpc.StatusCode`.

### Resource Cleanup
All managers implement proper cleanup. Always close DB connections, stop tasks, and clean up resources in finally blocks or context managers.

### Schema Introspection
Modules expose JSON schemas for all formats. Use `get_clean_model()` on SetupModel to filter fields for initial configuration.

### Enums
- Enums stay as enums - no mapping dictionaries
- Initialize by name via bracket notation: `MyEnum[name]`
- Initialize by value: `MyEnum(value)`
- Compare via enum: `if status == MyEnum.VALUE`
- For raw string value: use `.value` property
- For raw name: use `.name` property

## Testing Patterns

Tests are organized by component:
- `tests/core/` - Task and job manager tests
- `tests/grpc_server/` - Server and servicer tests
- `tests/modules/` - Module and trigger handler tests
- `tests/services/` - Service strategy tests
- `tests/performances/` - Performance benchmarks

Use `pytest.mark.asyncio` for async tests. The `asyncio_mode = "auto"` setting in pyproject.toml enables automatic async test detection.

## Integration Points

- **Redis**: Durable message passing via Redis Streams, session state, signal pub/sub
- **gRPC**: All inter-service communication
- **Protobuf**: Message definitions from `digitalkin-proto` package

## Examples

See `examples/` directory for:
- `examples/modules/` - Example module implementations (minimal, llm, google search)
- `start_grpc_server_module.py` - Start a module server
- `start_grpc_server_registry.py` - Start a registry server
- `start_grpc_client.py` - Example client interaction

## Creating New Modules

1. Subclass `BaseModule` or `ToolModule`/`ArchetypeModule`
2. Define input/output/setup/secret models as Pydantic classes
3. Create `TriggerHandler` subclasses with unique `protocol` values
4. Implement `handle()` method in each trigger handler
5. Configure services via `ServicesConfig` class attribute
6. Register module with a ModuleServer

## Version Management

This project uses conventional commits and semantic versioning. Use the following commit prefixes:
- `feat:` - New features (minor version bump)
- `fix:` - Bug fixes (patch version bump)
- `feat!:` or `fix!:` - Breaking changes (major version bump)
- `docs:` - Documentation changes
- `refactor:` - Code refactoring
- `test:` - Test changes
- `chore:` - Build/tooling changes

Bump version with: `task bump-version -- major|minor|patch|pre_l|pre_n`

## Publishing Process

1. Update code and commit changes (following conventional commit standard)
2. Use `task bump-version -- major|minor|patch` to commit new version
3. Use GitHub "Create Release" workflow to publish
4. Workflow automatically publishes to Test PyPI and PyPI

## Documentation

Documentation is built with MkDocs Material and supports versioning via mike. The `mkdocs.yml` configures:
- API documentation via mkdocstrings
- Code snippets with syntax highlighting
- Mermaid diagrams
- Version management with mike
- LLM-friendly text output via llmstxt plugin

Documentation files are in `docs/` and are deployed to GitHub Pages.
