# CLAUDE.md

DigitalKin is a Python SDK for building and managing agents in multi-agent systems: gRPC transport, Redis Streams for durable messaging, pluggable service strategies.

Detailed conventions live in `.claude/rules/` and load automatically when you touch matching files. Architecture is documented in `docs/architecture/`, `docs/gateway_protocol.md` and `docs/testing_strategy.md` — read those rather than asking for a tour.

## Code philosophy (MANDATORY)

Evaluate every change against these. They apply while writing new files, not only when editing existing ones.

1. **Minimal memory footprint.** No unnecessary variables, no redundant storage, no bloated structures.
2. **Minimal CPU cycles.** No extra method calls, no redundant operations, no over-abstraction.
3. **No standalone functions.** Every function is a method inside a class.
4. **No global variables.** No module-level constants. Inline a default only at a single use site alongside an env var.
5. **No over-engineering.** No helper method unless genuinely reused. No wrapper for a single operation.
6. **Direct code.** Prefer inline over extraction when used once. Keep the call stack shallow.

Before writing code, ask: is this the most minimal way to achieve it, and can anything be removed?

`hasattr` / `getattr` / `setattr` are prohibited outright. So are module-level functions and constants. `.claude/hooks/guard-edit.sh` blocks all three at the tool boundary.

## Commands

```bash
task setup-dev          # venv + deps + pre-commit hooks
task linter             # ruff format, import sort, ruff check, mypy
task run-tests          # full suite via docker compose
task check              # linter + tests
task build-package      # uv build

uv run pytest tests/path/to/test_file.py::test_name
uv run pytest -m smoke              # select by marker
uv run pytest --timeout=60 -q
uv run mypy src/digitalkin
uv run mkdocs serve
```

Tests need the docker compose stack (Redis). `uv run pytest` alone will skip or fail integration tests.

## Workflow

- Audit and plan before modifying code. State the plan, then execute it.
- Ask before editing outside the agreed scope, including in auto mode.
- Never edit version strings. `task bump-version` is the user's call alone; `__version__.py` and `.bumpversion.toml` are deny-listed.
- Do not run `git` in the main session — it is blocked by hook. Read files for baseline checks. Review subagents may read git for diffs.
- A behavior-changing bug fix emits a `[VALIDATE <id>]` log plus a `# TODO(validate)` comment. Sweep them with `rg "TODO\(validate\)"` after production validation.
- When dropping or filtering data, log every dropped item with its content, not an aggregate count.

## Review tooling

`/dk-check` fans out the review agents and returns one verdict. Individually: `code-reviewer`, `test-auditor`, `grpc-breaking`, `grpc-test-coverage`, `quality-gate`. Rubrics live in the `dk-review` and `dk-test-review` skills — keep them in sync with `.claude/rules/` when conventions change.

**Job Management** (`src/digitalkin/core/job_manager/`)
- `BaseJobManager`: Abstract base extending TaskManager
- `SingleJobManager`: Runs module instances in-process; `stop()` cancels live tasks (reason `shutdown`) and releases the Redis listener. Config-setup sessions are tracked apart from task slots
- Task output goes to Redis (see Data Flow); the in-memory session queue is only used by config-setup

**Task Management** (`src/digitalkin/core/task_manager/`)
- `BaseTaskManager`: Task lifecycle with concurrent task limits (semaphore-based waiting pool); `_cleanup_task` is shielded so a cancel can't interrupt it
- `ModuleRunner`: Gateway-side driver of one task — resolves setup, preloads the module, checks the `cancel:{id}` tombstone, writes every output to Redis via `_on_output`
- `TaskExecutor`: Runs the module coroutine as a single asyncio task; a cancel arrives as `task.cancel()` from `SharedRedisListener` and is handled in its `except CancelledError`
- `TaskSession`: Per-task state (status, cancellation reason, `pending_signal_action`); `cleanup()` runs `module.stop()` before `context.cleanup()`
- `redis/`: `RedisClient`, `SharedRedisListener` (one `psubscribe signal_ch:*` per process), `proto_streams` (stream read/write + cursor), `redis_idempotency` (`idem:{id}` claim)

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
Client
  → Gateway StartStream (claim idem:{id}, seed task:{id}:stream)
    → dial-back → ModuleRunner.run()
      → resolve setup → preload module → JobManager.run_instance()
        → TaskManager.create_task() → TaskExecutor
          → Module.start() → initialize() → run()
            → TriggerHandler.handle()
              → callbacks.send_message(output)
                → ModuleRunner._on_output → XADD task:{id}:stream
                  → Gateway Stream reader → StreamClient (+ stream.* sentinels)
```

Lifecycle is in-band: `stream.start`, `stream.error`, `stream.cancelled`, `stream.end` travel in the data Struct's `protocol` field (see `docs/gateway_protocol.md`).

### Signal Flow

```
Client SendSignal(CANCEL)
  → Gateway: pipeline EXISTS idem:{id} + SET cancel:{id} (tombstone) + PUBLISH signal_ch:{id}
    → SharedRedisListener.dispatch_signal (first cancel only)
      → task.cancel() → TaskExecutor except CancelledError → TaskSession._handle_cancel
        → Module.stop() emits stream.cancelled + stream.end
ModuleRunner checks the tombstone before/after preload and after run_instance (cancel sent before the task exists).
```

`stop` is a hard cancel that only changes the recorded reason. Server shutdown order: stop accepting RPCs → `job_manager.stop()` → gateway stop → Redis clients → channels.

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
Propagate `task_id`, `setup_id`, and `mission_id` through the system whenever they are available.

### Pydantic Models
All data models use Pydantic for validation and serialization. JSON schemas are generated for module introspection.

### Async-First
Most operations are async/await. Use `async def` for handlers and module methods.

### Type Annotations
Comprehensive type hints are used throughout. Always add type annotations to new code.

### Structured Logging
The `extra` parameter is **only for global context IDs** that help correlate logs across the system (e.g., `task_id`, `setup_id`, `mission_id`). These IDs are typically available via `self.session_ids` or `context.session.current_ids()`.

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

### Validation Markers
Every behavior-changing fix logs `"[VALIDATE <UPPER-KEBAB-ID>] ..."` next to a `# TODO(validate): <ID> <what to check>` comment. Keep `docs/validation_checklist.md` in sync; remove validated markers with `rg -n "TODO\(validate\)|\[VALIDATE" src`.

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

- **Redis**: Output streams (`task:{id}:stream`, `task:{id}:cursor`), idempotency claims (`idem:{id}`), cancel tombstones (`cancel:{id}`), signal pub/sub (`signal_ch:{id}`). Every key carries a TTL
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

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
