# Writing modules

Start from the [quick start](index.md#quick-start). The runtime path of a task is in [architecture.md](architecture.md).

Public imports come from `digitalkin`: `ToolModule`, `ArchetypeModule`, `TriggerHandler`, `ModuleContext`,
`ModuleStatus`, `ServicesConfig`, `GrpcCommunication` and the `M2M*` errors.
`BaseModule` (`digitalkin.modules._base_module`) is private; subclass one of the two public bases.

## Anatomy of a module

A module subclasses `ToolModule[Input, Output, Setup, Secret]` or `ArchetypeModule[...]`.

| Base | Registry type | Difference |
| --- | --- | --- |
| `ToolModule` | `TOOL_MODULE` | Called by agents or other modules. |
| `ArchetypeModule` | `ARCHETYPE` | Resolves the `ToolReference` fields of its setup into `context.tool_cache` before `initialize()`. |

| Class attribute | Required | Purpose |
| --- | --- | --- |
| `name`, `description` | `name` | Display name and documentation. |
| `input_format` | yes | `DataModel` whose `root` is a union of input triggers. |
| `output_format` | yes | `DataModel` whose `root` is a union of output triggers. |
| `setup_format` | yes | `SetupModel` subclass. |
| `secret_format` | yes | `BaseModel` describing the secret payload (schema only). |
| `metadata` | yes | `ClassVar[dict]`; `module_id` and `version` are read at registration. |
| `triggers_discoverer` | yes | `ModuleDiscoverer(packages=["triggers"])`. |
| `services_config_strategies` | no | Per-service strategy overrides (see [Services](#services-and-strategies)). |
| `services_config_params` | in practice | Constructor kwargs per service; `storage` and `cost` need `{"config": {...}}`. |

| Method | Required | Called |
| --- | --- | --- |
| `async initialize(self, context, setup_data)` | abstract | Once per task, before the first trigger. Build clients and agents here. |
| `async cleanup(self)` | abstract | On stop. No `context` argument; use `self.context`. |
| `async run_config_setup(self, context, config_setup_data)` | optional | During config setup; returns the setup to persist. Default returns it unchanged. |

`get_module_id()` returns `DIGITALKIN_MODULE_ID` when set, else `metadata["module_id"]`.
Registration in remote mode fails if neither is set.

## Triggers

A trigger handler serves one input `protocol`.

```python
@MyModule.register
class SearchTrigger(TriggerHandler):
    protocol = "search"
    description = "Web search."      # shown in registry documentation
    input_format = SearchIn          # the DataTrigger class, not the DataModel
    output_format = SearchOut

    async def handle(self, input_data: SearchIn, setup_data: MySetup, context: ModuleContext) -> None:
        ...
```

- `@MyModule.register` adds the class to the discoverer. `ModuleServer` calls `MyModule.discover()` at startup.
- `discover()` imports every `*_trigger.py` file in `triggers_discoverer.packages` (default `file_pattern`).
- Dispatch reads `input.root.protocol`, then picks the handler whose `input_format` matches `isinstance(root, ...)`.
- `handle()` receives the trigger itself (`input.root`): read `input_data.text`, not `input_data.root.text`.
- Handlers are instantiated once per task with `Handler(context)`. A custom `__init__` must accept `context`.
- Built-in triggers `healthcheck_ping`, `healthcheck_services` and `healthcheck_status` are added to every module.
- The registry of handlers is shared per process: serve one module class per process.

`TriggerHandler` mixes in helpers: `add_cost`, `get_cost`, `log_info`/`log_debug`/..., and the AG-UI `send_message`.

## Inputs and outputs

```python
class SearchIn(DataTrigger):
    protocol: Literal["search"] = "search"
    query: str

class MyInput(DataModel[SearchIn | ChatIn]):
    root: SearchIn | ChatIn = Field(discriminator="protocol")
```

`DataTrigger` carries `protocol` and `created_at`. `DataModel` carries `root` and `annotations: dict[str, str]`.

### Function labels

Each trigger of a tool module declares the label users see when an agent calls it, in English:

```python
class SearchIn(DataTrigger):
    display_name = "Search documents"
    protocol: Literal["search"] = "search"
    query: str
```

- Mandatory on `ToolModule`: defining a tool module whose `input_format` has a trigger without `display_name` raises `TypeError` naming the protocol. SDK utility protocols (`healthcheck_*`) are exempt; `ArchetypeModule` is not checked.
- `display_name` is a class variable: it is published on the trigger's schema definition, never in `properties`, so the LLM never sees it.
- The tool's own label is its registry setup name. The front receives both as `metadata.toolDisplayName` / `metadata.functionDisplayName` on `TOOL_CALL_START` (see [agui_events.md](agui_events.md#tool-calls)).
- Upgrading a tool repo to this SDK version: add `display_name` to every trigger of its input union, or the module fails at import.

Emit outputs with the context callback, as many times as needed:

```python
await context.callbacks.send_message(MyOutput(root=SearchOut(results=hits)))
```

Each call becomes one entry on the task's Redis stream. The SDK appends `stream.end` when the run returns.

For AG-UI frontends, call `await self.send_message(context, event)` on the handler instead.
It takes a `digitalkin.models.events` event (`RunStartedEvent`, `RunContentEvent`, `ToolCallStartedEvent`, ...).
It converts the event to `AgUiOutput` frames and manages run state.
The wire contract is in [agui_events.md](agui_events.md).

## Setup and secrets

`SetupModel` fields are filtered by `json_schema_extra`:

| Marker | In config-setup schema | In setup schema |
| --- | --- | --- |
| none | yes | yes |
| `{"config": True}` | yes | no |
| `{"ui:widget": "hidden"}` | no | yes |

```python
class MySetup(SetupModel):
    model_name: str = Field(default="gpt-4o", json_schema_extra={"config": True})
    temperature: float = 0.7
    system_prompt: str = Field(default="", json_schema_extra={"ui:widget": "hidden"})
```

`await MySetup.get_clean_model(config_fields=..., hidden_fields=..., force=False)` builds the filtered class.
Both flags are keyword-only and required. `force=True` re-resolves dynamic fields and bypasses the cache.

Config setup runs `run_config_setup(context, config_setup_data)` once, before the setup is saved.
Use it for one-off work such as indexing files; the module then initializes with the returned setup.

Secrets are not injected. Read them on demand with `await context.secret.get_secret()` (a `dict` or `None`).
In remote mode this calls `UserProfileService.GetSetupSecret` for the current setup and mission.

## ModuleContext

| Attribute | Content |
| --- | --- |
| `communication` | Module-to-module calls (`call_module`, `get_module_schemas`). |
| `cost` | Cost reporting and limits. |
| `filesystem` | File storage. |
| `identity` | Identity strategy. |
| `registry` | Module and setup discovery (`discover_by_id`). |
| `secret` | `get_secret()` for the current setup. |
| `storage` | Typed record storage; collections declared in `services_config_params["storage"]["config"]`. |
| `user_profile` | User profile access. |
| `setup` | Setup service, shared from the servicer; may be `None`. |
| `task_manager` | Signal transport for the task; may be `None`. |
| `session` | `job_id`, `mission_id`, `setup_id`, `setup_version_id`, `timezone`; `current_ids()` for log `extra`. |
| `callbacks` | `send_message` (outputs) and `logger`. |
| `metadata`, `helpers` | Free namespaces. |
| `state` | Per-task scratch namespace. |
| `shared` | Process-lifetime dict shared by all module instances. |
| `tool_cache` | Resolved tools (archetypes). See also `context.resolve_tool(setup_id)` and `context.create_openai_style_tools(setup_id)`. |
| `request_metadata` | Incoming gRPC headers. |
| `agui_run` | AG-UI run state, set by the AG-UI mixin. |

## Services and strategies

`ServicesConfig` picks one implementation per service from the services mode.

| Service | Local | Remote |
| --- | --- | --- |
| `storage` | `DefaultStorage` | `GrpcStorage` |
| `cost` | `DefaultCost` | `GrpcCost` |
| `registry` | `DefaultRegistry` | `GrpcRegistry` |
| `filesystem` | `DefaultFilesystem` | `GrpcFilesystem` |
| `identity` | `DefaultIdentity` | `DefaultIdentity` |
| `communication` | `DefaultCommunication` | `GrpcCommunication` |
| `user_profile` | `DefaultUserProfile` | `GrpcUserProfile` |
| `secret` | `DefaultSecret` | `GrpcSecret` |

Remote strategies get `client_config` from `CLIENT_CHANNEL_*` unless `services_config_params` provides one.
`secret` reuses the `user_profile` params when it has none.
`storage` always registers the SDK-owned `mission` collection (`MissionRecord`); a module's own `mission` entry wins.

```python
from digitalkin.services.services_models import ServicesStrategy

class MyModule(ToolModule[...]):
    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {
        "storage": ServicesStrategy(local=MyStorage, remote=GrpcStorage),
    }
    services_config_params: ClassVar[dict[str, Any]] = {
        "storage": {"config": {"notes": NoteRecord}},   # collection name -> model
        "cost": {"config": {}},
    }
```

## Dynamic and conditional schemas

`json_schema_extra` is evaluated at import time. Use `Dynamic` when the allowed values are only known at runtime.

```python
from typing import Annotated
from digitalkin.utils import Dynamic

async def fetch_models() -> list[str]:
    return await my_api.list_models()

class AgentSetup(SetupModel):
    model_name: Annotated[str, Dynamic(enum=fetch_models)] = Field(
        default="gpt-4o", json_schema_extra={"config": True, "ui:widget": "select"},
    )
```

- A fetcher is a zero-argument sync or async callable (`Fetcher`). `Dynamic` is an alias of `DynamicField`.
- `get_clean_model(..., force=True)` runs the fetchers into `json_schema_extra`. The SDK does this when serving schemas.
- Fetchers in nested models are resolved too: direct, optional, `list`, `dict`, `set`, `tuple` and unions.
- `DynamicSchemaResolver` exposes static methods: `has_dynamic`, `get_fetchers`, `resolve`, `resolve_safe`.
- `resolve(fetchers, timeout=None)` runs fetchers in parallel and raises on the first error.
- `resolve_safe(...)` returns a `ResolveResult` with `values`, `errors`, `success`, `partial` and `get(key, default)`.

`Conditional(trigger="mode", show_when="advanced")` in `Annotated` metadata, with `ConditionalSchemaMixin` as a base,
emits JSON Schema `if`/`then` clauses so react-jsonschema-form shows the field only when the trigger matches.

Full example: [examples/modules/dynamic_setup_module.py](https://github.com/DigitalKin-ai/digitalkin/tree/main/examples/modules/dynamic_setup_module.py).

## Module-to-module calls

`context.communication.call_module(...)` opens a gateway stream to another module and yields its outputs.

```python
from digitalkin import GrpcCommunication, M2MAtCapacityError, M2MCallTimeout, M2MTargetUnavailable

target = await context.registry.discover_by_id("modules:search")
try:
    async for out in context.communication.call_module(
        module_address=target.address,
        module_port=target.port,
        input_data={"root": {"protocol": "search", "query": q}},
        setup_id=tool_setup_id,
        mission_id=context.session.mission_id,
    ):
        if (err := GrpcCommunication.stream_error(out)) is not None:
            code, message = err
            break
        ...  # out is a google.protobuf Struct
except (M2MAtCapacityError, M2MTargetUnavailable, M2MCallTimeout):
    ...
```

| Error | Meaning |
| --- | --- |
| `M2MAtCapacityError` | No outbound call slot freed in time. |
| `M2MTargetUnavailable` | The target's circuit breaker is open. |
| `M2MCallTimeout` | No output from the target within the call timeout. |

`stream.start` and heartbeats are filtered; iteration stops at `stream.end`.
`ModuleServer` wires the call registry; a bare `GrpcCommunication` without it raises `RuntimeError`.
In local mode `DefaultCommunication.call_module` yields nothing.
Limits and timeouts are `DIGITALKIN_M2M_*` settings, listed in [environment_variables.md](environment_variables.md).

## Agno integration

Install `digitalkin[agno]`: Agno is not a core dependency, and `digitalkin.community.agno` (with `module_toolkit` and `toolkits`) requires it at import.

| Component | Role |
| --- | --- |
| `AgnoStreamAdapter` | Converts Agno stream events to `digitalkin.models.events`. One adapter per stream. |
| `AgnoHitlRunner` | Runs an agent, persists a paused run in `PausedRunStore` and resumes it on the tool result. |
| `AguiTools.make_tools_factory(base_tools)` | Agno `tools=` factory adding the run's AG-UI frontend tools as external functions. |
| `AgUiStreamInput` | `agui_stream` input trigger for AG-UI 1.0 `RunAgentInput`; upgrades legacy `binary` parts to media parts. |
| `ModuleToolkit(context, tool_module_info)` | Exposes a resolved tool module as Agno tools. |
| `DkToolkit` | Base toolkit with a fixed result envelope and optional AG-UI notifications. |
| `DefaultToolkits.build(context, session_id)` | `ChatHistoryTools`, `UserProfileTools`, the registry managers (when `context.setup` is set) and `LoadManager`. |

Typical AG-UI trigger:

```python
from agno.agent import Agent
from digitalkin.community.agno import AgnoHitlRunner, AguiTools, AgUiStreamInput
from digitalkin.community.agno.toolkits import DefaultToolkits
from digitalkin.models.module.ag_ui import AgUiOutput

class ChatTrigger(TriggerHandler):
    protocol = "agui_stream"
    input_format = AgUiStreamInput
    output_format = AgUiOutput

    async def handle(self, input_data: AgUiStreamInput, setup_data: MySetup, context: ModuleContext) -> None:
        tools = DefaultToolkits.build(context, session_id=input_data.thread_id)
        agent = Agent(model=..., db=db, tools=AguiTools.make_tools_factory(tools), cache_callables=False)
        DefaultToolkits.bind_host(tools, agent)
        runner = AgnoHitlRunner(agent=agent, storage=context.storage)
        await runner.handle_agui_input(input_data, send=lambda e: self.send_message(context, e), context=context)
```

`handle_agui_input` resumes a paused run when the input carries the tool result, otherwise starts a fresh run.
To name the mission, override `mission_title(self, context) -> str | None` on the trigger: return None while the conversation says too little, the SDK retries on the next run and emits a null title once the attempts run out ([agui_custom_events.md#mission-title](agui_custom_events.md#mission-title)). No storage declaration is needed.
`PausedRunStore` needs a storage collection: add `HITL_STORAGE_CONFIG` to `services_config_params["storage"]["config"]`.

### Memory

The SDK sets no Agno history option. Recommended agent settings:

| Setting | Value | Why |
| --- | --- | --- |
| `db` | A persistent Agno db | History, HITL resume and `ChatHistoryTools` read the session from it. |
| `num_history_runs` | Small, e.g. 3 | Every message in the window is resent each turn. |
| `store_history_messages` | `False` | `True` re-embeds history in each stored run; storage grows quadratically. |
| `max_tool_calls_from_history` | e.g. 3 | Old tool results are large and rarely useful. |

What the SDK trims already:

- `PausedRunStore` drops history messages and run `events` from the saved run. `continue_run` reloads history from the db.
- `ModuleToolkit` keeps only the last result frame and last error frame of a tool call.
- `ChatHistoryTools.outline_chat_history` returns at most 50 rows by default; `first`/`last` cap at 200.
- `ChatHistoryTools.read_chat_messages` reads at most 50 ids; `max_content_chars` caps at 20000. Capped results carry a `note`.

## Running locally vs remote

| | Local | Remote (default) |
| --- | --- | --- |
| Select | `SERVICE_MODE=local` or `-d LOCAL` | `SERVICE_MODE=remote` |
| Services | In-process `Default*` strategies | gRPC services provider at `CLIENT_CHANNEL_HOST:PORT` |
| Registry | No registration | Registers `module_id`, schemas and advertise address at startup |
| Minimum env (Redis always required) | `DIGITALKIN_REDIS_URL` | [`.env.minimum`](https://github.com/DigitalKin-ai/digitalkin/blob/main/.env.minimum) |

`ModuleServer(module_class, client_config=None, interceptors=None)` reads host and port from `SERVER_CHANNEL_*`.
In remote mode the registry records `SERVER_CHANNEL_ADVERTISE_HOST` (falls back to `SERVER_CHANNEL_HOST`); set a routable address.
Deployment and tuning: [operations.md](operations.md). All variables: [environment_variables.md](environment_variables.md).

Working examples: `examples/redis_demo/`, `examples/bench_module/`, `examples/monitoring/`,
`examples/modules/dynamic_setup_module.py`.

## Common mistakes

| Mistake | Fix |
| --- | --- |
| `input_data.root.text` in `handle()` | `handle()` gets the trigger: `input_data.text`. |
| `self.send_message(context, MyOutput(...))` or `self.send_message(MyOutput(...))` | That is the AG-UI event dispatcher. Use `context.callbacks.send_message(...)`. |
| `async def cleanup(self, context)` | `cleanup(self)` takes no context. |
| Missing `initialize` | It is abstract; the class cannot be instantiated. |
| `input_format = MyInput` (the `DataModel`) on a handler | Set the `DataTrigger` class; dispatch uses `isinstance` on `root`. |
| Trigger file not named `*_trigger.py` | Discovery skips it; the protocol has no handler. |
| `TypeError: <Module>: trigger(s) without display_name: …` | Add `display_name = "…"` to each listed trigger of the tool's input union. |
| No `storage`/`cost` `config` in `services_config_params` | Strategy construction fails with a missing `config` argument. |
| `{"hidden": True}` on a setup field | The filter reads `{"ui:widget": "hidden"}`. |
| `-d local` | The flag's choices are `LOCAL` / `REMOTE`; use `SERVICE_MODE=local` or `-d LOCAL`. |
