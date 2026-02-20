# SDK Flow: Server to Module Execution

## Architecture Stateless

```
┌─────────────────────────────────────────────────────────────────────┐
│                    ModuleServer (Long-lived)                         │
│  - Listens to gRPC continuously                                      │
│  - Stores NO state between requests                                  │
│  - Each request = new module instance                                │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Per-Request (Ephemeral)                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   job_id     │  │   Module     │  │ TaskSession  │              │
│  │   (uuid)     │  │   Instance   │  │   + Queue    │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
│         │                 │                  │                       │
│         └────────────────┼──────────────────┘                       │
│                          │                                           │
│                     Destroyed after execution                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Flow Diagram

```mermaid
flowchart TB
    subgraph Client["Client"]
        C1[ConfigSetupModuleRequest]
        C2[StartModuleRequest]
        C3[Receive Stream Response]
    end

    subgraph Server["ModuleServer - Stateless Long-lived"]
        S1[gRPC Listener]
        S2[ModuleServicer]
    end

    subgraph ConfigSetupFlow["ConfigSetupModule Flow"]
        CS1[Parse request.content → ConfigSetupModel]
        CS2[Parse setup_version → SetupModel]
        CS3[JobManager.create_config_setup_instance_job]
        CS4[ModuleFactory.create_module_instance]
        CS5[BaseModule.__init__]
        CS6[Create ModuleContext + Services]
        CS7[TaskSession with Queue]
        CS8[module.start_config_setup]
        CS9["asyncio.gather:<br/>_resolve_tools || run_config_setup"]
        CS10[callback → Queue → Response]
    end

    subgraph StartModuleFlow["StartModule Flow"]
        SM1[Parse input → InputModel]
        SM2[Load SetupModel from storage]
        SM3[JobManager.create_module_instance_job]
        SM4[ModuleFactory.create_module_instance]
        SM5[BaseModule.__init__]
        SM6[Create ModuleContext + Services]
        SM7[LocalTaskManager.create_task]
        SM8[TaskSession with Queue + SurrealDB]
    end

    subgraph TaskExecution["TaskExecutor - 3 Concurrent Tasks"]
        TE1[main_task:<br/>module.start]
        TE2[heartbeat_task:<br/>SurrealDB heartbeat every 2s]
        TE3[signal_task:<br/>Listen pause/resume/cancel]
    end

    subgraph ModuleStart["module.start Lifecycle"]
        MS1[Build ToolCache from SetupModel]
        MS2[Set context.tool_cache]
        MS3[Send ModuleStartInfoOutput]
        MS4[await initialize]
        MS5[Init TriggerHandlers]
        MS6[await run]
    end

    subgraph ModuleRun["module.run - Protocol Dispatch"]
        MR1[Validate InputModel]
        MR2[Get protocol from input.root.protocol]
        MR3[Lookup TriggerHandler by protocol]
        MR4[handler.handle input, setup, context]
        MR5[callback → send_message]
    end

    subgraph ModuleStop["module.stop Cleanup"]
        MST1[status = STOPPING]
        MST2[await cleanup]
        MST3[Send EndOfStreamOutput]
        MST4[status = STOPPED]
    end

    subgraph Streaming["Queue-Based Streaming"]
        Q1[asyncio.Queue maxsize=1000]
        Q2[add_to_queue job_id, output]
        Q3[generate_stream_consumer]
        Q4[yield StartModuleResponse]
    end

    %% Client to Server
    C1 --> S1
    C2 --> S1
    S1 --> S2

    %% ConfigSetup Flow
    S2 -->|ConfigSetupModule RPC| CS1
    CS1 --> CS2
    CS2 --> CS3
    CS3 --> CS4
    CS4 --> CS5
    CS5 --> CS6
    CS6 --> CS7
    CS7 --> CS8
    CS8 --> CS9
    CS9 --> CS10
    CS10 -->|Response| C3

    %% StartModule Flow
    S2 -->|StartModule RPC| SM1
    SM1 --> SM2
    SM2 --> SM3
    SM3 --> SM4
    SM4 --> SM5
    SM5 --> SM6
    SM6 --> SM7
    SM7 --> SM8
    SM8 --> TaskExecution

    %% Task Execution
    TE1 --> ModuleStart
    TE2 -.->|Monitoring| SM8
    TE3 -.->|Control| SM8

    %% Module Start
    MS1 --> MS2
    MS2 --> MS3
    MS3 --> MS4
    MS4 --> MS5
    MS5 --> MS6
    MS6 --> ModuleRun

    %% Module Run
    MR1 --> MR2
    MR2 --> MR3
    MR3 --> MR4
    MR4 --> MR5
    MR5 --> Q2

    %% Module Stop
    ModuleRun --> ModuleStop
    MST3 --> Q2

    %% Streaming
    Q2 --> Q1
    Q1 --> Q3
    Q3 --> Q4
    Q4 --> C3
```

---

## Two Main Flows

| Step | ConfigSetupModule | StartModule |
|------|-------------------|-------------|
| 1 | Parse ConfigSetupModel | Parse InputModel |
| 2 | Parse SetupModel (config fields) | Load SetupModel from storage |
| 3 | Create Module Instance | Create Module Instance |
| 4 | `start_config_setup()` | `create_task()` with 3 concurrent tasks |
| 5 | `_resolve_tools()` + `run_config_setup()` in parallel | `module.start()` → `run()` → TriggerHandler |
| 6 | Return updated SetupModel | Stream outputs via Queue |

---

## ConfigSetupModule Flow Detail

```
Client sends ConfigSetupModuleRequest
    │
    ▼
ModuleServicer.ConfigSetupModule()
    │
    ├─ Parse request.content → ConfigSetupModel
    ├─ Parse request.setup_version.content → SetupModel (config_fields=True)
    │
    ▼
JobManager.create_config_setup_instance_job()
    │
    ├─ job_id = uuid.uuid4()
    │
    ├─ module = ModuleFactory.create_module_instance()
    │       │
    │       └─ BaseModule.__init__()
    │           ├─ status = CREATED
    │           └─ context = ModuleContext(services, session, callbacks)
    │
    ├─ TaskSession(job_id, queue)
    │
    └─ module.start_config_setup(config_setup_data, callback)
            │
            ├─ status = RUNNING
            │
            └─ asyncio.gather(
                   _resolve_tools(config_setup_data),
                   run_config_setup(context, config_setup_data)
               )
               │
               └─ callback(setup_model) → Queue → Response
```

---

## StartModule Flow Detail

```
Client sends StartModuleRequest
    │
    ▼
ModuleServicer.StartModule()
    │
    ├─ input_data = parse InputModel
    ├─ setup_data = load from SetupStrategy
    │
    ▼
JobManager.create_module_instance_job()
    │
    ├─ job_id = uuid.uuid4()
    │
    ├─ module = ModuleFactory.create_module_instance()
    │
    ▼
LocalTaskManager.create_task()
    │
    ├─ TaskSession(job_id, queue, SurrealDB connection)
    │
    └─ TaskExecutor.execute_task()
            │
            ├─ main_task: module.start(input_data, setup_data, callback)
            ├─ heartbeat_task: session.generate_heartbeats()
            └─ signal_task: session.listen_signals()
```

---

## module.start() Lifecycle

```
BaseModule.start(input_data, setup_data, callback)
    │
    ├─ context.callbacks.send_message = callback
    │
    ├─ tool_cache = setup_data.build_tool_cache()
    ├─ context.tool_cache = tool_cache
    │
    ├─ callback(ModuleStartInfoOutput)  ──→ Queue
    │
    ├─ await initialize(context, setup_data)
    │
    ├─ triggers_discoverer.init_handlers(context)
    │
    ├─ await run(input_data, setup_data)
    │       │
    │       ├─ handler = get_trigger(input.root.protocol)
    │       └─ handler.handle(input, setup, context)
    │               │
    │               └─ send_message(output) ──→ Queue
    │
    └─ await stop()
            │
            ├─ await cleanup()
            └─ callback(EndOfStreamOutput) ──→ Queue
```

---

## Protocol-Based Dispatch

```
InputModel
    │
    └─ root: DataTrigger
           │
           └─ protocol: str  ←── "message", "file", "healthcheck_ping", etc.
                   │
                   ▼
           TriggerHandler lookup by protocol
                   │
                   ▼
           handler.handle(input, setup, context)
```

---

## Queue-Based Streaming

```
TriggerHandler.handle()
    │
    └─ await send_message(output)
            │
            ▼
       callback(output)
            │
            ▼
       add_to_queue(job_id, output)
            │
            ▼
       session.queue.put(output.model_dump())
            │
            ▼
       ┌─────────────────────────────────┐
       │  asyncio.Queue (maxsize=1000)   │
       └─────────────────────────────────┘
            │
            ▼
       generate_stream_consumer(job_id)
            │
            ▼
       yield StartModuleResponse(output)
            │
            ▼
       Client receives streamed response
```

---

## IDs Flow

```
Client provides:
    ├─ mission_id
    ├─ setup_id
    └─ setup_version_id

Server generates:
    └─ job_id = uuid.uuid4()  (unique per request)

All IDs stored in:
    └─ ModuleContext.session
            │
            ├─ Used in structured logging
            ├─ Used in SurrealDB records
            └─ Returned in responses
```

---

## Key Files

| Component | File |
|-----------|------|
| Server | `src/digitalkin/grpc_servers/module_server.py` |
| Servicer | `src/digitalkin/grpc_servers/module_servicer.py` |
| Module Base | `src/digitalkin/modules/_base_module.py` |
| Job Manager | `src/digitalkin/core/job_manager/single_job_manager.py` |
| Task Manager | `src/digitalkin/core/task_manager/local_task_manager.py` |
| Task Session | `src/digitalkin/core/task_manager/task_session.py` |
| Task Executor | `src/digitalkin/core/task_manager/task_executor.py` |
| Factories | `src/digitalkin/core/common/factories.py` |
| Trigger Handler | `src/digitalkin/modules/trigger_handler.py` |
