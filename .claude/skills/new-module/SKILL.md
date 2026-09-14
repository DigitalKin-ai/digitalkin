---
name: new-module
description: Scaffold a new DigitalKin module — base class choice, the four generic models, trigger handlers, service config and the matching tests.
argument-hint: "[module-name]"
disable-model-invocation: true
---

Build a new module named `$ARGUMENTS`. Read an existing module under `examples/modules/` first and follow its shape rather than inventing one.

1. Subclass `ToolModule` for a utility, `ArchetypeModule` for an agent, or `BaseModule` when neither fits. All three live in `src/digitalkin/modules/`.
2. Define the four Pydantic models the generic parameters need, in this order: `[InputModelT, OutputModelT, SetupModelT, SecretModelT]`. No `Any` in any of the four.
3. On the `SetupModel`, mark initial-config fields `json_schema_extra={"config": True}` and runtime-only fields `{"hidden": True}`. Never strip fields by hand — `get_clean_model()` does the filtering.
4. Create one `TriggerHandler` subclass per input protocol, each with a distinct `protocol` class variable and an `async def handle()`. `ModuleDiscoverer` finds them; do not register them manually.
5. Stream every result through `context.callbacks.send_message()`. Never return output directly from a handler.
6. Set the `ServicesConfig` class attribute for the services the module actually uses. Do not enable a service the module never calls.
7. Propagate `task_id`, `setup_id` and `mission_id` into every downstream call.

Then write the tests, in `tests/modules/`:
- one test per new `protocol` value exercising the awaited path,
- an error-path test asserting the mapped `grpc.StatusCode`,
- a cleanup assertion on the failure path.

Tag anything needing a live service with the right pytest marker or CI will run it in the unit leg. `.claude/rules/tests.md` has the marker list.

Finish with `/dk-check`.
