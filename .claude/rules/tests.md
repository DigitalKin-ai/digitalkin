---
paths:
  - "tests/**/*.py"
---

# Test rules

`asyncio_mode = "auto"`, so async tests need no marker. Verify the async path is actually awaited and asserted, not just constructed.

## Layout
Tests mirror the component they cover. New code lands in the matching directory.

`core/` task and job managers · `grpc_server/` servers and servicers · `gateway/` gateway servicer and streams · `modules/` modules and trigger handlers · `services/` service strategies · `mixins/` · `community/` · `utils/`

Cross-cutting suites: `integration/` (real external services) · `advanced/` · `benchmarks/` · `performances/` · `canary/` · `chaos/` (fault injection) · `stability/` (soak) · `observability/`

Shared machinery: `fixtures/` · `mocks/`

## Markers
Declared in `pyproject.toml` and selected by the CI matrix. Tag new tests with the narrowest applicable marker: `smoke`, `grpc`, `validation`, `edge_case`, `regression`, `integration`, plus `chaos`, `concurrency`, `contract`, `e2e`, `idempotency`, `property`, `stability`, `stress`, `unit`, `flaky`.

CI runs unit as the complement: `not integration and not grpc and not smoke and not validation and not edge_case and not regression`. An untagged test therefore runs in the unit leg — tag anything that needs a service.

## Required coverage
- Each new `TriggerHandler.handle()` `protocol` value: at least one test.
- Servicer RPCs: success path plus each error path, asserting the mapped `grpc.StatusCode`.
- Streaming: multiple messages, empty stream, and a mid-stream error or cancellation.
- Manager lifecycle: state transitions, semaphore and waiting-pool limits, signal-listener cancellation.
- Service strategies: both `Default*` and `Grpc*` when a method is added to either.
- Cleanup asserted on the failure path.
- `task_id` / `setup_id` / `mission_id` propagation where newly introduced.

## Redis and signals
Every test using `_FakePubSub` needs a paired integration test against the real Redis from the test compose stack. A fake-only test does not count as coverage for Redis behavior.

## Weak tests
Instantiating without asserting behavior; an error path with no status-code assertion; a mock so broad the test would pass regardless of the change.
