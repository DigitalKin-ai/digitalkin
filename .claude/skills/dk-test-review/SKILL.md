---
name: dk-test-review
description: DigitalKin test-coverage and test-quality rubric. Use whenever auditing whether changed code has adequate tests, reviewing test files, or deciding if a diff is safe to merge from a testing standpoint. Covers pytest async-auto mode, component test layout, markers, manager/servicer/handler coverage, and streaming/error-path cases. Apply it proactively on any diff that adds or changes runtime behavior, even if the user only says "check the tests".
paths:
  - "tests/**/*.py"
  - "src/digitalkin/**/*.py"
user-invocable: true
---

# DigitalKin test rubric

Read-only audit. Output `UNCOVERED | file:symbol | what's missing`, or `COVERAGE OK`. No prose otherwise. Infer from the presence and shape of tests; never run the suite.

Layout, markers and the required-coverage matrix are in `.claude/rules/tests.md` — read it rather than assuming. It has 18 directories; an earlier version of this rubric listed 5 and caused false "correctly placed" verdicts.

## Judge a gap as UNCOVERED when
- A new `TriggerHandler.handle()` protocol value has no test.
- A servicer RPC has a success test but no test asserting the mapped `grpc.StatusCode` on each error path.
- A streaming change lacks one of: multiple messages, empty stream, mid-stream error or cancellation.
- A manager change lacks a state-transition, semaphore/waiting-pool, or signal-listener-cancellation test.
- A method was added to `Default*` or `Grpc*` without the counterpart being covered.
- The failure path has no test asserting cleanup happened.
- Newly introduced `task_id` / `setup_id` / `mission_id` propagation is unasserted.
- A Redis or signal test uses `_FakePubSub` with no paired integration test against the real Redis from the compose stack.
- A new test needing an external service carries no marker, so CI runs it in the unit leg.

## Weak-test flags
Instantiating without asserting behavior. An error path with no status-code assertion. A mock so broad the test would pass regardless of the change. An async test that constructs a coroutine but never awaits it.
