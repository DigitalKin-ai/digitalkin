---
name: dk-check
description: Run the full DigitalKin review sweep on the current change — lint and types, code rules, test coverage, and proto wire compatibility — and return one merged verdict.
argument-hint: "[base-ref or paths]"
disable-model-invocation: true
allowed-tools: Agent, Read, Glob
---

Run the review agents concurrently in a single message, passing `$ARGUMENTS` through as scope to each. Do not review the diff yourself — the whole point is that a fresh context grades work the implementing context produced.

Always launch:
- `quality-gate` — real lint and type tooling, non-mutating
- `code-reviewer` — the `dk-review` rubric
- `test-auditor` — the `dk-test-review` rubric

Launch additionally only if the change touches `*.proto`, `*_pb2*.py`, or anything under `src/digitalkin/grpc_servers/`:
- `grpc-breaking`
- `grpc-test-coverage`

Check that condition with Glob before spawning, so a pure-Python change does not pay for two idle agents.

Then merge into one report:

1. `GATE PASS` / `GATE FAIL` from quality-gate, plus any failing `file:line`.
2. `BREAKING` findings, if grpc-breaking ran. These outrank everything else — a wire break needs a package version bump.
3. `BLOCK` findings from code-reviewer.
4. `UNCOVERED` findings from both coverage agents, deduplicated by `file:symbol`.
5. `WARN` and `NIT`, collapsed to counts unless there are fewer than five.

Close with a single line: `MERGE` if there is no GATE FAIL, no BREAKING and no BLOCK; `HOLD` otherwise, naming the single most important reason.

A reviewer asked to find gaps will find some. Report only what affects correctness or a stated requirement — drop speculative findings and style preferences the linter already owns.
