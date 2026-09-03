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

## Commits

Conventional commits, driving semantic versioning: `feat:` minor, `fix:` patch, `feat!:` / `fix!:` major, plus `docs:` `refactor:` `test:` `chore:`.
