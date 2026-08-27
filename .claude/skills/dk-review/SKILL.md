---
name: dk-review
description: DigitalKin code review checklist enforcing the project's MANDATORY rules (minimal footprint, no standalone functions, no global vars, no hasattr/getattr/setattr, Google docstrings, enum discipline, structured-logging extra rules, gRPC error mapping, async-first, resource cleanup, ID propagation). Use this whenever reviewing, auditing, or critiquing any Python diff or file in this repo — servicers, modules, trigger handlers, managers, services — even if the user just says "review this" or "is this clean". Always apply it before approving DigitalKin code changes.
paths:
  - "src/digitalkin/**/*.py"
user-invocable: true
---

# DigitalKin review rubric

Apply to the diff only. Output each finding as `SEV | file:line | rule | fix`, SEV ∈ BLOCK / WARN / NIT. No praise, no restating unchanged code. End with `APPROVE` / `APPROVE-WITH-NITS` / `REQUEST-CHANGES`.

The full rule text lives in `.claude/rules/`, which loads automatically when you read a matching file. This rubric is the severity mapping over those rules.

## BLOCK
- `hasattr` / `getattr` / `setattr` anywhere. The type design is wrong.
- Module-level function or constant. Every function is a method; inline a default only at a single use site alongside its env var.
- `logger.x("msg", extra={"local_var": ...})`. `extra=` carries only `task_id`/`setup_id`/`mission_id`.
- Open-without-close: a connection, task, queue or channel with no `finally` or context manager.
- Servicer error path surfacing a bare exception as `UNKNOWN`, or using `INTERNAL` as a catch-all.
- Blocking I/O on the event loop inside an async path.
- Streaming that bypasses `context.callbacks.send_message()`, or buffers unboundedly before yield.

## WARN
- Single-use private `_method` that should be inlined; wrapper for one operation; over-abstraction.
- Intermediate variable used once that could be inlined.
- Missing type annotation on new code.
- `task_id` / `setup_id` / `mission_id` in scope but not propagated.
- Parallel mapping dict beside an enum.
- Hand-rolled validation where Pydantic applies; manual `SetupModel` field stripping instead of `get_clean_model()`.
- Function-local import used to dodge a cycle or hot-path cost.
- Wrong or missing conventional-commit prefix on a reviewed commit or PR title.
- `insecure` server mode outside local and test.

## NIT
- Docstring missing `Args`/`Returns`/`Raises`/`Yields` where applicable, or padded with prose, numbered steps or restated signatures.
- `#` comment volume above what the surrounding code carries.

## Do not flag
`RUF105` and `RUF201` diagnostics — 209 pre-existing repo-wide, from the ruff 0.16 rename of rule codes to names. Not caused by any change under review.

Formatting, import order and type errors are the linter's job, not yours. `.claude/hooks/gate-quality.sh` already blocks the turn on those; do not duplicate them as findings.

The verdict line is required.
