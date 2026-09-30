---
paths:
  - "src/digitalkin/**/*.py"
---

# DigitalKin Python rules

## Prohibited
- `hasattr()` / `getattr()` / `setattr()`. Their presence means the type design is wrong. Use `is None` / `is not None` or a correct annotation. Blocked by `.claude/hooks/guard-edit.sh`.
- Module-level functions. Every function is a method on a class.
- Module-level constants or variables. Inline a default at its single use site alongside the env var: `os.environ.get("VAR", "default")`.
- `ClassVar` for a value used once.
- Private `_method` that is not reused within the class. Inline it.

## Typing and models
- Annotate every parameter and return on new code.
- Data models are Pydantic. Do not hand-roll validation or serialization.
- Modules carry all four type params `[InputModelT, OutputModelT, SetupModelT, SecretModelT]`. No `Any` leaking through them.
- `SetupModel` field visibility goes through `json_schema_extra` (`{"config": True}`, `{"hidden": True}`); filter with `get_clean_model()`, never by stripping fields manually.

## Enums
Enums stay enums, with no parallel mapping dict. By name `MyEnum[name]`, by value `MyEnum(value)`, compare `status == MyEnum.VALUE`, raw via `.value` / `.name`.

## Async
Handlers and module methods are `async def`. No blocking I/O on the event loop: no sync DB driver, no `requests`, no `time.sleep`.

## Resource cleanup
Connections, tasks, queues and channels close in `finally` or via a context manager. Generators release in `finally`. An open-without-close path is a defect.

## ID propagation
Propagate `task_id`, `setup_id` and `mission_id` wherever they are in scope — constructors, downstream calls, sessions.

## Structured logging
`extra=` carries only global correlation IDs, normally `self.session_ids` or `context.session.current_ids()`.

Local values go in the message as `%`-args. If no global IDs are in scope, omit `extra` entirely.

```python
logger.info("Task started (attempt %d/%d)", attempt, max_retries, extra=self.session_ids)
logger.error("Connection failed: %s", error_msg, extra=self.session_ids)
```

Never `logger.info("Task started", extra={"attempt": attempt})`.

## Docstrings
Google style, lean. `Args` / `Returns` / `Raises` / `Yields` when applicable. No flowery prose, no numbered steps, no restating the signature. Minimise `#` comments; long context belongs in `docs/`.
