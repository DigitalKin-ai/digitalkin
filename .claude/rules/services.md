---
paths:
  - "src/digitalkin/services/**/*.py"
  - "src/digitalkin/models/settings/**/*.py"
---

# Service strategy and settings rules

## Strategies
Each service has a local (`Default*`) and a remote (`Grpc*`) implementation, selected through `ServicesConfig` and injected via `ModuleContext`. Adding a method to one implementation means adding it to both.

## Settings
- New `pydantic-settings` classes use a scoped `env_prefix` of `DIGITALKIN_<SCOPE>_`. Never a bare `DIGITALKIN_`.
- Settings are consumed through an `@lru_cache`-decorated `get_*_settings()` factory. No per-field copies on consumers, no override arguments, no conditional construction.

## Imports
Every `from X import Y` is hoisted to module top. Do not use a function-local import to dodge a cycle or a hot-path cost — a cycle means the layering is wrong.

## Deletion bias
This codebase is over-built. When a feature is half-built or unwired, default to deleting it rather than finishing it. Remove dead paths as you find them; crash on a missing required dependency instead of adding a silent fallback.
