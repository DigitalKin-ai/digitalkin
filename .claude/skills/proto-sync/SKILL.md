---
name: proto-sync
description: Regenerate agentic-mesh-protocol from service-apis-py and install the rebuilt package into this venv, after a proto change.
argument-hint: "[version]"
disable-model-invocation: true
---

Sync this SDK against a proto change. The proto repo is `/home/gmx/Documents/Code/service-apis-py`; it owns generation, and this repo only consumes the built artifact.

Land the proto phase first and stop there. Never roll the SDK phase into the same pass without being asked — that is a standing rule for cross-repo work.

## Phase 1 — in service-apis-py

1. `task proto:breaking` before anything else. If it reports a break, stop and report it: an intentional break needs a package version bump (`vN` → `vN+1`), which is the user's call.
2. `task gen` — the full pipeline: submodule init, `buf generate`, rsync into `src/agentic_mesh_protocol/` and `src/buf/`, namespace init files, then build. Do not run the rsync steps by hand; they carry `--delete` and exclusion lists that are easy to get wrong.
3. Confirm a fresh artifact landed in `dist/`.

## Phase 2 — in dk-dev, only when asked

4. Copy the built artifact into `scripts/`.
5. Install it into this venv without disturbing anything else:
   `VIRTUAL_ENV=/home/gmx/Documents/Code/dk-dev/.venv uv pip install scripts/<artifact> --force-reinstall --no-deps`
6. Verify the import resolves and check the symbols the change touched.
7. `uv run mypy src/digitalkin` — regenerated stubs are the usual source of new type errors.

Report which RPCs, messages or fields changed shape, and every call site in `src/digitalkin/` that needs updating. Do not update them unless asked.
