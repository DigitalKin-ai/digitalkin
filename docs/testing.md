# Testing

## Commands

| Command | What it runs |
|---|---|
| `task run-tests` | `docker compose run --rm -T tests`: the full suite in the test image against the `tests-redis` container. |
| `task linter` | `ruff format`, import sort, `ruff check --fix`, `mypy src/digitalkin`. |
| `task check` | `linter`, `mypy`, then `run-tests`. |
| `task env-template-check` | Fails when `.env.exemple` / `.env.minimum` no longer match the settings classes. |
| `uv run pytest tests/path/test_x.py::test_name` / `-m smoke` | One test or one marker on the host. |

The `tests` service forwards `TEST_SELECTOR` (default `tests/`), `TEST_MARKER` (passed to `-m`) and `PYTEST_ARGS`. Exit code 5 (no test matched the marker) is treated as success. The image installs exactly what `uv.lock` pins; `PYTHON_VERSION` selects the interpreter (default `3.10`).

```bash
TEST_MARKER=integration TEST_SELECTOR=tests/integration/ docker compose run --rm -T tests
```

## Redis for tests

| Item | Value |
|---|---|
| Service | `tests-redis` (`redis:7-alpine`, no persistence, `maxmemory 256mb`) |
| Host port | First free port in `6399-6409` (override with `REDIS_PORT`), so two checkouts can run side by side. No fixed container names. |
| Find the port | `docker compose port tests-redis 6379` |
| URL inside compose | `redis://tests-redis:6379/0` (set on the `tests` service) |
| URL on the host | `DIGITALKIN_REDIS_URL`; the integration fixture defaults to `redis://localhost:6399/0` |
| Unreachable Redis | Integration tests skip. With `DIGITALKIN_REQUIRE_REDIS` set they fail instead. CI sets it on the integration leg. |

Running integration tests on the host:

```bash
docker compose up -d tests-redis
export DIGITALKIN_REDIS_URL="redis://localhost:$(docker compose port tests-redis 6379 | cut -d: -f2)/0"
DIGITALKIN_REQUIRE_REDIS=1 uv run pytest -m integration
```

Chaos tests need the `chaos` profile, which adds Toxiproxy (API on `8474`, proxied Redis on `26379`); without it they skip.

```bash
docker compose --profile chaos up -d
uv run pytest tests/chaos -m chaos
```

## Markers

Declared in `pyproject.toml`. Tag a new test with the narrowest one that applies.

| Marker | Use |
|---|---|
| `smoke` | Critical path |
| `grpc` | gRPC service functionality |
| `validation` | Input and schema validation |
| `edge_case` | Boundary conditions |
| `regression` | Previously fixed bugs |
| `integration` | Needs a real external service (Redis) |
| `chaos` | Fault injection (Toxiproxy) |
| `concurrency`, `idempotency` | Races; retry and duplicate handling |
| `contract`, `e2e`, `property` | Proto contract checks; full compose stack; Hypothesis |
| `stability`, `stress` | Soak; load |
| `unit` | Plain unit test with no other category |
| `flaky` | Known-flaky tests |

The CI unit leg is the complement `not integration and not grpc and not smoke and not validation and not edge_case and not regression`, so an untagged test runs there. Tag anything that needs a service.

`stress` and `stability` tests are not deselected by default: they run in the unit leg. Exclude them locally with `-m "not stress and not stability"`. Files under `tests/benchmarks/` are named `bench_*.py` and are not collected (`python_files = "test_*.py"`); run them directly.

## Configuration

| Setting | Value |
|---|---|
| `asyncio_mode` | `auto`: async tests need no marker. |
| Default timeout | `10` s (`timeout_method = "signal"`); slow modules set `pytest.mark.timeout(...)` in `pytestmark`. |
| Settings caches | `tests/conftest.py` lists every `@lru_cache get_*_settings()` factory in `_SETTINGS_FACTORIES` and an autouse fixture clears them (and resets `EnvManager`) before each test, so `monkeypatch.setenv` does not leak. Add a new settings factory to that tuple. |

## Layout

Tests mirror the component they cover (`.claude/rules/tests.md`).

| Directory | Covers |
|---|---|
| `core/` | Task and job managers, Redis client, signals |
| `grpc_server/` | Servers and servicers |
| `gateway/` | Gateway servicer, streams, dial-back |
| `modules/` | Modules and trigger handlers |
| `services/` | Service strategies (`Default*` and `Grpc*`) |
| `mixins/`, `community/`, `utils/` | Mixins, community adapters (agno), utilities |
| `integration/` | Real Redis (`integration/redis/`) |
| `advanced/`, `canary/`, `observability/` | Cross-cutting suites |
| `chaos/`, `stability/`, `performances/`, `benchmarks/` | Fault injection, soak, performance, benchmarks |
| `fixtures/`, `mocks/` | Shared machinery |

## Rules

- Every test that uses `_FakePubSub` (or fakeredis for Redis behavior) needs a paired integration test against the real `tests-redis`. A fake-only test is not coverage for Redis behavior.
- Servicer RPCs: success path plus each error path, asserting the mapped `grpc.StatusCode`.
- Streaming: several messages, empty stream, and a mid-stream error or cancellation.
- Assert cleanup on the failure path.

## CI

`.github/workflows/ci.yml`, on push and pull request to `main` and `dev`.

| Job | Python | Runs |
|---|---|---|
| `lint` | 3.10 | `ruff check`, `ruff format --check`, `task env-template-check`, package build |
| `test-full` (matrix) | 3.12 | `docker compose up --exit-code-from tests`, one leg per marker: Unit (complement above), Smoke, gRPC, Validation, Edge Case, Regression, Integration (`DIGITALKIN_REQUIRE_REDIS=1`) |
| `test-compat` (matrix) | 3.10, 3.11, 3.13 | Smoke, Validation, Regression |
| `test-summary` | — | Aggregates the JUnit reports of both matrices |
| `security-scan` | — | Trivy, TruffleHog |
