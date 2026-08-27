---
name: run-digitalkin
description: How to build, launch and exercise this project. Used by /run and /verify so they do not have to guess the launch from the README.
---

This project does not launch from a plain `python -m`. Tests and servers both need Redis, which comes from the compose stack.

## Test suite
`task run-tests` — `docker compose run --rm -T tests`. The `tests` service builds `docker/Dockerfile.test`, mounts the repo at `/app`, and waits on the `tests-redis` healthcheck.

Select a subset with environment variables, not CLI arguments — the entrypoint ignores arguments after the service name:
- `TEST_SELECTOR` a path, default `tests/`
- `TEST_MARKER` a pytest marker expression, e.g. `not integration`
- `PYTEST_ARGS` extra flags
- `DIGITALKIN_REQUIRE_REDIS=1` makes integration tests hard-fail instead of skipping when Redis is unreachable

Chaos tests need the extra profile: `docker compose --profile chaos up -d tests-toxiproxy` (Toxiproxy on 8474, proxied Redis on 26379).

## Outside the container
`uv run pytest` works for unit tests only. Integration tests skip or fail without Redis. To run them on the host, start Redis alone and point the SDK at the mapped port:

```bash
docker compose up -d tests-redis     # host port 6399 -> container 6379
DIGITALKIN_REDIS_URL=redis://localhost:6399/0 uv run pytest -m integration
```

Inside the compose network the URL is `redis://tests-redis:6379/0` instead — the host port mapping is not visible from a container.

## Servers
`examples/start_grpc_server_module.py` and `examples/start_grpc_server_registry.py` start a module and a registry server; `examples/start_grpc_client.py` drives them. Install the examples group first: `task examples-deps`.

## Teardown
`docker compose down --volumes --remove-orphans`.
