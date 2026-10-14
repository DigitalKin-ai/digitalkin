# Environment Variables

Every environment variable the SDK or a module reads is a field on a
[pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
class. Nothing calls `os.environ` or `os.getenv` directly: the settings class is
the single place a variable is declared, typed, defaulted and documented, and the
`.env` templates are generated from it.

There are two layers:

| Layer                                                  | Declared in                  | Read through                                       |
|--------------------------------------------------------|------------------------------|----------------------------------------------------|
| **SDK** — server, client, Redis, gateway, task manager | `digitalkin.models.settings` | the `get_*_settings()` singletons and `EnvManager` |
| **Module** — its own providers, tool module ids        | the module's `env.py`        | a subclass of `EnvManager`                         |

## What a module gets for free

`EnvManager` (`digitalkin.utils.env_manager`) turns the process environment into
the configuration every module needs, so a module no longer declares a
`ClientConfig`:

- **Services mode.** `EnvManager.services_mode()` reads the `-d/--dev-mode`
  launch flag first, then `SERVICE_MODE` (default `remote`).
- **Services-provider client.** `EnvManager.client_config()` builds one
  `ClientConfig` per process from the `CLIENT_*` variables. In remote mode,
  `ModuleServer` uses it to reach the registry, and `ServicesConfig` hands it to
  every remote strategy (`GrpcStorage`, `GrpcCost`, …) that was not given one.
- **Certificates.** In secure mode, credentials come from the explicit
  `CLIENT_CHANNEL_CREDENTIALS__*` / `SERVER_CHANNEL_CREDENTIALS__*` paths when set,
  otherwise from the certificate volumes:

  | Side | Volume | Files |
      |---|---|---|
  | client | `CERTIFICATE_SERVICES_PROVIDER_CERT_VOLUME` | `client.key`, `client.crt`, `ca.crt` |
  | server | `CERTIFICATE_CERT_VOLUME` | `server.key`, `server.crt`, `ca.crt` |

  The server's `ca.crt` is used only when `SERVER_CHANNEL_MTLS=true`: handing a
  root CA to gRPC turns on client authentication. A missing file raises
  `SecurityError` at startup, so secure mode fails closed.

The module server is therefore just:

```python
module_server = ModuleServer(MyModule)
```

and `services_config_params` only declares what is specific to the module:

```python
services_config_params: ClassVar[dict[str, dict[str, Any | None] | None]] = {
    "storage": {"config": {"chat_history": ChatHistory}},
    "cost": {"config": {"llm_input": CostConfig(...)}},
}
```

An explicit `"client_config"` entry still wins over the environment. Use one only
when a single service must reach a different host.

### SDK variable families

`.env.exemple` at the repository root lists every variable with its default and
description; `.env.minimum` lists the ones a deployment must set. Both are
generated from the classes below.

| Prefix                                                         | Settings class                                         | Configures                                                |
|----------------------------------------------------------------|--------------------------------------------------------|-----------------------------------------------------------|
| `SERVER_`                                                      | `ServerSettings`                                       | module gRPC server: concurrency, reflection, health check |
| `SERVER_CHANNEL_`                                              | `ServerChannelSettings`                                | listen address, security, advertised host                 |
| `SERVER_GRPC_`                                                 | `GrpcServerSettings`                                   | compression and `SERVER_GRPC_OPTIONS_*` channel options   |
| `CLIENT_`                                                      | `ClientSettings`                                       | unary query retries, backoff and deadline                 |
| `CLIENT_CHANNEL_`                                              | `ClientChannelSettings`                                | services-provider address and security                    |
| `CLIENT_GRPC_`                                                 | `GrpcClientSettings`                                   | compression and `CLIENT_GRPC_OPTIONS_*` channel options   |
| `CLIENT_GRPC_RETRY_`                                           | `GrpcClientRetrySettings`                              | channel-level retry policy                                |
| `CLIENT_CIRCUIT_BREAKER_`                                      | `CircuitBreakerSettings`                               | per-service circuit breaker                               |
| `CERTIFICATE_`                                                 | `CertificateSettings`                                  | certificate volumes                                       |
| `SERVICE_MODE`, `DIGITALKIN_MODULE_`                           | `ModuleSettings`                                       | services mode, module id, timezone, tool resolution       |
| `DIGITALKIN_MODULE_SERVICER_`                                  | `ModuleServicerSettings`                               | setup cache, completion timeout                           |
| `DIGITALKIN_REDIS_`                                            | `RedisSettings`, `RedisPoolSettings`                   | Redis URL, pools and TTLs                                 |
| `DIGITALKIN_SIGNAL_`                                           | `RedisSignalSettings`                                  | signal delivery                                           |
| `DIGITALKIN_GATEWAY_`                                          | `GatewaySettings` and its nested classes               | streams, dial-back, queues                                |
| `DIGITALKIN_M2M_`                                              | `GatewayM2MSettings`                                   | module-to-module calls                                    |
| `DIGITALKIN_TASK_MANAGER_`, `DIGITALKIN_JOB_MANAGER_`          | `TaskManagerSettings`, `JobManagerSettings`            | admission, concurrency, backpressure                      |
| `DIGITALKIN_BULKHEAD_`                                         | `BulkheadSettings`, `BulkheadServiceSettings`          | per-service concurrency limits                            |
| `DIGITALKIN_LOG_`, `DIGITALKIN_QUEUE_`, `DIGITALKIN_REGISTRY_` | `LoggingSettings`, `QueueSettings`, `RegistrySettings` | logging, queue sizes, registry search                     |
| `DIGITALKIN_`                                                  | `ProfilingSettings`                                    | profiler, uvloop                                          |

## Adding variables to a module

A module declares its own variables in `src/<package>/env.py`: one settings class
per concern, and a subclass of `EnvManager` that exposes them. The subclass
inherits `services_mode()`, `client_config()` and the certificate resolution, and
shares the SDK's cached `ClientConfig` instead of building a second one.

```python
"""Environment-driven configuration for the module."""

from typing import ClassVar

from digitalkin.utils.env_manager import EnvManager
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProxySettings(BaseSettings):
    """LLM proxy endpoint and bearer token."""

    model_config = SettingsConfigDict(env_prefix="LITELLM_", case_sensitive=False)

    base_url: str = Field(default="https://litellm.example.com/", description="LLM proxy base URL.")
    api_key: SecretStr = Field(
        default=SecretStr(""),
        description="Bearer token for the LLM proxy.",
        json_schema_extra={"env_required": True, "env_example": "sk-xxxx"},
    )


class MyModuleEnvManager(EnvManager):
    """SDK ``EnvManager`` extended with the module's own variables."""

    _proxy: ClassVar[ProxySettings | None] = None

    @classmethod
    def proxy(cls) -> ProxySettings:
        """Process-wide ``ProxySettings`` singleton.

        Returns:
            The shared ``ProxySettings`` instance.
        """
        if cls._proxy is None:
            cls._proxy = ProxySettings()
        return cls._proxy

    @classmethod
    def missing_required(cls) -> list[str]:
        """Required variables the environment leaves empty.

        Returns:
            The missing variable names, empty when the module can boot.
        """
        return cls.unset_required(cls.proxy())

    @classmethod
    def reset(cls) -> None:
        """Drop every cached setting. Tests call this after mutating env."""
        super().reset()
        cls._proxy = None
```

The archetype `ada` (`src/archetype_ada/env.py`) is a complete example.

### Reading a variable

Read through the manager at the point of use, never at import time:

```python
token = MyModuleEnvManager.proxy().api_key.get_secret_value()
```

Inside a pydantic `Field`, keep the lambda. Inlining the call would read the
environment once, when the class is defined:

```python
base_url: str = Field(default_factory=lambda: MyModuleEnvManager.proxy().base_url)
```

### Refusing to boot without the required variables

`unset_required()` reports every field marked `env_required` whose value is empty.
The same marker selects the variables of the minimum template, so the boot check
and `.env.minimum` cannot disagree. Check it in the entry point, before building
the server:

```python
missing = MyModuleEnvManager.missing_required()
if missing:
    msg = f"Required environment variable(s) not set: {', '.join(missing)}"
    raise OSError(msg)
module_server = ModuleServer(MyModule)
```

A secret that must be set defaults to empty and carries an `env_example`
placeholder. A non-empty default such as `"modules:xxxx"` would satisfy the check,
and the module would run with the placeholder.

### Testing

Settings are cached per process. Reset them around every test so a
`monkeypatch.setenv` in one test never leaks into the next:

```python
@pytest.fixture(autouse=True)
def _reset_env_manager():
    MyModuleEnvManager.reset()
    yield
    MyModuleEnvManager.reset()
```

## Field markers

The generator and `unset_required()` read three markers from `json_schema_extra`:

| Marker                       | Effect                                                                                                                                             |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `{"env_required": True}`     | listed in the minimum template; reported by `unset_required()` when empty                                                                          |
| `{"env_example": "sk-xxxx"}` | shown in the templates instead of the runtime default                                                                                              |
| `{"env_example": ""}`        | rendered commented out — for defaults computed on the running machine, such as `cpu_count() * 200`, which must not be frozen into a committed file |

A variable with no default and no marker is rendered commented out, so copying a
template never feeds an empty string to a typed field.

## Generating the templates

`digitalkin.utils.env_template` renders a `.env` template from every
`BaseSettings` class of the packages it is given:

```bash
python -m digitalkin.utils.env_template --package digitalkin.models.settings --output .env.exemple
python -m digitalkin.utils.env_template --package digitalkin.models.settings --minimum --output .env.minimum
```

`--check` compares instead of writing, and exits 1 when the file is out of date.
The SDK wires it three ways:

| Where                          | What                                                                                                                                 |
|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `task env-template`            | regenerates both files                                                                                                               |
| pre-commit hook `env-template` | regenerates **and stages** both files when a file under `models/settings/` or the generator changes, so they ship in the same commit |
| CI, lint job                   | `task env-template-check` fails the build when a template is stale                                                                   |

Never edit the generated files by hand. The next run overwrites the change, and
an unstaged manual edit conflicts with the hook's own rewrite: pre-commit then
rolls the whole commit back.

### In a module

Pass the SDK package and the module's `env.py`. Module templates include the SDK
variables, so one file documents the whole deployment:

```yaml
# taskfile.yaml
env-template:
  cmds:
    - uv run python -m digitalkin.utils.env_template --package digitalkin.models.settings --package my_module.env --output .env.example
    - uv run python -m digitalkin.utils.env_template --package digitalkin.models.settings --package my_module.env --minimum --output .env.minimum
```

```yaml
# .pre-commit-config.yaml
- repo: local
  hooks:
    - id: env-template
      name: env-template
      entry: bash -c 'uv run task env-template && git add -- .env.example .env.minimum'
      language: system
      pass_filenames: false
      files: ^src/my_module/env\.py$
```

The generator imports the module package, so the package must be installed in the
module's virtual environment:

- `pyproject.toml` needs a `[build-system]` table. Without it, uv treats the
  project as virtual, never installs it, and the generator fails with
  `ModuleNotFoundError`.
- When the dev tools live in an extra, sync with `uv sync --extra dev`. A plain
  `uv sync` removes every package that is not in the resolved default set,
  pre-commit and pytest included.

The hook only watches the module's `env.py`. A new SDK release can add variables
without triggering it, so run `task env-template-check` in the module's CI.

## Adding a variable to the SDK

### Does it belong in the SDK?

Add it to the SDK when it configures SDK machinery that every module runs:
the server, the gRPC clients, Redis, the gateway, the task and job managers,
logging. Keep it in the module's `env.py` when only one module reads it, or when
it configures a provider the module calls (an LLM proxy, a vector store, a
tracing backend).

### Steps

1. **Pick the settings class** for the component, from the
   [family table](#sdk-variable-families). Add a new class in
   `src/digitalkin/models/settings/` only for a new component, with an
   `env_prefix` following the existing families.
2. **Declare the field** with a keyword default and a description — the
   description becomes the template comment:

    ```python
    stream_batch_size: int = Field(default=50, gt=0, description="Entries per stream read.")
    ```

   Always write `Field(default=...)`. With a positional default, the pydantic
   mypy plugin treats the field as required in the synthesized `__init__`.

3. **Expose a new class** through a cached getter, or compose it into its parent
   settings class (as `ServerSettings.channel` does):

    ```python
    @lru_cache(maxsize=1)
    def get_stream_settings() -> StreamSettings:
        return StreamSettings()
    ```

   Register the getter in `_SETTINGS_FACTORIES` in `tests/conftest.py`, so its
   cache is cleared before each test.

4. **Read it at the point of use** — `get_stream_settings().stream_batch_size` —
   never in a module-level constant.
5. **Regenerate the templates** with `task env-template`. The pre-commit hook
   does it on commit if you forget.

### Special cases

| Situation                       | Pattern                                                             | Example                                                                  |
|---------------------------------|---------------------------------------------------------------------|--------------------------------------------------------------------------|
| unprefixed or historical name   | `validation_alias="NAME"`                                           | `ModuleSettings.services_mode` → `SERVICE_MODE`                          |
| name built at runtime           | pass `_env_prefix` from `__init__`; the generator skips such fields | `BulkheadServiceSettings("storage")` → `DIGITALKIN_BULKHEAD_STORAGE_MAX` |
| secret                          | `SecretStr`, rendered in clear only in the templates                | `RedisPoolSettings.url`                                                  |
| default computed on the machine | `json_schema_extra={"env_example": ""}`                             | `ServerSettings.max_concurrent_rpcs`                                     |
| nested model                    | `env_nested_delimiter="__"` on the class                            | `SERVER_CHANNEL_CREDENTIALS__KEY_PATH`                                   |
| gRPC channel option             | field with an `alias`, added to the `options` property              | `SERVER_GRPC_OPTIONS_KEEPALIVE_TIME`                                     |

Renaming or removing a variable breaks every deployment that sets it: commit it as
`feat!:` or `fix!:`.

## Pitfalls

- **Unknown variables are ignored silently.** pydantic-settings only reads declared
  fields: a misspelt variable changes nothing and raises nothing. The generated
  template is the reference for exact names.
- **Declare only what your code reads.** A declared field that nothing consumes
  is dead configuration: it documents a knob that does nothing.
- **Settings are cached.** A test that changes the environment must clear the
  cache — the conftest fixture and `EnvManager.reset()` do it.
