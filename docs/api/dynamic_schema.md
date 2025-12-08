# Dynamic Schema

Mark fields as dynamic using `Annotated` metadata for runtime value refresh via sync or async fetcher functions.

## Problem

`json_schema_extra` values are evaluated at class definition time:

```python
# REGISTRY might be empty at import time!
class AgentSetup(SetupModel):
    model_name: str = Field(
        default="gpt-4",
        json_schema_extra={"enum": list(REGISTRY.model_names)},
    )
```

## Solution

Use `Annotated` with `Dynamic` metadata:

```python
from typing import Annotated
from digitalkin.utils import Dynamic

class AgentSetup(SetupModel):
    model_name: Annotated[str, Dynamic(enum=lambda: list(REGISTRY.model_names))] = Field(
        default="gpt-4"
    )
```

Call `get_clean_model(force=True)` during module initialization to resolve fetchers.

## Combining Static and Dynamic

Static `json_schema_extra` values and dynamic fetchers are cleanly separated:

```python
class AgentSetup(SetupModel):
    model_name: Annotated[str, Dynamic(enum=fetch_models)] = Field(
        default="gpt-4",
        json_schema_extra={
            "config": True,
            "ui:widget": "select",
        },
    )
```

When `force=True` is used, the resolved `enum` values are merged into `json_schema_extra`.

## API

### Classes

- `Dynamic(**fetchers)` - Alias for `DynamicField`, metadata class for `Annotated` fields with dynamic fetchers
- `DynamicField(**fetchers)` - Full class name (use `Dynamic` for cleaner code)
- `ResolveResult` - Structured result from `resolve_safe()` with `values`, `errors`, `success`, and `partial` properties

### Functions

- `get_dynamic_metadata(field_info)` - Extract `Dynamic` metadata from a FieldInfo
- `has_dynamic(field_info)` - Check if field has `Dynamic` metadata
- `get_fetchers(field_info)` - Extract fetcher callables from field
- `resolve(fetchers, *, timeout=None)` - Resolve fetchers to values in parallel (async, raises on error)
- `resolve_safe(fetchers, *, timeout=None)` - Resolve fetchers with structured error handling (async, returns `ResolveResult`)

### Types

- `Fetcher` - Type alias for fetcher callables: `Callable[[], T | Awaitable[T]]`
- `DEFAULT_TIMEOUT` - Default timeout for resolution (None = no timeout)

## Usage

The `force` parameter on `SetupModel.get_clean_model()` triggers fetcher resolution:

```python
# During module initialization
model = await MySetup.get_clean_model(
    config_fields=True,
    hidden_fields=False,
    force=True,  # Resolve dynamic fetchers
)

# Generated schema will contain resolved enum values
schema = model.model_json_schema()
```

## Fetchers

Fetchers are callables (sync or async) that take no arguments and return a value:

```python
# Sync fetcher
def get_models() -> list[str]:
    return ["gpt-4", "gpt-3.5", "claude"]

# Async fetcher
async def fetch_models() -> list[str]:
    response = await api.get_models()
    return [m.name for m in response]

# Usage
class Setup(SetupModel):
    model: Annotated[str, Dynamic(enum=get_models)] = Field(default="gpt-4")
    model_async: Annotated[str, Dynamic(enum=fetch_models)] = Field(default="gpt-4")
```

## Parallel Resolution

Multiple fetchers are resolved concurrently using `asyncio.gather()`:

```python
from digitalkin.utils import resolve

fetchers = {
    "models": fetch_models,      # async, takes 100ms
    "languages": fetch_languages,  # async, takes 50ms
    "defaults": get_defaults,    # sync
}

# All fetchers run in parallel - completes in ~100ms, not 150ms
resolved = await resolve(fetchers)
# resolved = {"models": [...], "languages": [...], "defaults": {...}}
```

## Timeout Support

Both `resolve()` and `resolve_safe()` support timeouts:

```python
from digitalkin.utils import resolve, resolve_safe

# Using resolve() - raises asyncio.TimeoutError on timeout
try:
    result = await resolve(fetchers, timeout=5.0)  # 5 second timeout
except asyncio.TimeoutError:
    print("Resolution timed out")

# Using resolve_safe() - captures timeout as error
result = await resolve_safe(fetchers, timeout=5.0)
if not result.success:
    for key, error in result.errors.items():
        if isinstance(error, asyncio.TimeoutError):
            print(f"{key} timed out")
```

## Error Handling

### Using `resolve()` (fail-fast)

Errors propagate immediately:

```python
from digitalkin.utils import resolve

try:
    resolved = await resolve(fetchers)
except ValueError as e:
    print(f"A fetcher failed: {e}")
```

### Using `resolve_safe()` (partial success)

Errors are captured in a structured result:

```python
from digitalkin.utils import resolve_safe, ResolveResult

result: ResolveResult = await resolve_safe(fetchers)

if result.success:
    # All fetchers succeeded
    print("Resolved:", result.values)

elif result.partial:
    # Some succeeded, some failed
    print("Partial success:", result.values)
    print("Errors:", result.errors)

else:
    # All failed
    print("All fetchers failed:", result.errors)

# Access individual values with defaults
models = result.get("models", ["gpt-4"])
```

`SetupModel.get_clean_model()` uses `resolve_safe()` internally, allowing partial success when some fetchers fail.

## Nested Model Support

Dynamic fields in nested `BaseModel` classes are also resolved when `force=True`:

```python
class NestedConfig(BaseModel):
    option: Annotated[str, Dynamic(enum=fetch_options)] = Field(default="a")

class AgentSetup(SetupModel):
    # Direct nested model
    config: NestedConfig = Field(default_factory=NestedConfig)

    # Optional nested model
    optional_config: NestedConfig | None = Field(default=None)

    # List of nested models
    items: list[NestedConfig] = Field(default_factory=list)

    # Dict with nested model values
    configs: dict[str, NestedConfig] = Field(default_factory=dict)

# All nested Dynamic fields are resolved
model = await AgentSetup.get_clean_model(
    config_fields=True,
    hidden_fields=True,
    force=True,
)
```

Supported container types:
- Direct `BaseModel` subclass
- `Optional[BaseModel]` / `BaseModel | None`
- `list[BaseModel]`
- `dict[str, BaseModel]`
- `set[BaseModel]`
- `tuple[BaseModel, ...]`
- `Union[..., BaseModel, ...]`

## Example Module

See `examples/modules/dynamic_setup_module.py` for a complete example demonstrating:
- Async fetchers simulating API calls
- Sync fetchers for static values
- Integration with the `DataModel`/`DataTrigger` pattern
- Using `get_clean_model(force=True)` for schema generation
