"""General utils folder."""

from digitalkin.utils.dynamic_schema import (
    DEFAULT_TIMEOUT,
    DynamicField,
    Fetcher,
    ResolveResult,
    get_dynamic_metadata,
    get_fetchers,
    has_dynamic,
    resolve,
    resolve_safe,
)

# Alias for cleaner API: `Dynamic` is shorter than `DynamicField`
Dynamic = DynamicField

__all__ = [
    "DEFAULT_TIMEOUT",
    "Dynamic",
    "DynamicField",
    "Fetcher",
    "ResolveResult",
    "get_dynamic_metadata",
    "get_fetchers",
    "has_dynamic",
    "resolve",
    "resolve_safe",
]
