"""Dynamic schema utilities for runtime value refresh in Pydantic models.

This module provides a clean way to mark fields as dynamic using Annotated metadata,
allowing their schema values to be refreshed at runtime via sync or async fetchers.

Example:
    from typing import Annotated
    from digitalkin.utils import DynamicField

    class AgentSetup(SetupModel):
        model_name: Annotated[str, DynamicField(enum=fetch_models)] = Field(default="gpt-4")

See Also:
    - Documentation: docs/api/dynamic_schema.md
    - Tests: tests/utils/test_dynamic_schema.py
"""

from __future__ import annotations

import asyncio
import time
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from itertools import starmap
from typing import TYPE_CHECKING, Any, TypeVar

from digitalkin.logger import logger

if TYPE_CHECKING:
    from pydantic.fields import FieldInfo

T = TypeVar("T")

# Fetcher callable type: sync or async function with no arguments
Fetcher = Callable[[], T | Awaitable[T]]

# Default timeout for fetcher resolution (None = no timeout)
DEFAULT_TIMEOUT: float | None = None


@dataclass
class ResolveResult:
    """Result of resolving dynamic fetchers.

    Provides structured access to resolved values and any errors that occurred.
    This allows callers to handle partial failures gracefully.

    Attributes:
        values: Dict mapping key names to successfully resolved values.
        errors: Dict mapping key names to exceptions that occurred during resolution.
    """

    values: dict[str, Any] = field(default_factory=dict)
    errors: dict[str, Exception] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        """Check if all fetchers resolved successfully.

        Returns:
            True if no errors occurred, False otherwise.
        """
        return len(self.errors) == 0

    @property
    def partial(self) -> bool:
        """Check if some but not all fetchers succeeded.

        Returns:
            True if there are both values and errors, False otherwise.
        """
        return len(self.values) > 0 and len(self.errors) > 0

    def get(self, key: str, default: T | None = None) -> T | None:
        """Get a resolved value by key.

        Args:
            key: The fetcher key name.
            default: Default value if key not found or errored.

        Returns:
            The resolved value or default.
        """
        return self.values.get(key, default)  # type: ignore[return-value]


class DynamicField:
    """Metadata class for Annotated fields with dynamic fetchers.

    Use with typing.Annotated to mark fields that need runtime value resolution.
    Fetchers are callables (sync or async) that return values at runtime.

    Args:
        **fetchers: Mapping of key names to fetcher callables.
            Each fetcher is a function (sync or async) that takes no arguments
            and returns the value for that key (e.g., enum values, defaults).

    Example:
        from typing import Annotated

        async def fetch_models() -> list[str]:
            return await api.get_models()

        class Setup(SetupModel):
            model: Annotated[str, DynamicField(enum=fetch_models)] = Field(default="gpt-4")
    """

    __slots__ = ("fetchers",)

    def __init__(self, **fetchers: Fetcher[Any]) -> None:
        """Initialize with fetcher callables."""
        self.fetchers: dict[str, Fetcher[Any]] = fetchers

    def __repr__(self) -> str:
        """Return string representation."""
        keys = ", ".join(self.fetchers.keys())
        return f"DynamicField({keys})"

    def __eq__(self, other: object) -> bool:
        """Check equality based on fetchers.

        Returns:
            True if fetchers are equal, NotImplemented for non-DynamicField types.
        """
        if not isinstance(other, DynamicField):
            return NotImplemented
        return self.fetchers == other.fetchers

    def __hash__(self) -> int:
        """Hash based on fetcher keys (fetchers themselves aren't hashable).

        Returns:
            Hash value based on sorted fetcher keys.
        """
        return hash(tuple(sorted(self.fetchers.keys())))


def get_dynamic_metadata(field_info: FieldInfo) -> DynamicField | None:
    """Extract DynamicField metadata from a FieldInfo's metadata list.

    Args:
        field_info: The Pydantic FieldInfo object to inspect.

    Returns:
        The DynamicField metadata instance if found, None otherwise.
    """
    for meta in field_info.metadata:
        if isinstance(meta, DynamicField):
            return meta
    return None


def has_dynamic(field_info: FieldInfo) -> bool:
    """Check if a field has DynamicField metadata.

    Args:
        field_info: The Pydantic FieldInfo object to check.

    Returns:
        True if the field has DynamicField metadata, False otherwise.
    """
    return get_dynamic_metadata(field_info) is not None


def get_fetchers(field_info: FieldInfo) -> dict[str, Fetcher[Any]]:
    """Extract fetchers from a field's DynamicField metadata.

    Args:
        field_info: The Pydantic FieldInfo object to extract from.

    Returns:
        Dict mapping key names to fetcher callables, empty if no DynamicField metadata.
    """
    meta = get_dynamic_metadata(field_info)
    if meta is None:
        return {}
    return meta.fetchers


def _get_fetcher_info(fetcher: Fetcher[Any]) -> str:
    """Get descriptive info about a fetcher for logging.

    Args:
        fetcher: The fetcher callable.

    Returns:
        A string describing the fetcher (module.name or repr).
    """
    if hasattr(fetcher, "__module__") and hasattr(fetcher, "__qualname__"):
        return f"{fetcher.__module__}.{fetcher.__qualname__}"
    if hasattr(fetcher, "__name__"):
        return fetcher.__name__
    return repr(fetcher)


async def _resolve_one(key: str, fetcher: Fetcher[Any]) -> tuple[str, Any]:
    """Resolve a single fetcher.

    Args:
        key: The fetcher key name.
        fetcher: The fetcher callable.

    Returns:
        Tuple of (key, resolved_value).

    Raises:
        Exception: If the fetcher raises an exception.
    """
    fetcher_info = _get_fetcher_info(fetcher)
    logger.debug(
        "Resolving fetcher '%s' using %s",
        key,
        fetcher_info,
        extra={"fetcher_key": key, "fetcher": fetcher_info},
    )

    start_time = time.perf_counter()

    try:
        result = fetcher()
        is_async = asyncio.iscoroutine(result)

        if is_async:
            logger.debug(
                "Fetcher '%s' returned coroutine, awaiting...",
                key,
                extra={"fetcher_key": key, "is_async": True},
            )
            result = await result

    except Exception as e:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.error(
            "Fetcher '%s' (%s) failed after %.2fms: %s: %s",
            key,
            fetcher_info,
            elapsed_ms,
            type(e).__name__,
            str(e) or "(no message)",
            extra={
                "fetcher_key": key,
                "fetcher": fetcher_info,
                "elapsed_ms": elapsed_ms,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
            },
        )
        raise

    elapsed_ms = (time.perf_counter() - start_time) * 1000

    logger.debug(
        "Fetcher '%s' resolved successfully in %.2fms, result type: %s",
        key,
        elapsed_ms,
        type(result).__name__,
        extra={
            "fetcher_key": key,
            "elapsed_ms": elapsed_ms,
            "result_type": type(result).__name__,
        },
    )

    return key, result


async def resolve(
    fetchers: dict[str, Fetcher[Any]],
    *,
    timeout: float | None = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Resolve all dynamic fetchers to their actual values in parallel.

    Fetchers are executed concurrently using asyncio.gather() for better
    performance when multiple async fetchers are involved.

    Args:
        fetchers: Dict mapping key names to fetcher callables.
        timeout: Optional timeout in seconds for all fetchers combined.
            If None (default), no timeout is applied.

    Returns:
        Dict mapping key names to resolved values.

    Raises:
        asyncio.TimeoutError: If timeout is exceeded.
        Exception: If any fetcher raises an exception, it is propagated.

    Example:
        fetchers = {"enum": fetch_models, "default": get_default}
        resolved = await resolve(fetchers, timeout=5.0)
        # resolved = {"enum": ["gpt-4", "gpt-3.5"], "default": "gpt-4"}
    """
    if not fetchers:
        logger.debug("resolve() called with empty fetchers, returning {}")
        return {}

    fetcher_keys = list(fetchers.keys())
    fetcher_infos = {k: _get_fetcher_info(f) for k, f in fetchers.items()}

    logger.info(
        "resolve() starting parallel resolution of %d fetcher(s): %s",
        len(fetchers),
        fetcher_keys,
        extra={
            "fetcher_count": len(fetchers),
            "fetcher_keys": fetcher_keys,
            "fetcher_infos": fetcher_infos,
            "timeout": timeout,
        },
    )

    start_time = time.perf_counter()

    # Create tasks for parallel execution
    tasks = list(starmap(_resolve_one, fetchers.items()))

    # Execute with optional timeout
    try:
        if timeout is not None:
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        else:
            results = await asyncio.gather(*tasks)
    except asyncio.TimeoutError:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.error(
            "resolve() timed out after %.2fms (timeout=%.2fs)",
            elapsed_ms,
            timeout,
            extra={"elapsed_ms": elapsed_ms, "timeout": timeout},
        )
        raise

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    logger.info(
        "resolve() completed successfully in %.2fms, resolved %d fetcher(s)",
        elapsed_ms,
        len(results),
        extra={"elapsed_ms": elapsed_ms, "resolved_count": len(results)},
    )

    return dict(results)


async def resolve_safe(
    fetchers: dict[str, Fetcher[Any]],
    *,
    timeout: float | None = DEFAULT_TIMEOUT,
) -> ResolveResult:
    """Resolve fetchers with structured error handling.

    Unlike `resolve()`, this function catches individual fetcher errors
    and returns them in a structured result, allowing partial success.

    Args:
        fetchers: Dict mapping key names to fetcher callables.
        timeout: Optional timeout in seconds for all fetchers combined.
            If None (default), no timeout is applied. Note: timeout applies
            to the entire operation, not individual fetchers.

    Returns:
        ResolveResult with values and any errors that occurred.

    Example:
        result = await resolve_safe(fetchers, timeout=5.0)
        if result.success:
            print("All resolved:", result.values)
        elif result.partial:
            print("Partial success:", result.values)
            print("Errors:", result.errors)
        else:
            print("All failed:", result.errors)
    """
    if not fetchers:
        logger.debug("resolve_safe() called with empty fetchers, returning empty ResolveResult")
        return ResolveResult()

    fetcher_keys = list(fetchers.keys())
    fetcher_infos = {k: _get_fetcher_info(f) for k, f in fetchers.items()}

    logger.info(
        "resolve_safe() starting parallel resolution of %d fetcher(s): %s",
        len(fetchers),
        fetcher_keys,
        extra={
            "fetcher_count": len(fetchers),
            "fetcher_keys": fetcher_keys,
            "fetcher_infos": fetcher_infos,
            "timeout": timeout,
        },
    )

    start_time = time.perf_counter()
    result = ResolveResult()

    async def safe_resolve_one(key: str, fetcher: Fetcher[Any]) -> None:
        """Resolve one fetcher, capturing errors."""
        try:
            _, value = await _resolve_one(key, fetcher)
            result.values[key] = value
        except Exception as e:
            # Error already logged in _resolve_one, just capture it
            result.errors[key] = e

    # Create tasks for parallel execution
    tasks = list(starmap(safe_resolve_one, fetchers.items()))

    try:
        if timeout is not None:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        else:
            await asyncio.gather(*tasks)
    except asyncio.TimeoutError as e:
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        # Add timeout error for any keys that didn't complete
        resolved_keys = set(result.values.keys()) | set(result.errors.keys())
        timed_out_keys = [key for key in fetchers if key not in resolved_keys]
        for key in timed_out_keys:
            result.errors[key] = e

        logger.error(
            "resolve_safe() timed out after %.2fms (timeout=%.2fs), %d succeeded, %d failed, %d timed out",
            elapsed_ms,
            timeout,
            len(result.values),
            len(result.errors) - len(timed_out_keys),
            len(timed_out_keys),
            extra={
                "elapsed_ms": elapsed_ms,
                "timeout": timeout,
                "succeeded_keys": list(result.values.keys()),
                "failed_keys": [k for k in result.errors if k not in timed_out_keys],
                "timed_out_keys": timed_out_keys,
            },
        )

    elapsed_ms = (time.perf_counter() - start_time) * 1000

    # Log summary
    if result.success:
        logger.info(
            "resolve_safe() completed successfully in %.2fms, all %d fetcher(s) resolved",
            elapsed_ms,
            len(result.values),
            extra={
                "elapsed_ms": elapsed_ms,
                "success": True,
                "resolved_count": len(result.values),
            },
        )
    elif result.partial:
        logger.warning(
            "resolve_safe() completed with partial success in %.2fms: %d succeeded, %d failed",
            elapsed_ms,
            len(result.values),
            len(result.errors),
            extra={
                "elapsed_ms": elapsed_ms,
                "success": False,
                "partial": True,
                "resolved_count": len(result.values),
                "error_count": len(result.errors),
                "succeeded_keys": list(result.values.keys()),
                "failed_keys": list(result.errors.keys()),
            },
        )
    else:
        logger.error(
            "resolve_safe() completed with all failures in %.2fms: %d failed",
            elapsed_ms,
            len(result.errors),
            extra={
                "elapsed_ms": elapsed_ms,
                "success": False,
                "partial": False,
                "error_count": len(result.errors),
                "failed_keys": list(result.errors.keys()),
            },
        )

    return result
