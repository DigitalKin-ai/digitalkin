"""Dynamic schema utilities for runtime value refresh in Pydantic models.

Mark fields as dynamic with ``Annotated`` metadata so their schema values
can be refreshed at runtime via sync or async fetchers.

See ``docs/api/dynamic_schema.md`` and ``tests/utils/test_dynamic_schema.py``.
"""

from __future__ import annotations

import asyncio
import time
import types
from collections.abc import Awaitable, Callable
from itertools import starmap
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from pydantic.fields import FieldInfo

from digitalkin.logger import logger
from digitalkin.models.utils.dynamic_schema import ResolveResult

T = TypeVar("T")

Fetcher = Callable[[], T | Awaitable[T]]
"""Zero-arg sync or async fetcher."""


class DynamicField:
    """Metadata class for Annotated fields with dynamic fetchers.

    Use with typing.Annotated to mark fields that need runtime value resolution.
    Fetchers are callables (sync or async) that return values at runtime.

    Args:
        **fetchers: Mapping of key names to fetcher callables. Each fetcher
            takes no arguments and returns the value for that key.
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


Dynamic = DynamicField


class DynamicSchemaResolver:
    """Extract and resolve ``DynamicField`` fetchers from Pydantic fields."""

    @staticmethod
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

    @staticmethod
    def has_dynamic(field_info: FieldInfo) -> bool:
        """Check if a field has DynamicField metadata.

        Args:
            field_info: The Pydantic FieldInfo object to check.

        Returns:
            True if the field has DynamicField metadata, False otherwise.
        """
        return DynamicSchemaResolver.get_dynamic_metadata(field_info) is not None

    @staticmethod
    def get_fetchers(field_info: FieldInfo) -> dict[str, Fetcher[Any]]:
        """Extract fetchers from a field's DynamicField metadata.

        Args:
            field_info: The Pydantic FieldInfo object to extract from.

        Returns:
            Dict mapping key names to fetcher callables, empty if no DynamicField metadata.
        """
        meta = DynamicSchemaResolver.get_dynamic_metadata(field_info)
        if meta is None:
            return {}
        return meta.fetchers

    @staticmethod
    def _get_fetcher_info(fetcher: Fetcher[Any]) -> str:
        """Get descriptive info about a fetcher for logging.

        Args:
            fetcher: The fetcher callable.

        Returns:
            ``module.qualname`` for functions/methods, ``repr`` otherwise.
        """
        if isinstance(fetcher, types.FunctionType | types.MethodType | types.BuiltinFunctionType):
            return f"{fetcher.__module__}.{fetcher.__qualname__}"
        return repr(fetcher)

    @staticmethod
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
        fetcher_info = DynamicSchemaResolver._get_fetcher_info(fetcher)
        logger.debug("Resolving fetcher '%s' using %s", key, fetcher_info)

        start_time = time.perf_counter()

        try:
            result = fetcher()
            if asyncio.iscoroutine(result):
                logger.debug("Fetcher '%s' returned coroutine, awaiting...", key)
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
            )
            raise

        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.debug(
            "Fetcher '%s' resolved successfully in %.2fms, result type: %s",
            key,
            elapsed_ms,
            type(result).__name__,
        )
        return key, result

    @staticmethod
    async def resolve(
        fetchers: dict[str, Fetcher[Any]],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Resolve all dynamic fetchers to their actual values in parallel.

        Args:
            fetchers: Dict mapping key names to fetcher callables.
            timeout: Optional timeout in seconds for all fetchers combined.
                If None (default), no timeout is applied.

        Returns:
            Dict mapping key names to resolved values.

        Raises:
            asyncio.TimeoutError: If timeout is exceeded.
            Exception: If any fetcher raises an exception, it is propagated.
        """
        if not fetchers:
            logger.debug("resolve() called with empty fetchers, returning {}")
            return {}

        fetcher_keys = list(fetchers.keys())
        logger.info("resolve() starting parallel resolution of %d fetcher(s): %s", len(fetchers), fetcher_keys)

        start_time = time.perf_counter()
        tasks = list(starmap(DynamicSchemaResolver._resolve_one, fetchers.items()))

        try:
            if timeout is not None:
                results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
            else:
                results = await asyncio.gather(*tasks)
        except asyncio.TimeoutError:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            logger.error("resolve() timed out after %.2fms (timeout=%.2fs)", elapsed_ms, timeout)
            raise

        elapsed_ms = (time.perf_counter() - start_time) * 1000
        logger.info("resolve() completed successfully in %.2fms, resolved %d fetcher(s)", elapsed_ms, len(results))
        return dict(results)

    @staticmethod
    async def resolve_safe(
        fetchers: dict[str, Fetcher[Any]],
        *,
        timeout: float | None = None,
    ) -> ResolveResult:
        """Resolve fetchers with structured error handling.

        Unlike ``resolve()``, this catches individual fetcher errors and
        returns them in a structured result, allowing partial success.

        Args:
            fetchers: Dict mapping key names to fetcher callables.
            timeout: Optional timeout in seconds for the whole operation.
                If None (default), no timeout is applied.

        Returns:
            ResolveResult with values and any errors that occurred.
        """
        if not fetchers:
            logger.debug("resolve_safe() called with empty fetchers, returning empty ResolveResult")
            return ResolveResult()

        fetcher_keys = list(fetchers.keys())
        logger.info("resolve_safe() starting parallel resolution of %d fetcher(s): %s", len(fetchers), fetcher_keys)

        start_time = time.perf_counter()
        result = ResolveResult()

        async def safe_resolve_one(key: str, fetcher: Fetcher[Any]) -> None:
            """Resolve one fetcher, capturing errors."""
            try:
                _, value = await DynamicSchemaResolver._resolve_one(key, fetcher)
                result.values[key] = value
            except Exception as e:
                result.errors[key] = e

        tasks = list(starmap(safe_resolve_one, fetchers.items()))

        try:
            if timeout is not None:
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
            else:
                await asyncio.gather(*tasks)
        except asyncio.TimeoutError as e:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
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
            )

        elapsed_ms = (time.perf_counter() - start_time) * 1000

        if result.success:
            logger.info(
                "resolve_safe() completed successfully in %.2fms, all %d fetcher(s) resolved",
                elapsed_ms,
                len(result.values),
            )
        elif result.partial:
            logger.warning(
                "resolve_safe() completed with partial success in %.2fms: %d succeeded, %d failed",
                elapsed_ms,
                len(result.values),
                len(result.errors),
            )
        else:
            logger.error(
                "resolve_safe() completed with all failures in %.2fms: %d failed",
                elapsed_ms,
                len(result.errors),
            )

        return result
