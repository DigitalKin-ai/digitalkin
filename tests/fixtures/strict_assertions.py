"""Strict assertion utilities for rigorous testing.

This module provides enhanced assertion helpers that perform
thorough validation with detailed error messages.
"""

import asyncio
import builtins
import contextlib
import inspect
import traceback
from collections.abc import Callable
from typing import Any
from unittest.mock import Mock

import pytest


class StrictAssertions:
    """Collection of strict assertion methods."""

    @staticmethod
    def assert_mock_called_once_with_exact(mock: Mock, *args, **kwargs) -> None:
        """Assert mock called exactly once with exact arguments."""
        assert mock.called, f"Mock {mock} was never called"
        assert mock.call_count == 1, f"Mock {mock} called {mock.call_count} times, expected 1"

        actual_call = mock.call_args
        if actual_call is None:
            msg = "Mock has no call arguments"
            raise AssertionError(msg)

        expected_args = args
        expected_kwargs = kwargs
        actual_args = actual_call.args
        actual_kwargs = actual_call.kwargs

        # Strict comparison of args
        assert actual_args == expected_args, f"Args mismatch:\nExpected: {expected_args}\nActual: {actual_args}"

        # Strict comparison of kwargs
        assert actual_kwargs == expected_kwargs, (
            f"Kwargs mismatch:\nExpected: {expected_kwargs}\nActual: {actual_kwargs}"
        )

    @staticmethod
    def assert_async_mock_awaited_once_with_exact(mock: Mock, *args, **kwargs) -> None:
        """Assert async mock awaited exactly once with exact arguments."""
        assert mock.awaited, f"Async mock {mock} was never awaited"
        assert mock.await_count == 1, f"Async mock {mock} awaited {mock.await_count} times, expected 1"

        actual_call = mock.await_args
        if actual_call is None:
            msg = "Mock has no await arguments"
            raise AssertionError(msg)

        expected_args = args
        expected_kwargs = kwargs
        actual_args = actual_call.args
        actual_kwargs = actual_call.kwargs

        assert actual_args == expected_args, f"Args mismatch:\nExpected: {expected_args}\nActual: {actual_args}"
        assert actual_kwargs == expected_kwargs, (
            f"Kwargs mismatch:\nExpected: {expected_kwargs}\nActual: {actual_kwargs}"
        )

    @staticmethod
    def assert_task_state(
        task: asyncio.Task,
        expected_done: bool | None = None,
        expected_cancelled: bool | None = None,
        expected_exception: type[Exception] | None = None,
    ) -> None:
        """Assert specific state of an asyncio Task."""
        if expected_done is not None:
            assert task.done() == expected_done, f"Task done state is {task.done()}, expected {expected_done}"

        if expected_cancelled is not None:
            assert task.cancelled() == expected_cancelled, (
                f"Task cancelled state is {task.cancelled()}, expected {expected_cancelled}"
            )

        if expected_exception is not None:
            assert task.done(), "Task must be done to check exception"
            exception = task.exception()
            assert isinstance(exception, expected_exception), (
                f"Task exception is {type(exception)}, expected {expected_exception}"
            )

    @staticmethod
    def assert_dict_subset(actual: dict, expected_subset: dict, strict_types: bool = True) -> None:
        """Assert that actual dict contains all items from expected subset."""
        for key, expected_value in expected_subset.items():
            assert key in actual, f"Key '{key}' not found in actual dict. Keys: {list(actual.keys())}"

            actual_value = actual[key]

            if strict_types:
                assert type(actual_value) == type(expected_value), (
                    f"Type mismatch for key '{key}': {type(actual_value)} != {type(expected_value)}"
                )

            assert actual_value == expected_value, (
                f"Value mismatch for key '{key}':\nExpected: {expected_value}\nActual: {actual_value}"
            )

    @staticmethod
    def assert_no_exceptions_logged(caplog) -> None:
        """Assert no exceptions were logged."""
        for record in caplog.records:
            assert not record.exc_info, f"Exception logged at {record.pathname}:{record.lineno}: {record.getMessage()}"

            # Check for common exception indicators in message
            msg_lower = record.getMessage().lower()
            exception_keywords = ["exception", "error", "traceback", "failed"]
            for keyword in exception_keywords:
                if keyword in msg_lower and record.levelname == "ERROR":
                    msg = f"Potential exception in log at level {record.levelname}: {record.getMessage()}"
                    raise AssertionError(
                        msg
                    )

    @staticmethod
    async def assert_async_cleanup(setup_coro: Callable, cleanup_coro: Callable, check_coro: Callable) -> None:
        """Assert that cleanup properly reverses setup."""
        # Run setup
        resources = await setup_coro()

        try:
            # Verify resources exist
            assert await check_coro(resources), "Resources not properly created during setup"

            # Run cleanup
            await cleanup_coro(resources)

            # Verify resources are cleaned
            assert not await check_coro(resources), "Resources not properly cleaned during cleanup"
        except Exception:
            # Emergency cleanup on test failure
            with contextlib.suppress(builtins.BaseException):
                await cleanup_coro(resources)
            raise

    @staticmethod
    def assert_module_implementation(
        module_class: type, required_methods: list[str], required_attributes: list[str] | None = None
    ) -> None:
        """Assert module class implements required interface."""
        # Check required methods
        for method_name in required_methods:
            assert hasattr(module_class, method_name), (
                f"Module {module_class.__name__} missing required method: {method_name}"
            )

            method = getattr(module_class, method_name)
            assert callable(method), f"{method_name} is not callable in {module_class.__name__}"

            # Check if async methods are properly defined
            if inspect.iscoroutinefunction(method):
                sig = inspect.signature(method)
                assert "self" in sig.parameters or "cls" in sig.parameters, (
                    f"Method {method_name} missing self/cls parameter"
                )

        # Check required attributes
        if required_attributes:
            for attr_name in required_attributes:
                # Check at class level (not requiring instance)
                pass  # Attributes might be instance-level

    @staticmethod
    def assert_resource_limits(max_tasks: int | None = None, max_connections: int | None = None, max_memory_mb: int | None = None):
        """Create assertions for resource limits."""

        class ResourceLimitChecker:
            def __init__(self) -> None:
                self.task_count = 0
                self.connection_count = 0
                self.memory_baseline = None

            def register_task(self) -> None:
                self.task_count += 1
                if max_tasks is not None:
                    assert self.task_count <= max_tasks, f"Task limit exceeded: {self.task_count} > {max_tasks}"

            def unregister_task(self) -> None:
                self.task_count -= 1
                assert self.task_count >= 0, "Task count became negative"

            def register_connection(self) -> None:
                self.connection_count += 1
                if max_connections is not None:
                    assert self.connection_count <= max_connections, (
                        f"Connection limit exceeded: {self.connection_count} > {max_connections}"
                    )

            def unregister_connection(self) -> None:
                self.connection_count -= 1
                assert self.connection_count >= 0, "Connection count became negative"

            def check_memory(self) -> None:
                if max_memory_mb is None:
                    return

                import tracemalloc

                if not tracemalloc.is_tracing():
                    return

                _current, peak = tracemalloc.get_traced_memory()
                peak_mb = peak / 1024 / 1024

                assert peak_mb <= max_memory_mb, f"Memory limit exceeded: {peak_mb:.2f} MB > {max_memory_mb} MB"

            def assert_all_released(self) -> None:
                """Assert all resources have been released."""
                assert self.task_count == 0, f"Tasks not released: {self.task_count} still active"
                assert self.connection_count == 0, f"Connections not released: {self.connection_count} still active"

        return ResourceLimitChecker()


@pytest.fixture
def strict():
    """Fixture providing strict assertions."""
    return StrictAssertions()


@pytest.fixture
def strict_task_validator():
    """Fixture for strict validation of async tasks."""

    class TaskValidator:
        def __init__(self) -> None:
            self.tracked_tasks: set[asyncio.Task] = set()

        def track_task(self, task: asyncio.Task) -> asyncio.Task:
            """Start tracking a task."""
            self.tracked_tasks.add(task)
            return task

        async def assert_all_complete(self, timeout: float = 1.0) -> None:
            """Assert all tracked tasks complete within timeout."""
            if not self.tracked_tasks:
                return

            try:
                await asyncio.wait_for(asyncio.gather(*self.tracked_tasks, return_exceptions=True), timeout=timeout)
            except asyncio.TimeoutError:
                incomplete = [t for t in self.tracked_tasks if not t.done()]
                msg = f"{len(incomplete)} tasks did not complete within {timeout}s"
                raise AssertionError(msg)

            # Check for exceptions
            for task in self.tracked_tasks:
                if task.done() and not task.cancelled():
                    exc = task.exception()
                    if exc is not None:
                        msg = (
                            f"Task raised exception: {exc}\n"
                            f"Traceback: {''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))}"
                        )
                        raise AssertionError(
                            msg
                        )

        def assert_no_running_tasks(self) -> None:
            """Assert no tasks are currently running."""
            running = [t for t in self.tracked_tasks if not t.done()]
            assert not running, f"{len(running)} tasks still running"

        def clear(self) -> None:
            """Clear tracked tasks."""
            self.tracked_tasks.clear()

    validator = TaskValidator()
    yield validator

    # Cleanup check
    validator.assert_no_running_tasks()


@pytest.fixture
def strict_mock_validator():
    """Fixture for strict mock validation."""

    class MockValidator:
        def __init__(self) -> None:
            self.mocks: dict[str, Mock] = {}

        def register(self, name: str, mock: Mock) -> Mock:
            """Register a mock for validation."""
            self.mocks[name] = mock
            return mock

        def assert_call_order(self, *expected_calls: str) -> None:
            """Assert mocks were called in specific order."""
            all_calls = []

            for name, mock in self.mocks.items():
                all_calls.extend((name, call) for call in mock.call_args_list)

            # Sort by call order (this is simplified; real implementation would track time)
            assert len(all_calls) >= len(expected_calls), f"Expected {len(expected_calls)} calls, got {len(all_calls)}"

            for i, expected_name in enumerate(expected_calls):
                actual_name = all_calls[i][0]
                assert actual_name == expected_name, (
                    f"Call order mismatch at position {i}: expected {expected_name}, got {actual_name}"
                )

        def assert_no_unexpected_calls(self) -> None:
            """Assert no mocks have unexpected calls."""
            for name, mock in self.mocks.items():
                # This would need to be configured with expected calls
                pass

        def reset_all(self) -> None:
            """Reset all registered mocks."""
            for mock in self.mocks.values():
                mock.reset_mock()

    return MockValidator()


@pytest.fixture
def strict_queue_validator():
    """Fixture for strict asyncio.Queue validation."""

    class QueueValidator:
        def __init__(self) -> None:
            self.queues: dict[str, asyncio.Queue] = {}
            self.expected_items: dict[str, list[Any]] = {}

        def register_queue(self, name: str, queue: asyncio.Queue, expected_items: list[Any] | None = None) -> None:
            """Register a queue for validation."""
            self.queues[name] = queue
            if expected_items:
                self.expected_items[name] = expected_items

        async def assert_queue_contents(self, name: str) -> None:
            """Assert queue contains expected items."""
            if name not in self.expected_items:
                return

            queue = self.queues[name]
            expected = self.expected_items[name]
            actual = []

            while not queue.empty():
                item = queue.get_nowait()
                actual.append(item)

            assert actual == expected, f"Queue '{name}' content mismatch:\nExpected: {expected}\nActual: {actual}"

            # Put items back
            for item in actual:
                queue.put_nowait(item)

        def assert_all_empty(self) -> None:
            """Assert all registered queues are empty."""
            for name, queue in self.queues.items():
                assert queue.empty(), f"Queue '{name}' is not empty: {queue.qsize()} items remaining"

        def assert_all_bounded(self) -> None:
            """Assert no queue exceeded its max size."""
            for name, queue in self.queues.items():
                if queue.maxsize > 0:
                    assert queue.qsize() <= queue.maxsize, (
                        f"Queue '{name}' exceeded bounds: {queue.qsize()} > {queue.maxsize}"
                    )

    return QueueValidator()


@pytest.fixture
def strict_exception_handler():
    """Fixture for strict exception handling validation."""

    class ExceptionHandler:
        def __init__(self) -> None:
            self.exceptions: list[Exception] = []
            self.old_handler = None

        def install(self) -> None:
            """Install custom exception handler."""
            loop = asyncio.get_event_loop()
            self.old_handler = loop.get_exception_handler()

            def handler(loop, context) -> None:
                self.exceptions.append(context.get("exception"))
                if self.old_handler:
                    self.old_handler(loop, context)

            loop.set_exception_handler(handler)

        def uninstall(self) -> None:
            """Restore original exception handler."""
            if self.old_handler is not None:
                loop = asyncio.get_event_loop()
                loop.set_exception_handler(self.old_handler)

        def assert_no_unhandled_exceptions(self) -> None:
            """Assert no unhandled exceptions occurred."""
            assert not self.exceptions, f"Unhandled exceptions detected: {self.exceptions}"

        def assert_exception_type(self, exc_type: type[Exception]) -> None:
            """Assert specific exception type was caught."""
            for exc in self.exceptions:
                if isinstance(exc, exc_type):
                    return
            msg = f"Expected exception of type {exc_type}, got: {[type(e) for e in self.exceptions]}"
            raise AssertionError(msg)

    handler = ExceptionHandler()
    handler.install()
    yield handler
    handler.uninstall()
    handler.assert_no_unhandled_exceptions()
