"""Exceptions for the communication service."""


class InvalidConsumerAddressError(ValueError):
    """``address`` is not a valid ``host:port`` for dial-back."""


class M2MTargetUnavailable(RuntimeError):  # ruff: ignore[error-suffix-on-exception-name]  # public API name, predates the refactor
    """The per-target circuit breaker is open; fast-fail without hitting the wire."""


class M2MCallTimeout(RuntimeError):  # ruff: ignore[error-suffix-on-exception-name]  # public API name, predates the refactor
    """``output_queue.get()`` exceeded ``call_timeout_s`` waiting for a target output."""


class ToolCallError(RuntimeError):
    """A called tool failed: fatal ``stream.error``/``stream.cancelled`` or a ``ModuleToolkit`` failure.

    Sentinel failures carry ``[CODE] message``; a ``ModuleToolkit`` failure carries the error JSON the model reads.
    """
