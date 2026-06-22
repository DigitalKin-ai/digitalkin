"""Exceptions for the communication service."""


class InvalidConsumerAddressError(ValueError):
    """``address`` is not a valid ``host:port`` for dial-back."""


class M2MTargetUnavailable(RuntimeError):  # noqa: N818  # public API name, predates the refactor
    """The per-target circuit breaker is open; fast-fail without hitting the wire."""


class M2MCallTimeout(RuntimeError):  # noqa: N818  # public API name, predates the refactor
    """``output_queue.get()`` exceeded ``call_timeout_s`` waiting for a target output."""
